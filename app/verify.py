"""佐证核查核心逻辑：引用链搜索与后台核查任务。

判定规则：从起点条目出发，若存在两条引用链，满足
  1. 除起点外互不重合（中间条目与末端原件均不重叠）；
  2. 分别通向两个不同档号的在册原件；
则佐证成立，保存两条链；否则记为佐证不足。

取图优化见 app/graph_cache.py：同一进程内，事项已整图取过且之后只有成功
提交的引用改录时，下一次核查复用基线图并只合入对应版本补丁，跳过未变的
条目、档号与引用；首次取图、重启或其他资料变动后整图读库。
"""
import json
import time
from datetime import datetime

from sqlalchemy import text, update

from .database import SessionLocal, engine
from .graph_cache import graph_cache
from .models import (DOC_REGISTERED, ENTRY_ACTIVE, V_CONFIRMED, V_INFLIGHT,
                     V_INSUFFICIENT, Citation, Document, Entry, Matter,
                     Verification)

# 简单路径枚举上限，防止极端图下耗时过长（实际事项规模远低于此）
MAX_PATHS = 2000


def snapshot_graph(session, matter_id):
    """读取某事项当前的引用图快照：在录条目、在册档号、有效引用边。"""
    entries = {
        e.id: e
        for e in session.query(Entry).filter(
            Entry.matter_id == matter_id, Entry.status == ENTRY_ACTIVE
        )
    }
    docs = {
        d.id: d
        for d in session.query(Document).filter(
            Document.matter_id == matter_id, Document.status == DOC_REGISTERED
        )
    }
    adj = {}
    if entries:
        q = session.query(Citation).filter(
            Citation.matter_id == matter_id,
            Citation.from_entry_id.in_(list(entries)),
        )
        for c in q:
            if c.to_entry_id is not None:
                if c.to_entry_id in entries:  # 被引条目须在录
                    adj.setdefault(c.from_entry_id, []).append(("entry", c.to_entry_id))
            elif c.to_document_id is not None:
                if c.to_document_id in docs:  # 被引档号须在册
                    adj.setdefault(c.from_entry_id, []).append(("document", c.to_document_id))
    return entries, docs, adj


def _read_version_row(session, matter_id):
    return (
        session.query(Matter.version, Matter.created_at)
        .filter(Matter.id == matter_id)
        .one_or_none()
    )


def _cache_or_none(matter_id, version, created_at):
    try:
        return graph_cache.try_build(matter_id, version, created_at)
    except Exception:  # noqa: BLE001 - 缓存是优化层，异常不得拖垮核查
        return None


def _store_full_safely(matter_id, version, entries, docs, adj, created_at):
    try:
        graph_cache.note_full_read()
        graph_cache.store_full(matter_id, version, entries, docs, adj, created_at)
    except Exception:  # noqa: BLE001
        pass


def _sqlite_snapshot_read(session, matter_id):
    """在显式 BEGIN 固定的 SQLite/WAL 读快照内连读版号与整图。

    SQLAlchemy 2.x 的 pysqlite 对纯 SELECT 默认走驱动自动提交、不持有读
    事务，因此这里显式 BEGIN：此后其他连接的提交不会改变本事务所见状态，
    版号与图必然来自同一次已提交状态。不在此结束事务——返回的 ORM 对象
    属性均已加载，调用方完成计算后由外层 session.close() 收尾（连接归还
    池时会 ROLLBACK 这个只读事务）。
    """
    session.rollback()  # 结束 SA 逻辑事务的未定状态；此时通常并无物理事务
    raw = session.connection().connection.dbapi_connection
    if not getattr(raw, "in_transaction", False):
        session.execute(text("BEGIN"))
    row = _read_version_row(session, matter_id)
    if row is None:
        return None
    version, created_at = row[0], row[1]
    entries, docs, adj = snapshot_graph(session, matter_id)
    _store_full_safely(matter_id, version, entries, docs, adj, created_at)
    return version, created_at, entries, docs, adj, "database"


def _bracketed_read(session, matter_id, max_attempts=200):
    """通用兜底：用「版号前读 → 整图 → 版号后读」夹住整图读取。

    前后版号一致才返回（任何资料提交都会在同一事务内令 version+1）；
    否则回滚并重新取图。
    """
    version = created_at = None
    row = None
    for _ in range(max_attempts):
        row = _read_version_row(session, matter_id)
        if row is None:
            return None
        version, created_at = row[0], row[1]
        entries, docs, adj = snapshot_graph(session, matter_id)
        end_version = session.query(Matter.version).filter(
            Matter.id == matter_id
        ).scalar()
        if end_version == version:
            _store_full_safely(matter_id, version, entries, docs, adj, created_at)
            return version, created_at, entries, docs, adj, "database"
        session.rollback()
    # 持续高并发写入下多次未得稳定窗口：退回显式快照（sqlite）或最后一次结果
    try:
        if session.get_bind().dialect.name == "sqlite":
            return _sqlite_snapshot_read(session, matter_id)
    except Exception:  # noqa: BLE001
        pass
    if row is None:
        return None
    entries, docs, adj = snapshot_graph(session, matter_id)
    return version, created_at, entries, docs, adj, "database"


def read_consistent_graph(session, matter_id):
    """取与资料版号来自同一次已提交状态的图。

    返回 ``(version, created_at, entries, docs, adj, source)``；事项不存在时
    为 ``(None, None, {}, {}, {}, "missing")``。
    热缓存按精确版本号逻辑复原（source="cache"）；未命中则整图读库
    （source="database"），SQLite 下用显式读快照保证一致。
    """
    row = _read_version_row(session, matter_id)
    if row is None:
        return None, None, {}, {}, {}, "missing"
    version, created_at = row[0], row[1]

    cached = _cache_or_none(matter_id, version, created_at)
    if cached is not None:
        return version, created_at, cached[0], cached[1], cached[2], "cache"

    try:
        dialect_name = session.get_bind().dialect.name
    except Exception:  # noqa: BLE001
        dialect_name = engine.dialect.name

    if dialect_name == "sqlite":
        try:
            result = _sqlite_snapshot_read(session, matter_id)
            if result is not None:
                return result
            return None, None, {}, {}, {}, "missing"
        except Exception:  # noqa: BLE001 - 显式快照异常时退回夹读
            pass
    result = _bracketed_read(session, matter_id)
    if result is None:
        return None, None, {}, {}, {}, "missing"
    return result


def find_two_chains(start_entry_id, entries, docs, adj):
    """枚举起点到各在册原件的简单路径（条目可互引，故按访问集去环），
    寻找一对除起点外互不重合且档号不同的链。找到返回 (chain1, chain2)，否则 None。"""
    if start_entry_id not in entries:
        return None
    paths = []
    stack = [(start_entry_id, {start_entry_id}, [("entry", start_entry_id)])]
    while stack and len(paths) < MAX_PATHS:
        node, visited, path = stack.pop()
        for kind, tid in adj.get(node, ()):
            if kind == "document":
                paths.append(path + [("document", tid)])
                if len(paths) >= MAX_PATHS:
                    break
            elif tid not in visited:
                stack.append((tid, visited | {tid}, path + [("entry", tid)]))
    for i in range(len(paths)):
        p1 = paths[i]
        rest1 = set(p1[1:])  # 起点之外的节点
        doc1 = p1[-1][1]
        for j in range(i + 1, len(paths)):
            p2 = paths[j]
            doc2 = p2[-1][1]
            if docs[doc1].archive_no == docs[doc2].archive_no:
                continue  # 必须通向不同档号
            if rest1.isdisjoint(set(p2[1:])):
                return p1, p2
    return None


def chain_to_jsonable(path, entries, docs):
    out = []
    for kind, nid in path:
        if kind == "entry":
            e = entries[nid]
            out.append({"type": "entry", "id": e.id, "label": e.label})
        else:
            d = docs[nid]
            out.append({"type": "document", "id": d.id, "archive_no": d.archive_no})
    return out


def run_verification(verification_id, delay=0.0):
    """后台核查：先取与版号一致的图并计算，再条件写入结论。

    取图/计算使用独立 session，完成后即释放连接；核查（含模拟耗时的 delay）
    期间资料可正常改动，后续改录发布的补丁不会改动本核查手里的图。
    写入结论时仅当记录仍为“在途”才生效——若期间本事项资料变动，该核查已被
    置为“退回”，此处的条件 UPDATE 命中 0 行，不写回任何结论。
    """
    session = SessionLocal()
    version0 = None
    try:
        ver = session.get(Verification, verification_id)
        if ver is None or ver.status != V_INFLIGHT:
            return
        matter_id = ver.matter_id
        start_id = ver.start_entry_id

        version0, _created0, entries, docs, adj, source = read_consistent_graph(
            session, matter_id
        )
        if source == "missing":
            return
        result = find_two_chains(start_id, entries, docs, adj)
        if result:
            chain_a = json.dumps(chain_to_jsonable(result[0], entries, docs), ensure_ascii=False)
            chain_b = json.dumps(chain_to_jsonable(result[1], entries, docs), ensure_ascii=False)
            status = V_CONFIRMED
        else:
            chain_a = chain_b = None
            status = V_INSUFFICIENT
    finally:
        session.close()  # 释放连接与只读快照，核查等待期间不占用数据库

    if delay and delay > 0:
        time.sleep(delay)  # 模拟核查耗时；此窗口内资料改动会使本核查被退回

    session = SessionLocal()
    try:
        exec_result = session.execute(
            update(Verification)
            .where(Verification.id == verification_id, Verification.status == V_INFLIGHT)
            .values(
                status=status,
                chain_a=chain_a,
                chain_b=chain_b,
                matter_version=version0,
                updated_at=datetime.utcnow(),
            )
        )
        session.commit()
        # exec_result.rowcount == 0：已被资料变动置为“退回”，不得写回结论
        _ = exec_result.rowcount
    except Exception:
        session.rollback()  # 写入失败：保留原状（仍为在途或已被退回）
    finally:
        session.close()
