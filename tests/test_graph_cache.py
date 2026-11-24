"""增量取图缓存的自测：

- 只有改录引用时复用原图、只合入成功提交的补丁（连续改录）；
- 首次/重启/其他资料变动时整图读库；
- 图与 matter.version 一致；热路径不查 entries/documents/citations；
- 旧核查手里的图不被后续改录改动；
- 并行核查结果一致；
- 改录校验失败/提交失败时保留原图、资料与核查状态，失败改录不入缓存。

判定规则与既有接口不变，这些用例只验证取图优化及相关并发/失败语义。
"""
import concurrent.futures
import threading
import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event
from sqlalchemy.exc import SQLAlchemyError

from app import verify as verify_mod
from app.database import Base, SessionLocal, engine
from app.graph_cache import graph_cache
from app.main import app, get_session


@pytest.fixture(autouse=True)
def _clear_graph_cache():
    # clean_db 的 drop_all/create_all 也会经 DDL 事件清空；这里再显式兜底
    graph_cache.clear()
    yield
    graph_cache.clear()


@pytest.fixture(autouse=True)
def clean_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    app.state.verify_delay = 0.0
    yield
    app.dependency_overrides.clear()
    app.state.verify_delay = 0.0


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def snapshot_spy(monkeypatch):
    """统计整图读库 snapshot_graph 的调用次数（包装原函数，不改行为）。"""
    state = {"calls": 0}
    orig = verify_mod.snapshot_graph

    def spy(session, matter_id):
        state["calls"] += 1
        return orig(session, matter_id)

    monkeypatch.setattr(verify_mod, "snapshot_graph", spy)
    return state


@pytest.fixture
def graph_sql_counter():
    """统计核查请求期间对 entries/documents/citations 的 SELECT 次数。"""
    state = {"count": 0, "statements": []}
    lock = threading.Lock()

    def before(conn, cursor, statement, params, context, executemany):
        normalized = " ".join(statement.split()).upper()
        if not normalized.startswith("SELECT"):
            return
        for table in ("ENTRIES", "DOCUMENTS", "CITATIONS"):
            if f"FROM {table}" in normalized:
                with lock:
                    state["count"] += 1
                    state["statements"].append(" ".join(statement.split()))
                return

    event.listen(engine, "before_cursor_execute", before)
    try:
        yield state
    finally:
        event.remove(engine, "before_cursor_execute", before)


# ---------- 辅助 ----------

def make_matter(client, old="中山路", new="解放路", date="2026-01-01"):
    r = client.post("/matters", json={"old_name": old, "new_name": new,
                                      "effective_date": date})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def add_doc(client, mid, no):
    r = client.post(f"/matters/{mid}/documents", json={"archive_no": no})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def add_entry(client, mid, label, citations=None):
    r = client.post(f"/matters/{mid}/entries",
                    json={"label": label, "citations": citations or []})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def put_citations(client, mid, eid, citations):
    r = client.put(f"/matters/{mid}/entries/{eid}/citations",
                   json={"citations": citations})
    assert r.status_code == 200, r.text
    return r.json()


def verify(client, mid, entry_id, wait=True):
    r = client.post(f"/matters/{mid}/verifications?wait={str(wait).lower()}",
                    json={"entry_id": entry_id})
    assert r.status_code == 202, r.text
    return r.json()


def matter_version(client, mid):
    return client.get(f"/matters/{mid}").json()["matter"]["version"]


def build_two_chain(client):
    """E1 → E2 → A-001；E1 → E3 → A-002。返回 (mid,e1,e2,e3,doc1,doc2)。"""
    mid = make_matter(client)
    d1 = add_doc(client, mid, "A-001")
    d2 = add_doc(client, mid, "A-002")
    e2 = add_entry(client, mid, "县志卷三", [{"archive_no": "A-001"}])
    e3 = add_entry(client, mid, "地名志", [{"archive_no": "A-002"}])
    e1 = add_entry(client, mid, "市志",
                   [{"entry_id": e2}, {"entry_id": e3}])
    return mid, e1, e2, e3, d1, d2


# ---------- 增量复用：只有改录引用 ----------

def test_citation_rerecord_reuses_graph(client, snapshot_spy):
    mid, e1, e2, e3, _, _ = build_two_chain(client)

    v1 = verify(client, mid, e1)
    assert v1["status"] == "佐证成立"
    assert snapshot_spy["calls"] == 1  # 首次取图整图读库

    put_citations(client, mid, e3, [{"archive_no": "A-001"}])
    v2 = verify(client, mid, e1)
    assert v2["status"] == "佐证不足"
    assert snapshot_spy["calls"] == 1  # 没有再次整图读库
    assert graph_cache.stats["incremental_hits"] >= 1

    # 改回后再次成立：仍然复用
    put_citations(client, mid, e3, [{"archive_no": "A-002"}])
    v3 = verify(client, mid, e1)
    assert v3["status"] == "佐证成立"
    assert snapshot_spy["calls"] == 1


def test_continuous_citation_rerecords_merge_all_committed(client, snapshot_spy):
    """连续多次改录（含同一条目多次整体替换），每次核查结果都必须与最新库状态一致。"""
    mid, e1, e2, e3, _, _ = build_two_chain(client)
    verify(client, mid, e1)
    assert snapshot_spy["calls"] == 1

    expected = []
    # 交替把 E3 指向 A-001 / A-002，连续 6 次改录并逐次核查
    for i in range(6):
        target = "A-001" if i % 2 == 0 else "A-002"
        put_citations(client, mid, e3, [{"archive_no": target}])
        v = verify(client, mid, e1)
        expected_status = "佐证不足" if target == "A-001" else "佐证成立"
        assert v["status"] == expected_status
        assert v["matter_version"] == matter_version(client, mid)
        expected.append(v["status"])
    assert expected[-1] == "佐证成立"
    assert snapshot_spy["calls"] == 1  # 全程只在首次整图读库

    # 再连续改录 E2 与 E3 为空引用，结论应为不足，且仍走增量
    put_citations(client, mid, e2, [])
    put_citations(client, mid, e3, [])
    v = verify(client, mid, e1)
    assert v["status"] == "佐证不足"
    assert snapshot_spy["calls"] == 1


def test_repeated_verify_without_change_uses_cache(client, snapshot_spy):
    mid, e1, *_ = build_two_chain(client)
    verify(client, mid, e1)
    verify(client, mid, e1)
    verify(client, mid, e1)
    assert snapshot_spy["calls"] == 1


def test_warm_verify_emits_no_graph_table_sql(client, graph_sql_counter):
    mid, e1, _, e3, _, _ = build_two_chain(client)
    verify(client, mid, e1)
    put_citations(client, mid, e3, [{"archive_no": "A-001"}])

    graph_sql_counter["count"] = 0
    graph_sql_counter["statements"].clear()
    v = verify(client, mid, e1)
    assert v["status"] == "佐证不足"
    # 热路径：起点校验走缓存、取图走增量，未变的条目/档号/引用一行都不重读
    assert graph_sql_counter["count"] == 0, graph_sql_counter["statements"]


# ---------- 首次 / 重启 / 其他资料变动 → 整图读库 ----------

def test_restart_forces_full_read(client, snapshot_spy):
    mid, e1, *_ = build_two_chain(client)
    verify(client, mid, e1)
    assert snapshot_spy["calls"] == 1
    # 模拟进程重启：内存缓存清空（DDL 不动）
    graph_cache.clear()
    v = verify(client, mid, e1)
    assert v["status"] == "佐证成立"
    assert snapshot_spy["calls"] == 2


def test_other_material_changes_force_full_read(client, snapshot_spy):
    mid, e1, _, e3, _, _ = build_two_chain(client)
    verify(client, mid, e1)
    assert snapshot_spy["calls"] == 1

    # 改录引用：仍增量
    put_citations(client, mid, e3, [{"archive_no": "A-001"}])
    verify(client, mid, e1)
    assert snapshot_spy["calls"] == 1

    # 登记新档号：其他资料变动 → 失效 → 整图读库
    add_doc(client, mid, "A-003")
    verify(client, mid, e1)
    assert snapshot_spy["calls"] == 2

    # 撤回档号同样整图
    r = client.delete(f"/matters/{mid}/documents/A-003")
    assert r.status_code == 200
    verify(client, mid, e1)
    assert snapshot_spy["calls"] == 3

    # 新增条目 → 整图
    add_entry(client, mid, "新编条目")
    verify(client, mid, e1)
    assert snapshot_spy["calls"] == 4

    # 撤回条目 → 整图
    r = client.delete(f"/matters/{mid}/entries/{e3}")
    assert r.status_code == 200
    verify(client, mid, e1)
    assert snapshot_spy["calls"] == 5


def test_ddl_rebuild_clears_cache(client):
    mid, e1, *_ = build_two_chain(client)
    verify(client, mid, e1)
    assert graph_cache.info()["matters"]
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    # DDL 事件已自动清空，不会拿旧事项的图套新库同 id 事项
    assert graph_cache.info()["matters"] == []


# ---------- 版本一致性 ----------

def test_graph_and_version_come_from_same_state(client):
    mid, e1, _, e3, _, _ = build_two_chain(client)
    v0 = verify(client, mid, e1)
    assert v0["matter_version"] == matter_version(client, mid) == 5

    for target in ("A-001", "A-002", "A-001"):
        put_citations(client, mid, e3, [{"archive_no": target}])
        v = verify(client, mid, e1)
        assert v["matter_version"] == matter_version(client, mid)
        # 缓存信息中的基线+补丁头也必须到达当前版本
        info = [m for m in graph_cache.info()["matters"]
                if m["matter_id"] == mid][0]
        assert info["head_version"] == v["matter_version"]


# ---------- 旧核查的图保持原样（copy-on-write 隔离） ----------

def test_handed_graph_stays_unchanged_after_later_patch():
    mid = 777
    entries = {1: object(), 2: object()}
    docs = {10: object(), 11: object()}
    adj = {1: [("entry", 2)], 2: [("document", 10)]}
    graph_cache.store_full(mid, 5, entries, docs, adj, created_at="t0")

    g_old = graph_cache.try_build(mid, 5)
    old_adj = g_old[2]
    assert old_adj[2] == (("document", 10),)

    # 乱序发布也应能正确复原
    graph_cache.record_citation_change(mid, 7, 2, [("document", 11)])
    graph_cache.record_citation_change(mid, 6, 2, [("document", 11)])

    g6 = graph_cache.try_build(mid, 6)
    g7 = graph_cache.try_build(mid, 7)
    assert g6[2][2] == (("document", 11),)
    assert g7[2][2] == (("document", 11),)
    # 已交到“旧核查”手里的图不变，且与新图不是同一个 adj dict
    assert old_adj[2] == (("document", 10),)
    assert old_adj is not g6[2] and old_adj is not g7[2]
    # 顺序构建后基线压缩到最新版本，已合入补丁清空（未合入的更高版本补丁保留）
    info = graph_cache.info()["matters"][0]
    assert info["base_version"] == 7
    assert info["patch_versions"] == []


def test_cache_compaction_keeps_only_new_delta(client, snapshot_spy):
    """连续改录并逐次核查：每次只合入新增量，基线随核查推进。"""
    mid, e1, _, e3, _, _ = build_two_chain(client)
    verify(client, mid, e1)  # base=5
    put_citations(client, mid, e3, [{"archive_no": "A-001"}])  # v6
    verify(client, mid, e1)  # 合入 v6 并压缩到 6
    info = [m for m in graph_cache.info()["matters"] if m["matter_id"] == mid][0]
    assert info["base_version"] == 6 and info["patch_versions"] == []

    put_citations(client, mid, e3, [{"archive_no": "A-002"}])  # v7
    verify(client, mid, e1)
    info = [m for m in graph_cache.info()["matters"] if m["matter_id"] == mid][0]
    assert info["base_version"] == 7 and info["patch_versions"] == []
    assert snapshot_spy["calls"] == 1


# ---------- 并行核查 ----------

def test_parallel_verifications_consistent(client):
    mid, e1, *_ = build_two_chain(client)
    verify(client, mid, e1)  # 预热缓存

    def one(_):
        c = TestClient(app)
        r = c.post(f"/matters/{mid}/verifications?wait=true",
                   json={"entry_id": e1})
        assert r.status_code == 202, r.text
        return r.json()["status"], r.json()["matter_version"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(one, range(12)))
    assert all(status == "佐证成立" for status, _ in results), results
    assert all(ver == 5 for _, ver in results)


def test_parallel_verifications_with_continuous_rerecords(client):
    app.state.verify_delay = 0.15
    try:
        mid, e1, _, e3, _, _ = build_two_chain(client)
        verify(client, mid, e1)

        stop = threading.Event()
        outcomes = []
        outcomes_lock = threading.Lock()

        def verifier():
            c = TestClient(app)
            while not stop.is_set():
                r = c.post(f"/matters/{mid}/verifications?wait=true",
                           json={"entry_id": e1})
                if r.status_code != 202:
                    with outcomes_lock:
                        outcomes.append(("http_error", r.status_code))
                    return
                with outcomes_lock:
                    outcomes.append((r.json()["status"], r.json().get("matter_version")))

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futs = [pool.submit(verifier) for _ in range(4)]
            # 主线程连续改录
            for i in range(8):
                target = "A-001" if i % 2 == 0 else "A-002"
                put_citations(client, mid, e3, [{"archive_no": target}])
                time.sleep(0.03)
            time.sleep(0.4)
            stop.set()
            for f in futs:
                f.result(timeout=15)

        assert not any(s == "http_error" for s in outcomes), outcomes
        # 在途核查若撞上资料变动会被退回；其余结论必须是两种判定之一
        assert all(s in ("佐证成立", "佐证不足", "退回") for s, _ in outcomes), outcomes

        # 落定到 A-002 后，新核查必须用新图得出成立
        put_citations(client, mid, e3, [{"archive_no": "A-002"}])
        final = verify(client, mid, e1)
        assert final["status"] == "佐证成立"
        assert final["matter_version"] == matter_version(client, mid)
    finally:
        app.state.verify_delay = 0.0


# ---------- 改录失败：保留原图、资料、核查状态，失败补丁不入缓存 ----------

def test_invalid_rerecord_422_keeps_graph_and_cache(client, snapshot_spy):
    mid, e1, _, e3, _, _ = build_two_chain(client)
    old_v = verify(client, mid, e1)
    assert old_v["status"] == "佐证成立"
    version_before = matter_version(client, mid)

    # 自引用 → 422（事务回滚）
    r = client.put(f"/matters/{mid}/entries/{e3}/citations",
                   json={"citations": [{"entry_id": e3}]})
    assert r.status_code == 422
    assert matter_version(client, mid) == version_before

    # 原图仍可增量复用，结论不变，且没有整图重读
    v = verify(client, mid, e1)
    assert v["status"] == "佐证成立"
    assert snapshot_spy["calls"] == 1


def test_failed_citation_commit_keeps_original_graph_and_status(
        client, snapshot_spy):
    mid, e1, _, e3, _, _ = build_two_chain(client)
    old_v = verify(client, mid, e1)
    assert old_v["status"] == "佐证成立"
    version_before = matter_version(client, mid)
    recorded_before = graph_cache.stats["citation_patches_recorded"]

    def broken_session():
        s = SessionLocal()

        def boom():
            raise SQLAlchemyError("simulated citation write failure")

        s.commit = boom
        try:
            yield s
        finally:
            s.rollback()
            s.close()

    app.dependency_overrides[get_session] = broken_session
    try:
        r = client.put(f"/matters/{mid}/entries/{e3}/citations",
                       json={"citations": [{"archive_no": "A-001"}]})
        assert r.status_code == 500
        assert "沿用原资料" in r.json()["detail"]
    finally:
        app.dependency_overrides.clear()

    # 资料、版本、原核查结论均未变
    detail = client.get(f"/matters/{mid}").json()
    e3_out = next(e for e in detail["entries"] if e["id"] == e3)
    assert [c.get("archive_no") for c in e3_out["citations"]] == ["A-002"]
    assert detail["matter"]["version"] == version_before
    assert client.get(f"/verifications/{old_v['id']}").json()["status"] == "佐证成立"
    # 失败改录没有发布补丁
    assert graph_cache.stats["citation_patches_recorded"] == recorded_before

    # 下一次核查仍复用原图得出原结论，不整图读库
    v = verify(client, mid, e1)
    assert v["status"] == "佐证成立"
    assert snapshot_spy["calls"] == 1


def test_returned_verification_not_written_back(client):
    """长核查窗口内改录引用：旧核查被退回且不写结论；新核查用新图。"""
    app.state.verify_delay = 0.8
    try:
        mid, e1, _, e3, _, _ = build_two_chain(client)
        r = client.post(f"/matters/{mid}/verifications", json={"entry_id": e1})
        vid = r.json()["id"]
        assert r.json()["status"] == "在途"
        time.sleep(0.1)

        put_citations(client, mid, e3, [{"archive_no": "A-001"}])

        deadline = time.time() + 8
        final_old = None
        while time.time() < deadline:
            final_old = client.get(f"/verifications/{vid}").json()
            if final_old["status"] != "在途":
                break
            time.sleep(0.05)
        assert final_old["status"] == "退回"
        assert final_old["chain_a"] is None and final_old["chain_b"] is None

        # 新核查使用新图 → 不足
        new_v = TestClient(app).post(
            f"/matters/{mid}/verifications?wait=true", json={"entry_id": e1}
        ).json()
        assert new_v["status"] == "佐证不足"
    finally:
        app.state.verify_delay = 0.0


# ---------- 版号与图同状态：并发写入风暴下的一致性 ----------

def test_consistent_graph_matches_version_under_write_storm():
    """只有档号登记时：版本号 == 在册档号数。并发登记期间，一致性读取
    返回的 (version, graph) 必须始终满足该对应关系，不得给出跨版本拼图。"""
    import datetime
    from app.models import Matter as M, Document as D
    from app.verify import read_consistent_graph

    s = SessionLocal()
    m = M(old_name="风暴街", new_name="稳定街",
          effective_date=datetime.date(2026, 1, 1), version=0)
    s.add(m)
    s.commit()
    mid = m.id
    s.close()

    stop = threading.Event()
    errors = []

    def writer():
        k = 0
        while not stop.is_set() and k < 200:
            w = SessionLocal()
            try:
                w.add(D(matter_id=mid, archive_no=f"M-{k}", status="在册"))
                w.query(M).filter(M.id == mid).update(
                    {"version": M.version + 1}, synchronize_session=False)
                w.commit()
                k += 1
            except Exception as e:  # noqa: BLE001
                w.rollback()
                # 偶发锁冲突可重试，不计为断言失败
                if "locked" not in str(e).lower():
                    errors.append(repr(e))
            finally:
                w.close()

    wt = threading.Thread(target=writer)
    wt.start()
    reads = 0
    deadline = time.time() + 12
    try:
        while time.time() < deadline and wt.is_alive():
            graph_cache.clear()  # 强制走整图读库路径
            rs = SessionLocal()
            try:
                version, _ca, entries, docs, adj, source = read_consistent_graph(rs, mid)
            finally:
                rs.close()
            reads += 1
            if source == "missing":
                errors.append("matter missing during storm")
                break
            if len(docs) != version:
                errors.append(f"inconsistent pair: version={version} docs={len(docs)}")
                break
    finally:
        stop.set()
        wt.join(timeout=15)

    assert not errors, errors[:3]
    assert reads > 5


# ---------- 调试观测接口 ----------

def test_debug_graph_cache_endpoint(client):
    mid, e1, _, e3, _, _ = build_two_chain(client)
    verify(client, mid, e1)
    put_citations(client, mid, e3, [{"archive_no": "A-001"}])
    v = verify(client, mid, e1)

    r = client.get("/debug/graph-cache")
    assert r.status_code == 200
    body = r.json()
    assert body["stats"]["full_reads"] >= 1
    assert body["stats"]["incremental_hits"] >= 1
    mine = client.get("/debug/graph-cache", params={"matter_id": mid}).json()
    assert len(mine["matters"]) == 1
    mstat = mine["matters"][0]
    assert mstat["base_version"] == v["matter_version"] == mstat["head_version"]
