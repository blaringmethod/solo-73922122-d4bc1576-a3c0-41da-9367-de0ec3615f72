"""进程内「核查取图」增量缓存。

背景
----
编目员改录引用（PUT citations）后通常要再次核查。若两次核查之间该事项只有
「改录引用」这一种资料变动，就不必整图读库：复用首次读取的基线图，只把其间
**已成功提交**的改录补丁按版本顺序合入（copy-on-write），未变的条目、原件
档号和引用边全部原样复用。

规则
----
- 每个事项缓存一份与 ``base_version`` 已提交状态一致的整图；
- 每次引用改录**事务提交成功后**发布一个 :class:`CitationPatch`（带新版本号）；
  提交失败/校验失败不发布补丁，原图不受影响；
- 条目或档号的登记、撤回等「其他资料变动」提交后，对应事项缓存整体失效，
  下一次核查整图读库；
- 进程重启后内存为空，首次取图整图读库；
- 每次核查以其数据库只读快照里读到的 ``matter.version`` 为准，只合入
  ``(base_version, version]`` 区间内版本连续齐全的补丁；缺任一补丁（例如
  发布竞态）或基线版本更新时，返回 None，由调用方在**同一快照事务**内整图读库，
  从而保证「图」和「资料版号」来自同一次已提交状态；
- 交给核查线程的是新建的邻接 dict（值为冻结 tuple），entries/docs 只读共享，
  因此旧核查手里的图不会被后续改录或新核查改动。

测试夹具常 ``drop_all/create_all`` 重建表，这里监听同一 ``MetaData`` 的 DDL
事件自动清空缓存，避免跨用例的同 id 事项串图。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Optional

from sqlalchemy import event

from .database import Base

# 边的种类，与 app.verify.snapshot_graph 保持一致
EDGE_ENTRY = "entry"
EDGE_DOCUMENT = "document"

Edge = tuple[str, int]


@dataclass(frozen=True)
class CitationPatch:
    """一次已成功提交的引用改录：整体替换 ``entry_id`` 的全部出边。"""

    to_version: int
    entry_id: int
    edges: tuple[Edge, ...]


@dataclass
class _MatterCache:
    base_version: int
    entries: dict
    docs: dict
    adj: dict  # 基线邻接的冻结副本：{from_entry_id: tuple[Edge, ...]}
    created_at: Any = None
    # 以版本号为键，允许补丁发布顺序与版本先后不一致（提交串行、发布可能乱序）
    patches: dict[int, CitationPatch] = field(default_factory=dict)


class GraphCache:
    """线程安全的每事项图缓存（单进程内有效）。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._init_locked()

    def _init_locked(self):
        self._matters: dict[int, _MatterCache] = {}
        self.stats = {
            # 核查侧统计
            "full_reads": 0,          # 核查整图读库次数
            "incremental_hits": 0,    # 核查增量复用成功次数
            "patches_merged": 0,      # 增量合并的补丁总数
            # 缓存维护统计
            "stores": 0,
            "citation_patches_recorded": 0,
            "citation_patches_dropped": 0,  # 发布时没有可附着的基线图
            "invalidations": 0,
            "resets": 0,
        }

    # ---------- 生命周期 ----------

    def clear(self) -> None:
        """清空全部缓存（模拟进程重启；DDL 重建时也会调用）。"""
        with self._lock:
            self._init_locked()
            self.stats["resets"] += 1

    # ---------- 写入侧钩子（均在数据库事务成功提交之后调用） ----------

    def record_citation_change(self, matter_id: int, to_version: int,
                               entry_id: int, edges) -> bool:
        """登记一次已提交的引用改录。无基线图可附着时返回 False（下次整图读库）。"""
        patch = CitationPatch(
            to_version=int(to_version),
            entry_id=int(entry_id),
            edges=tuple((str(kind), int(tid)) for kind, tid in edges),
        )
        with self._lock:
            st = self._matters.get(matter_id)
            if st is None or patch.to_version <= st.base_version:
                # 没有基线图：无需留存补丁，下一次核查自然整图读库
                self.stats["citation_patches_dropped"] += 1
                return False
            st.patches[patch.to_version] = patch
            self.stats["citation_patches_recorded"] += 1
            return True

    def invalidate(self, matter_id: int) -> None:
        """其他资料变动（条目/档号登记、撤回等）提交后令该事项缓存失效。"""
        with self._lock:
            if self._matters.pop(matter_id, None) is not None:
                self.stats["invalidations"] += 1

    def store_full(self, matter_id: int, version: int, entries: dict,
                  docs: dict, adj: dict, created_at: Any = None) -> bool:
        """存入一次与 ``version`` 一致的整图读库结果。

        已有同版本或更新的基线时不回退覆盖；旧基线之上、版本号更新的补丁
        可以保留并附着到新基线（它们本来就是引用改录补丁）。
        入参不是预期的 dict 结构时安全忽略（缓存只是优化，不能影响核查主流程）。
        """
        if not (isinstance(entries, dict) and isinstance(docs, dict)
                and isinstance(adj, dict)) or version is None:
            return False
        try:
            frozen_adj = {int(k): tuple(v) for k, v in adj.items()}
        except TypeError:
            return False
        with self._lock:
            old = self._matters.get(matter_id)
            if old is not None and created_at is not None \
                    and old.created_at is not None and old.created_at != created_at:
                old = None  # 同 id 但已是重建后的另一条事项
            if old is not None and old.base_version >= int(version):
                return False
            carried = {}
            if old is not None:
                carried = {v: p for v, p in old.patches.items() if v > int(version)}
            self._matters[matter_id] = _MatterCache(
                base_version=int(version),
                entries=entries,
                docs=docs,
                adj=frozen_adj,
                created_at=created_at,
                patches=carried,
            )
            self.stats["stores"] += 1
            return True

    # ---------- 核查取图 ----------

    def try_build(self, matter_id: int, version: Optional[int],
                  created_at: Any = None, *, count: bool = True):
        """尝试构造与 ``version`` 已提交状态一致的图。

        成功返回 ``(entries, docs, adj)``；无法精确复原该版本时返回 None，
        调用方应整图读库。返回的 adj 是新建 dict，调用方可自由使用，不影响缓存
        与其他核查。
        """
        if version is None:
            return None
        with self._lock:
            st = self._matters.get(matter_id)
            if st is None:
                return None
            if created_at is not None and st.created_at is not None \
                    and st.created_at != created_at:
                self._matters.pop(matter_id, None)
                return None
            if st.base_version > version:
                return None  # 缓存基线比所需快照更新，不能反向打补丁
            adj = dict(st.adj)
            merged = 0
            # 版本号按每次资料变动 +1 连续递增；区间内任一补丁缺失即放弃增量
            for v in range(st.base_version + 1, int(version) + 1):
                patch = st.patches.get(v)
                if patch is None:
                    return None
                adj[patch.entry_id] = patch.edges
                merged += 1
            if count:
                self.stats["incremental_hits"] += 1
                self.stats["patches_merged"] += merged
                # 压缩：把基线推进到本次构造的版本并丢弃已合入补丁。
                # 之后的核查只需合入真正的新增量，连续改录也不会积压无限日志。
                # 已交出去的 adj 是独立 dict，不受这里保存副本的影响。
                if merged > 0:
                    st.adj = dict(adj)
                    st.base_version = int(version)
                    st.patches = {
                        v: p for v, p in st.patches.items() if v > int(version)
                    }
            # entries/docs 只读共享；adj 已复制，当前核查独占此 dict
            return st.entries, st.docs, adj

    def note_full_read(self) -> None:
        with self._lock:
            self.stats["full_reads"] += 1

    # ---------- 观测 ----------

    def info(self) -> dict:
        with self._lock:
            matters = []
            for mid, st in sorted(self._matters.items()):
                matters.append({
                    "matter_id": mid,
                    "base_version": st.base_version,
                    "patch_versions": sorted(st.patches),
                    "head_version": max([st.base_version, *st.patches.keys()]),
                    "entries_cached": len(st.entries),
                    "documents_cached": len(st.docs),
                })
            return {"stats": dict(self.stats), "matters": matters}


# 进程级单例：「同一进程中」复用
graph_cache = GraphCache()


def reset_graph_cache() -> None:
    graph_cache.clear()


# 测试/运维中 drop_all()/create_all() 重建表后自动清空，防止同 id 串图
def _clear_on_ddl(*_args, **_kwargs):
    graph_cache.clear()


event.listen(Base.metadata, "after_drop", _clear_on_ddl)
event.listen(Base.metadata, "after_create", _clear_on_ddl)
