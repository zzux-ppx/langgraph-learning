"""
mini_langgraph.py — 手写的迷你 LangGraph（与 Pregel 模型同构）
================================================================
配合 notes/12、notes/13 阅读。零依赖（纯标准库），实现了：

  1. Channel 两类：LastValue(覆盖) / BinaryOperatorAggregate(reducer) + 单调版本号
  2. 超步循环(BSP)：版本比较调度 -> 线程池并发执行 -> 写入合并 -> barrier 同步
  3. 静态边 / 条件边 / Send 动态并行(PUSH 任务)
  4. 内存 checkpoint（每超步一个快照）+ 时间旅行(fork 历史快照)
  5. interrupt() / Command(resume=...) 人工中断-恢复
  6. API 同构：add_node/add_edge/add_conditional_edges/compile().invoke(...)

与真源码的对应关系见各段注释中的 "对应 ..."。
运行:  python code/deep_dive/mini_langgraph.py
"""

from __future__ import annotations

import copy
import operator
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Annotated, Any, Callable, TypedDict, get_type_hints

START, END = "__start__", "__end__"


# ---------------------------------------------------------------------------
# Channel 层（对应 langgraph/channels/{last_value,binop}.py）
# ---------------------------------------------------------------------------

class LastValue:
    """无 Annotated 的字段：保留最后一次写入（覆盖语义）。"""

    def __init__(self) -> None:
        self.value: Any = None
        self.has_value = False

    def update(self, values: list[Any]) -> bool:
        if not values:
            return False
        self.value, self.has_value = values[-1], True
        return True


class BinaryOperatorAggregate:
    """Annotated[T, reducer]：逐个应用 reducer。首值直接赋值（old 为 None 的兜底）。"""

    def __init__(self, op: Callable[[Any, Any], Any]) -> None:
        self.op = op
        self.value: Any = None
        self.has_value = False

    def update(self, values: list[Any]) -> bool:
        if not values:
            return False
        for v in values:
            self.value = self.op(self.value, v) if self.has_value else v
            self.has_value = True
        return True


def _channels_from_schema(schema: type) -> dict[str, Any]:
    """TypedDict schema -> channels（对应 StateGraph._get_channels）。

    真源码用 get_type_hints(include_extras=True) 提取 Annotated 元数据：
      x: str                            -> LastValue
      x: Annotated[list[str], operator.add] -> BinaryOperatorAggregate(operator.add)
    """
    channels: dict[str, Any] = {}
    for key, tp in get_type_hints(schema, include_extras=True).items():
        meta = getattr(tp, "__metadata__", ())          # Annotated 的附加项
        reducer = meta[0] if meta and callable(meta[0]) else None
        channels[key] = BinaryOperatorAggregate(reducer) if reducer else LastValue()
    return channels


# ---------------------------------------------------------------------------
# 构建器层（对应 langgraph/graph/state.py 的 StateGraph）
# ---------------------------------------------------------------------------

@dataclass
class Send:
    """对应 langgraph.types.Send：动态并行 PUSH 任务。"""
    node: str
    arg: Any


@dataclass
class Command:
    """对应 langgraph.types.Command：既更新又导航；resume 用于恢复中断。"""
    update: dict | None = None
    goto: str | list[str] | list[Send] | None = None
    resume: Any = None


class GraphInterrupt(Exception):
    """对应 langgraph.errors.GraphInterrupt。"""
    def __init__(self, payload: Any):
        self.payload = payload


_RESUME: dict[str, Any] = {}          # thread_id -> 待消费的 resume 值
_CT = threading.local()               # 节点执行线程的上下文（真框架用 contextvars）


def interrupt(payload: Any) -> Any:
    """对应 langgraph.types.interrupt：节点内调用；无 resume 值则抛中断。

    有 resume 值时返回它（代码从断点继续），否则抛 GraphInterrupt。
    """
    tid = getattr(_CT, "tid", None)
    if tid is not None and tid in _RESUME:
        return _RESUME.pop(tid)                     # 恢复：消费掉 resume 值
    raise GraphInterrupt(payload)


class MiniStateGraph:
    def __init__(self, schema: type) -> None:
        self.schema = schema
        self.nodes: dict[str, Callable] = {}
        self.edges: list[tuple[str, str]] = []          # (start, end)
        self.conds: list[tuple[str, Callable]] = []     # (start, 路由函数)

    def add_node(self, name: str, func: Callable) -> "MiniStateGraph":
        self.nodes[name] = func
        return self

    def add_edge(self, start: str, end: str) -> "MiniStateGraph":
        self.edges.append((start, end))
        return self

    def add_conditional_edges(self, start: str, router: Callable) -> "MiniStateGraph":
        self.conds.append((start, router))
        return self

    def compile(self) -> "MiniPregel":
        if not any(s == START for s, _ in self.edges + self.conds):
            raise ValueError("图缺少入口：需要 add_edge(START, ...)")
        for node in {e for _, e in self.edges}:
            if node not in self.nodes and node != END:
                raise ValueError(f"边指向了未注册节点: {node}")
        return MiniPregel(self)


# ---------------------------------------------------------------------------
# 运行时层（对应 langgraph/pregel/{main,_loop,_algo}.py）
# ---------------------------------------------------------------------------

@dataclass
class Checkpoint:
    """对应 langgraph.checkpoint.base.Checkpoint 的核心字段。

    sig_ver 即 channel_versions（含触发通道版本）；seen 即 versions_seen。
    """
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    step: int = 0
    channels: dict[str, Any] = field(default_factory=dict)      # channel 对象
    chan_ver: dict[str, int] = field(default_factory=dict)      # 数据通道版本
    sig_ver: dict[str, int] = field(default_factory=dict)       # 触发通道版本(to:node)
    seen: dict[str, int] = field(default_factory=dict)          # node -> 上次看到的 to:self 版本
    mailbox: list[Send] = field(default_factory=list)           # TASKS 通道里的 Send
    pending_interrupt: Any = None


    def values(self) -> dict:
        """channels -> state 视图（对应 get_state().values）。"""
        return {k: c.value for k, c in self.channels.items() if getattr(c, "has_value", False)}


class MiniPregel:
    def __init__(self, builder: MiniStateGraph, max_workers: int = 8) -> None:
        self.b = builder
        self.history: dict[str, list[Checkpoint]] = {}          # thread_id -> 快照(旧->新)
        self._pool = ThreadPoolExecutor(max_workers=max_workers)

    # ---- 对外 API（对应 Pregel.invoke/get_state/get_state_history）----------

    def invoke(self, inputs: dict | Command | None, config: dict | None = None) -> dict:
        cfg = (config or {}).get("configurable", {})
        thread = cfg.get("thread_id", "t0")
        limit = (config or {}).get("recursion_limit", 25)

        if isinstance(inputs, Command) and inputs.resume is not None:
            _RESUME[thread] = inputs.resume                   # 供 interrupt() 消费
            inputs = None
        if inputs is None:
            cid = cfg.get("checkpoint_id")                    # 时间旅行：指定历史快照
            hist = self.history.get(thread)
            if not hist:
                raise RuntimeError("线程无历史状态，无法续跑/时间旅行")
            ckpt = copy.deepcopy(hist[-1] if cid is None else
                                  next(c for c in hist if c.id == cid))
        else:
            ckpt = Checkpoint(channels=_channels_from_schema(self.b.schema))
            self._apply_input(ckpt, inputs)                   # 写入输入 + 发 START 信号

        result = self._loop(ckpt, thread, limit)
        return result

    def get_state(self, config: dict) -> Checkpoint:
        thread = (config or {}).get("configurable", {}).get("thread_id", "t0")
        return copy.deepcopy(self.history[thread][-1])

    def get_state_history(self, config: dict) -> list[Checkpoint]:
        thread = (config or {}).get("configurable", {}).get("thread_id", "t0")
        return list(reversed(self.history[thread]))           # 新 -> 旧

    def fork(self, config: dict, new_thread: str, checkpoint_id: str | None = None) -> Checkpoint:
        """时间旅行：把某历史快照复制到新线程（真框架=新thread_id+旧checkpoint_id）。

        不传 checkpoint_id 则取最新快照。
        """
        thread = (config or {}).get("configurable", {}).get("thread_id", "t0")
        hist = self.history[thread]
        snap = copy.deepcopy(next((c for c in hist if c.id == checkpoint_id), hist[-1]))
        self.history.setdefault(new_thread, []).append(snap)
        return snap

    # ---- 超步主循环（对应 _loop.py::tick）----------------------------------

    def _loop(self, ckpt: Checkpoint, thread: str, limit: int) -> dict:
        while True:
            # ① 调度：PUSH(先消费 Send 信箱) + PULL(触发通道版本比较)
            #    对应 _algo.py::prepare_next_tasks / _triggers
            tasks: list[tuple[str, Any]] = [(s.node, s.arg) for s in ckpt.mailbox]
            ckpt.mailbox = []
            scheduled: dict[str, int] = {}   # 本超步各节点"此刻看到"的触发版本
            for name in self.b.nodes:
                if ckpt.sig_ver.get(f"to:{name}", 0) > ckpt.seen.get(name, 0):
                    tasks.append((name, None))                # None -> 读全量 state
                    scheduled[name] = ckpt.sig_ver.get(f"to:{name}", 0)

            if not tasks:
                return self._read_state(ckpt)                 # 无活跃任务 -> 自然结束

            ckpt.step += 1
            if ckpt.step > limit:
                raise RuntimeError(f"GraphRecursionError: 超过 {limit} 个超步")

            # ② 并发执行（真源码为 asyncio task group；这里用线程池演示并发）
            futs = [self._pool.submit(self._run_node, n, arg, ckpt, thread) for n, arg in tasks]
            results = [f.result() for f in futs]

            # ③ 写入阶段：interrupt 优先处理（丢弃写入，对应"超步事务回滚"）
            #    简化点：真框架只丢弃被中断任务的写入，同超步其他任务照常提交
            if any(r.interrupted for r in results):
                ckpt.pending_interrupt = next(r.payload for r in results if r.interrupted)
                self._save(thread, ckpt)
                print(f"  ⏸ [mini] 超步{ckpt.step} 触发 interrupt，图已暂停（写入已丢弃）")
                return self._read_state(ckpt)
            ckpt.pending_interrupt = None                        # 中断已被消费

            new_sends: list[Send] = []
            ran: set[str] = set()
            for r in results:
                ran.add(r.name)
                for key, val in (r.update or {}).items():     # 数据写入走 reducer
                    if key in ckpt.channels and ckpt.channels[key].update([val]):
                        ckpt.chan_ver[key] = ckpt.chan_ver.get(key, 0) + 1
                if r.goto:                                    # Command 导航
                    for g in r.goto if isinstance(r.goto, list) else [r.goto]:
                        if isinstance(g, Send):
                            new_sends.append(g)
                        elif g == END:
                            ckpt.mailbox = []
                            self._save(thread, ckpt)
                            return self._read_state(ckpt)
                        else:
                            self._signal(ckpt, g)

            # 静态边：a 执行完就给下游发信号（1.x 真实现=往 branch:to:b 通道写）
            for start, end in self.b.edges:
                if start in ran and end != END:
                    self._signal(ckpt, end)

            # 条件边：路由函数在"写入阶段"评估（对应 _control_branch）
            state_view = self._read_state(ckpt)
            for start, router in self.b.conds:
                if start in ran:
                    route = router(state_view)
                    for target in route if isinstance(route, list) else [route]:
                        if isinstance(target, Send):
                            new_sends.append(target)
                        elif target == END:
                            ckpt.mailbox = []
                            self._save(thread, ckpt)
                            return self._read_state(ckpt)
                        else:
                            self._signal(ckpt, target)

            ckpt.mailbox = new_sends                          # Send 进 TASKS 信箱

            # ④ 提交 versions_seen：用"调度时刻"记录的版本（不是写入后的！）
            #    —— 若在写入阶段才记录，本超步新发的信号会被自己"看见"而丢失触发
            ckpt.seen.update(scheduled)
            # ⑤ barrier：存快照，进入下一超步
            self._save(thread, ckpt)
            print(f"  ▣ [mini] 超步{ckpt.step}: {sorted(ran) or ['仅Send任务']}")

    # ---- 节点执行包装（对应 RunnableCallable + GraphInterrupt 捕获）----------

    @dataclass
    class _NodeResult:
        name: str
        update: dict | None
        goto: Any
        interrupted: bool
        payload: Any

    def _run_node(self, name: str, arg: Any, ckpt: Checkpoint, thread: str) -> _NodeResult:
        _CT.tid = thread                                        # 供 interrupt() 定位线程
        try:
            state_arg = arg if arg is not None else self._read_state(ckpt)
            out = self.b.nodes[name](state_arg)               # ← 你的节点函数
            if isinstance(out, Command):
                return self._NodeResult(name, out.update, out.goto, False, None)
            return self._NodeResult(name, out or {}, None, False, None)
        except GraphInterrupt as gi:                          # 写入全部丢弃
            return self._NodeResult(name, None, None, True, gi.payload)

    # ---- 通道/信号辅助 -------------------------------------------------------

    def _apply_input(self, ckpt: Checkpoint, inputs: dict) -> None:
        for key, val in inputs.items():                       # 对应 START 的 writer
            if key in ckpt.channels:
                ckpt.channels[key].update([val])
                ckpt.chan_ver[key] = ckpt.chan_ver.get(key, 0) + 1
        for start, end in self.b.edges:
            if start == START:
                self._signal(ckpt, end)
        for start, router in self.b.conds:
            if start == START:
                for target in router(inputs) if not isinstance(router(inputs), list) else router(inputs):
                    if target != END:
                        self._signal(ckpt, target)

    def _signal(self, ckpt: Checkpoint, target: str) -> None:
        """给目标节点的私有触发通道 bump 版本（对应写 branch:to:<node>）。"""
        key = f"to:{target}"
        ckpt.sig_ver[key] = ckpt.sig_ver.get(key, 0) + 1

    def _read_state(self, ckpt: Checkpoint) -> dict:
        """channels -> state 视图（对应节点 mapper，每次从通道重新组装=值拷贝）。"""
        return {k: c.value for k, c in ckpt.channels.items() if getattr(c, "has_value", False)}

    def _save(self, thread: str, ckpt: Checkpoint) -> None:
        snap = copy.deepcopy(ckpt)
        snap.pending_interrupt = ckpt.pending_interrupt
        self.history.setdefault(thread, []).append(snap)


# ---------------------------------------------------------------------------
# 演示：跑完所有机制
# ---------------------------------------------------------------------------

def demo() -> None:

    class State(TypedDict, total=False):
        topic: str
        tasks: list[str]
        results: Annotated[list[str], operator.add]
        report: str
        rounds: Annotated[int, operator.add]

    # --- 演示1：静态链 + reducer ------------------------------------------------
    print("== 演示1: 静态链 ==")
    def a1(s): return {"rounds": 1, "report": "A"}
    def b1(s): return {"report": s["report"] + "+B"}

    g1 = MiniStateGraph(State)
    g1.add_node("a", a1).add_node("b", b1)
    g1.add_edge(START, "a"); g1.add_edge("a", "b"); g1.add_edge("b", END)
    out = g1.compile().invoke({"rounds": 0})
    print("  结果:", out, "(rounds 走 reducer 累加, report 被覆盖)\n")

    # --- 演示2：条件边循环（最多3轮）---------------------------------------------
    print("== 演示2: 条件边循环 ==")
    def work(s): return {"rounds": 1}
    def route(s): return "work" if s["rounds"] < 3 else END

    g2 = MiniStateGraph(State)
    g2.add_node("work", work)
    g2.add_edge(START, "work"); g2.add_conditional_edges("work", route)
    out = g2.compile().invoke({"rounds": 0})
    print("  结果: rounds =", out["rounds"], "\n")

    # --- 演示3：Send 动态并行（MapReduce）----------------------------------------
    print("== 演示3: Send 并行 MapReduce ==")
    def planner(s):
        return {"tasks": ["要点A", "要点B", "要点C", "要点D"]}
    def worker(s):                       # Send 任务看到的是 Send.arg，不是全局 state！
        time.sleep(0.2)
        return {"results": [f"结论({s['task']})"]}
    def fan_out(s):
        return [Send("worker", {"task": t}) for t in s["tasks"]]
    def aggregate(s):
        return {"report": " | ".join(s["results"])}

    g3 = MiniStateGraph(State)
    g3.add_node("planner", planner).add_node("worker", worker).add_node("aggregate", aggregate)
    g3.add_edge(START, "planner")
    g3.add_conditional_edges("planner", fan_out)
    g3.add_edge("worker", "aggregate")
    g3.add_edge("aggregate", END)
    t0 = time.time()
    out = g3.compile().invoke({"rounds": 0})
    dt = time.time() - t0
    print(f"  报告: {out['report']}")
    print(f"  4个worker各sleep(0.2)总耗时仅 {dt:.2f}s -> 真并发 ✅\n")

    # --- 演示4：interrupt + Command(resume) --------------------------------------
    print("== 演示4: interrupt/resume ==")
    def approval(s):
        answer = interrupt({"q": "是否通过报告?", "report": s.get("report", "")})
        return {"report": s.get("report", "") + f"[已审批:{answer}]"}
    def after(s): return {}

    g4 = MiniStateGraph(State)
    g4.add_node("approval", approval).add_node("after", after)
    g4.add_edge(START, "approval"); g4.add_edge("approval", "after"); g4.add_edge("after", END)
    cfg = {"configurable": {"thread_id": "approve-1"}}
    app = g4.compile()
    app.invoke({"report": "v1草稿"}, cfg)                    # 第一段：停在 interrupt
    snap = app.get_state(cfg)
    print("  暂停时 pending_interrupt =", snap.pending_interrupt)
    out = app.invoke(Command(resume="yes"), cfg)             # 第二段：恢复
    print("  恢复后 report =", out["report"], "\n")

    # --- 演示5：时间旅行 ----------------------------------------------------------
    print("== 演示5: 时间旅行(fork 历史快照) ==")
    cfg = {"configurable": {"thread_id": "approve-1"}}
    hist = app.get_state_history(cfg)
    print(f"  approve-1 线程共 {len(hist)} 个快照, 超步号: {[c.step for c in hist]}")
    oldest = hist[-1]                                          # 超步1 = 中断时刻的快照
    app.fork(cfg, "approve-1-fork", checkpoint_id=oldest.id)   # 从中断点 fork 新时间线
    out2 = app.invoke(Command(resume="no"), {"configurable": {"thread_id": "approve-1-fork"}})
    print("  fork线程 resume('no') -> report =", out2["report"])
    print("  原线程不受影响, report =", app.get_state(cfg).values()["report"])


if __name__ == "__main__":
    demo()
