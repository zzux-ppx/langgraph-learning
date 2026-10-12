"""
pregel_superstep_trace.py — 用真 LangGraph 的 debug 流观察 Pregel 超步机制
=======================================================================
配合 notes/12 阅读。观察点：
  1. langgraph_step      : 超步编号（同一超步内的并行节点编号相同）
  2. langgraph_triggers  : 该任务被哪些通道触发（branch:to:<node> / join:...）
  3. 并行节点同超步执行、barrier 汇合
  4. recursion_limit 数的是超步数（并行分支只 +1）

运行:  python code/deep_dive/pregel_superstep_trace.py
"""

from typing import Annotated, TypedDict
import operator

from langgraph.graph import StateGraph, START, END
from langgraph.types import Send


class State(TypedDict):
    x: str
    parts: Annotated[list[str], operator.add]      # 并行写入必须靠 reducer


def source(state: State) -> dict:
    return {"x": "数据"}


def fan_out(state: State) -> list[Send]:
    # 动态并行：3 个 worker 分支（运行期决定数量）
    return [Send("worker", {"wid": i}) for i in range(3)]


def worker(state: dict) -> dict:
    # Send 分支看到的 state 是 Send.arg（不是全局 State！）
    return {"parts": [f"分支{state['wid']}的产出"]}


def join(state: State) -> dict:
    return {"x": "汇总:" + ",".join(state["parts"])}


def build() -> "StateGraph":
    b = StateGraph(State)
    b.add_node("source", source)
    b.add_node("worker", worker)
    b.add_node("join", join)
    b.add_edge(START, "source")
    b.add_conditional_edges("source", fan_out)
    b.add_edge("worker", "join")       # barrier：所有 worker 完成后 join 才执行
    b.add_edge("join", END)
    return b


def main() -> None:
    graph = build().compile()

    print("=" * 62)
    print("任务事件（debug 流）：超步 / 节点 / 触发通道")
    print("=" * 62)
    # 1.2.x 的 debug 流里 task 事件按"波次"到达：同一超步的所有 task 连续出现，
    # 中间夹着上一波任务的 task_result —— 以此划分超步边界。
    step, seen_result = 0, False
    for ev in graph.stream({"x": "", "parts": []}, stream_mode="debug"):
        if ev["type"] not in ("task", "task_result"):
            continue
        if ev["type"] == "task_result":
            seen_result = True
            continue
        if seen_result:
            step += 1
            seen_result = False
        p = ev["payload"]
        print(f"  超步 {step:<2} | 节点 {p['name']:<8} | triggers={p.get('triggers', ())}")

    print()
    print("=" * 62)
    print("结论对照：")
    print("  · 3 个 worker 同超步且 triggers=__pregel_push -> Send 是 PUSH 任务")
    print("  · source 的 triggers=branch:to:source -> 印证笔记12：")
    print("    每个节点有私有触发通道，边=往目标通道写信号")
    print("  · recursion_limit 数超步：本图 3 个任务波次只算 3 个超步")

    # 破坏性对照实验：把 parts 改成无 reducer 的 str，并行写入直接报错
    print()
    print("=" * 62)
    print("对照实验：无 reducer 时并行写入（1.x 快速失败）")

    class BadState(TypedDict):
        parts: str                      # 覆盖语义：同超步两路写入 => 冲突

    def worker_bad(state: dict) -> dict:
        return {"parts": f"分支{state['wid']}"}

    b = StateGraph(BadState)
    b.add_node("source", lambda s: {"parts": "init"})        # 用 dict 签名避免schema冲突
    b.add_node("worker", worker_bad)
    b.add_edge(START, "source")
    b.add_conditional_edges("source", lambda s: [Send("worker", {"wid": i}) for i in range(3)])
    b.add_edge("worker", END)
    try:
        out = b.compile().invoke({"parts": ""})
        print("  结果:", out["parts"])
    except Exception as e:
        print(f"  抛出 {type(e).__name__}: {str(e).splitlines()[0]}")
        print("  -> 0.x 静默覆盖(结果看运气)；1.x 直接报错强制你声明 reducer")


if __name__ == "__main__":
    main()
