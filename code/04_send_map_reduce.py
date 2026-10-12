"""
04_send_map_reduce.py — Send 动态并行 / MapReduce（笔记04 配套）
================================================================
演示：
  1. planner 拆任务 -> Send 扇出 -> 并发 worker -> reducer 汇合
  2. Send 分支看到的是 Send.arg 而非全局 state
  3. 真·并发计时验证
  4. 不同任务路由到不同 worker 节点

运行:  python code/04_send_map_reduce.py
"""

import time
from typing import Annotated, TypedDict
import operator

from langgraph.graph import StateGraph, START, END
from langgraph.types import Send


class State(TypedDict):
    topic: str
    tasks: list[str]
    results: Annotated[list[str], operator.add]     # reduce 的关键！
    report: str


def planner(state: State) -> dict:
    """真实场景：这里调用 LLM 结构化输出拆子课题。"""
    return {"tasks": [
        f"{state['topic']}的原理",
        f"{state['topic']}的生态",
        f"{state['topic']}的坑",
        f"{state['topic']}的替代品",
    ]}


def fan_out(state: State) -> list[Send]:
    """动态并行边：运行期才知道几个分支。"""
    return [Send("worker", {"task": t, "topic": state["topic"]}) for t in state["tasks"]]


def worker(state: dict) -> dict:
    """注意签名：Send 分支的入参是 Send.arg（不是全局 State！）
    需要的全局信息（如 topic）必须由 fan_out 复制进 arg。"""
    print(f"    [worker] 研究中: {state['task']}")
    time.sleep(0.3)                                # 模拟耗时检索
    return {"results": [f"《{state['task']}》的结论"]}


def aggregate(state: State) -> dict:
    return {"report": "\n".join(f"- {r}" for r in state["results"])}


def demo_map_reduce() -> None:
    print("== 1. MapReduce（4路Send并行） ==")
    b = StateGraph(State)
    b.add_node("planner", planner)
    b.add_node("worker", worker)
    b.add_node("aggregate", aggregate)
    b.add_edge(START, "planner")
    b.add_conditional_edges("planner", fan_out)
    b.add_edge("worker", "aggregate")              # barrier：全部完成才汇合
    b.add_edge("aggregate", END)
    graph = b.compile()

    t0 = time.time()
    out = graph.invoke({"topic": "LangGraph", "tasks": [], "results": [], "report": ""})
    dt = time.time() - t0
    print(out["report"])
    print(f"4 个 worker 各 sleep(0.3)，总耗时 {dt:.2f}s（串行需 1.2s）-> 真并发 ✅")


def demo_heterogeneous() -> None:
    print("\n== 2. 按任务类型路由到不同 worker ==")

    class S(TypedDict):
        jobs: list[dict]
        done: Annotated[list[str], operator.add]

    def dispatch(state: S) -> list[Send]:
        sends = []
        for j in state["jobs"]:
            target = "fast_worker" if j["size"] == "small" else "slow_worker"
            sends.append(Send(target, j))
        return sends

    def fast_worker(j: dict) -> dict:
        return {"done": [f"快处理:{j['name']}"]}

    def slow_worker(j: dict) -> dict:
        return {"done": [f"慢处理:{j['name']}"]}

    b = StateGraph(S)
    b.add_node("fast_worker", fast_worker)
    b.add_node("slow_worker", slow_worker)
    b.add_conditional_edges(START, dispatch)
    b.add_edge("fast_worker", END)
    b.add_edge("slow_worker", END)
    out = b.compile().invoke({
        "jobs": [{"name": "a", "size": "small"}, {"name": "b", "size": "big"},
                 {"name": "c", "size": "small"}],
        "done": [],
    })
    print("  结果:", out["done"])


if __name__ == "__main__":
    demo_map_reduce()
    demo_heterogeneous()

    # 动手实验：
    # 1) 把 worker 改成 async def + asyncio.sleep，用 ainvoke 验证事件循环并发
    # 2) fan_out 里过滤掉带"生态"的任务，观察 results 减少
    # 3) 去掉 results 的 Annotated reducer，看并行结果丢失
