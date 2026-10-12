"""
06_subgraph.py — 子图（笔记06 配套）
====================================
演示：
  1. 方式A：共享 State —— 编译后的图直接当节点
  2. 方式B：包装函数 —— 显式做父子状态映射（生产推荐）
  3. 子图内部细节透传：stream(subgraphs=True)
  4. 子图内 interrupt 也能恢复（入口在 09 篇，这里看结构）

运行:  python code/06_subgraph.py
"""

from typing import Annotated, TypedDict
import operator

from langgraph.graph import StateGraph, START, END, MessagesState


# ---------------------------------------------------------- 子图：一个mini agent
class ChildState(MessagesState):
    answer: str


def child_think(state: ChildState) -> dict:
    q = state["messages"][-1].content
    return {"answer": f"关于[{q}]的深度和答案"}


def build_child() -> StateGraph:
    c = StateGraph(ChildState)
    c.add_node("think", child_think)
    c.add_edge(START, "think")
    c.add_edge("think", END)
    return c


child_graph = build_child().compile()          # 编译产物本身就是 Runnable => 可当节点


# ------------------------------------------------- 方式A：共享 State（字段同名）
class ParentState(MessagesState):
    answer: str                              # 与子图共享 messages / answer


def demo_shared_state() -> None:
    print("== 1. 方式A: 共享 State（messages/answer 同名直通）==")
    p = StateGraph(ParentState)
    p.add_node("child", child_graph)         # 直接塞进去
    p.add_node("after", lambda s: {"answer": s["answer"] + "（父图后处理）"})
    p.add_edge(START, "child")
    p.add_edge("after", END)
    p.add_edge("child", "after")
    out = p.compile().invoke({"messages": [("user", "什么是Pregel")], "answer": ""})
    print("  answer:", out["answer"])


# ------------------------------------- 方式B：包装函数（显式状态映射，生产推荐）
class ParentState2(TypedDict):
    raw_question: str                        # 字段名与子图完全不同
    final_answer: str


def demo_wrapper() -> None:
    print("\n== 2. 方式B: 包装函数做状态映射 ==")

    def call_child(state: ParentState2) -> dict:
        child_out = child_graph.invoke({"messages": [("user", state["raw_question"])],
                                        "answer": ""})
        answer = child_out["answer"]
        return {"final_answer": "[" + answer + "]"}

    p = StateGraph(ParentState2)
    p.add_node("agent", call_child)
    p.add_edge(START, "agent")
    p.add_edge("agent", END)
    out = p.compile().invoke({"raw_question": "什么是BSP模型", "final_answer": ""})
    print("  final_answer:", out["final_answer"])
    print("  -> 父子完全解耦：子图改 schema 不影响父图")


# ----------------------------------------------------- 透传子图内部事件
def demo_subgraph_stream() -> None:
    print("\n== 3. subgraphs=True 透传子图内部事件 ==")

    p = StateGraph(ParentState)
    p.add_node("child", child_graph)
    p.add_edge(START, "child")
    p.add_edge("child", END)
    graph = p.compile()

    print("  [不开 subgraphs] 只看到子图整体一个 update：")
    for ev in graph.stream({"messages": [("user", "Q")], "answer": ""},
                           stream_mode="updates"):
        print("   ", ev)

    print("  [开 subgraphs=True] 能看到子图内部 think 节点：")
    for ev in graph.stream({"messages": [("user", "Q")], "answer": ""},
                           stream_mode="updates", subgraphs=True):
        print("   ", ev)


if __name__ == "__main__":
    demo_shared_state()
    demo_wrapper()
    demo_subgraph_stream()

    # 动手实验：
    # 1) 把 ParentState2 的包装函数改成异步，父图节点用 async def
    # 2) 子图里加一个循环节点，父图 recursion_limit 设 5 观察共享计数
    # 3) 子图 think 里调用 interrupt()，父图跑起来看暂停点（结合 09 篇）
