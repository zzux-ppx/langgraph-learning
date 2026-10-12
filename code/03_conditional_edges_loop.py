"""
03_conditional_edges_loop.py — 条件边、循环与递归限制（笔记03 配套）
===================================================================
演示：
  1. 条件边 add_conditional_edges + path_map
  2. ReAct 式循环（工作 -> 检查 -> 不合格则回炉）
  3. recursion_limit 与 GraphRecursionError 排错
  4. 多目标条件边（返回列表 => 分流再汇合）

运行:  python code/03_conditional_edges_loop.py
"""

from typing import Annotated, TypedDict
import operator

from langgraph.errors import GraphRecursionError
from langgraph.graph import StateGraph, START, END


class State(TypedDict):
    draft: str
    score: int
    rounds: Annotated[int, operator.add]      # 循环轮次计数（reducer 累加）


def write_draft(state: State) -> dict:
    n = state["rounds"] + 1
    quality = 60 + 15 * (n - 1)                # 第3轮(90分)才过85分线
    return {"draft": f"第{n}版草稿", "score": min(quality, 95), "rounds": 1}


def review(state: State) -> dict:
    print(f"  [review] 审阅 {state['draft']} (score={state['score']})")
    return {}


def should_loop(state: State) -> str:
    """路由函数：只做选择，不做计算。"""
    return "write" if state["score"] < 85 else END


def build() -> StateGraph:
    b = StateGraph(State)
    b.add_node("write", write_draft)
    b.add_node("review", review)
    b.add_edge(START, "write")
    b.add_edge("write", "review")
    b.add_conditional_edges(
        "review",
        should_loop,
        {"write": "write", END: END},         # path_map：文档化 + 静态可查
    )
    return b


def demo_loop() -> None:
    print("== 1. 质检回炉循环 ==")
    graph = build().compile()
    out = graph.invoke({"draft": "", "score": 0, "rounds": 0})
    print("  最终:", out)


def demo_recursion_error() -> None:
    print("\n== 2. 死循环与 GraphRecursionError 排错 ==")

    def never_exit(state: State) -> str:
        return "write"                        # 永远不合格 => 死循环

    b = StateGraph(State)
    b.add_node("write", write_draft)
    b.add_node("review", review)
    b.add_edge(START, "write")
    b.add_edge("write", "review")
    b.add_conditional_edges("review", never_exit)
    graph = b.compile()

    config = {"recursion_limit": 6}           # 故意调小，快点触发
    try:
        graph.invoke({"draft": "", "score": 0, "rounds": 0}, config)
    except GraphRecursionError:
        pass

    # 排错姿势：在流式循环里捕获，看最后几步卡在哪
    print(f"  触发 GraphRecursionError（limit={config['recursion_limit']}）")
    steps = []
    try:
        for ev in graph.stream({"draft": "", "score": 0, "rounds": 0},
                               config, stream_mode="updates"):
            steps.append(ev)
    except GraphRecursionError:
        pass
    print("  最后两次更新:", steps[-2:])
    print("  -> 死循环根因：never_exit 永远路由回 write，出口条件不可达；")
    print("     带 checkpointer 时可用 graph.get_state(config) 看卡住时状态")


def demo_multi_route() -> None:
    print("\n== 3. 条件边一次去多个节点（分流再汇合）==")

    class S(TypedDict):
        q: str
        parts: Annotated[list[str], operator.add]

    def router(state: S) -> list[str]:
        # 返回列表：同超步并发去往多个节点
        return ["translate", "summarize"] if "both" in state["q"] else ["summarize"]

    b = StateGraph(S)
    b.add_node("translate", lambda s: {"parts": ["已翻译"]})
    b.add_node("summarize", lambda s: {"parts": ["已摘要"]})
    b.add_node("merge", lambda s: {"parts": [f"合并({len(s['parts'])}路):" + "+".join(s["parts"])]})
    b.add_conditional_edges(START, router)
    b.add_edge("translate", "merge")
    b.add_edge("summarize", "merge")          # barrier 汇合
    b.add_edge("merge", END)
    out = b.compile().invoke({"q": "please both handle", "parts": []})
    print("  汇合结果:", out["parts"])


if __name__ == "__main__":
    demo_loop()
    demo_recursion_error()
    demo_multi_route()

    # 动手实验：
    # 1) 把合格线从 75 调到 95，数一数轮次变化
    # 2) should_loop 返回 ["write", "log"]，加一个 log 节点观察同超步并发
    # 3) 给 write 节点加 retry=RetryPolicy(max_attempts=2)，在节点里 raise 试试
