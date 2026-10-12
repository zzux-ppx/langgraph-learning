"""
05_command_navigation.py — Command 导航（笔记05 配套）
=====================================================
演示：
  1. Command(update + goto)：节点内"既改状态又指路"
  2. Command(goto=[...])：一次派多个节点（含 Send）
  3. Supervisor 路由雏形 + 返回类型注解 Command[Literal[...]]
  4. 对比：Command 节点 + 静态出边 => 双跳陷阱

运行:  python code/05_command_navigation.py
"""

from typing import Annotated, Literal, TypedDict
import operator

from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.types import Command, Send


class State(MessagesState):
    phase: str
    notes: Annotated[list[str], operator.add]


def supervisor(state: State) -> Command[Literal["researcher", "writer", "__end__"]]:
    """决策即节点：根据当前材料决定交给谁。真实场景由 LLM 输出决定。"""
    if not state.get("notes"):
        return Command(goto="researcher", update={"phase": "研究中"})
    if len(state["notes"]) < 2:
        return Command(goto="researcher", update={"phase": "补充研究中"})
    if "报告" not in " ".join(state["notes"]):
        return Command(goto="writer", update={"phase": "写作中"})
    return Command(goto="__end__", update={"phase": "完成"})


def researcher(state: State) -> dict:
    return {"notes": [f"素材{len(state['notes']) + 1}"]}


def writer(state: State) -> dict:
    return {"notes": ["报告:基于" + "、".join(state["notes"])]}


def demo_supervisor() -> None:
    print("== 1. Command 版 Supervisor（researcher x2 -> writer -> END）==")
    b = StateGraph(State)
    b.add_node("supervisor", supervisor)
    b.add_node("researcher", researcher)
    b.add_node("writer", writer)
    b.add_edge(START, "supervisor")
    for ag in ("researcher", "writer"):
        b.add_edge(ag, "supervisor")            # 干完活回 supervisor
    graph = b.compile()
    out = graph.invoke({"messages": [], "phase": "", "notes": []})
    print("  phase:", out["phase"])
    print("  notes:", out["notes"])


def demo_goto_many() -> None:
    print("\n== 2. Command(goto=[...]) 同超步派多个节点 ==")

    class S(TypedDict):
        done: Annotated[list[str], operator.add]

    def dispatcher(state: S) -> Command:
        return Command(
            update={"done": ["调度完毕"]},
            goto=["task_a", "task_b"],           # 并行
        )

    b = StateGraph(S)
    b.add_node("dispatcher", dispatcher)
    b.add_node("task_a", lambda s: {"done": ["A完成"]})
    b.add_node("task_b", lambda s: {"done": ["B完成"]})
    b.add_node("join", lambda s: {"done": ["join看到:" + ",".join(s["done"])]})
    b.add_edge(START, "dispatcher")
    b.add_edge("task_a", "join")
    b.add_edge("task_b", "join")
    b.add_edge("join", END)
    out = b.compile().invoke({"done": []})
    print("  结果:", out["done"])


def demo_goto_send() -> None:
    print("\n== 3. Command(goto=[Send...]) 动态并行 ==")

    class S(TypedDict):
        items: list[str]
        results: Annotated[list[str], operator.add]

    def fan(state: S) -> Command:
        return Command(goto=[Send("upper", {"s": x}) for x in state["items"]])

    b = StateGraph(S)
    b.add_node("fan", fan)
    b.add_node("upper", lambda s: {"results": [s["s"].upper()]})
    b.add_edge(START, "fan")
    b.add_edge("upper", END)
    out = b.compile().invoke({"items": ["lang", "graph", "666"], "results": []})
    print("  结果:", out["results"])


def demo_double_jump() -> None:
    print("\n== 4. 双跳陷阱：Command 节点 + 静态出边 ==")

    class S(TypedDict):
        trace: Annotated[list[str], operator.add]

    def decide(state: S) -> Command:
        return Command(goto="left", update={"trace": ["decide选了left"]})

    b = StateGraph(S)
    b.add_node("decide", decide)
    b.add_node("left", lambda s: {"trace": ["走了left"]})
    b.add_node("right", lambda s: {"trace": ["走了right(不该出现!)"]})
    b.add_edge(START, "decide")
    b.add_edge("decide", "right")               # ← 静态出边与 goto 同时生效！
    b.add_edge("left", END)
    b.add_edge("right", END)
    out = b.compile().invoke({"trace": []})
    print("  结果:", out["trace"])
    print("  -> left 和 right 都执行了：goto 是'追加'不是'覆盖'，别混用！")


if __name__ == "__main__":
    demo_supervisor()
    demo_goto_many()
    demo_goto_send()
    demo_double_jump()

    # 动手实验：
    # 1) 把 supervisor 的阈值改成 len<4，观察多跑两轮
    # 2) 删掉 demo4 的静态出边，验证只走 left
    # 3) supervisor 改成条件边版本，对比两种写法的代码量与可测性
