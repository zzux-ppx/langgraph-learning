"""
02_state_reducer.py — 状态与 Reducer（笔记02 配套）
=================================================
演示：
  1. 默认覆盖(LastValue) vs operator.add(追加) vs 自定义 reducer
  2. add_messages 的"同 id 替换 / RemoveMessage 删除"语义
  3. input/output schema 裁剪对外接口
  4. 并行节点写同名字段：有 reducer 才安全

运行:  python code/02_state_reducer.py
"""

import operator
from typing import Annotated, TypedDict

from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.graph.message import add_messages


# --------------------------------------------------------------- 1. 三种合并策略
class State(TypedDict):
    text: str                                          # 覆盖
    logs: Annotated[list[str], operator.add]           # 追加（纯函数拼接）
    best: Annotated[float, "keep_max"]                 # 自定义（见下方定义）


def keep_max(old: float | None, new: float) -> float:
    return new if old is None else max(old, new)


State.__annotations__["best"] = Annotated[float, keep_max]   # 运行期挂 reducer 亦可


def node_x(state: State) -> dict:
    return {"text": "X", "logs": ["x跑了"], "best": 0.7}


def node_y(state: State) -> dict:
    return {"text": "Y", "logs": ["y跑了"], "best": 0.9}


def demo_reducers() -> None:
    print("== 1. 覆盖 / 追加 / 自定义 reducer ==")
    b = StateGraph(State)
    b.add_node("x", node_x)
    b.add_node("y", node_y)
    b.add_edge(START, "x")
    b.add_edge("x", "y")
    b.add_edge("y", END)
    out = b.compile().invoke({"text": "", "logs": [], "best": 0.0})
    print("  串行 x->y 结果:", out)
    # text 被覆盖为 "Y"；logs 追加成 ['x跑了','y跑了']；best 取 max=0.9

    print("\n== 1b. 并行写同名字段：无 reducer 会怎样？ ==")
    b2 = StateGraph(State)
    b2.add_node("x", node_x)
    b2.add_node("y", node_y)
    b2.add_edge(START, "x")
    b2.add_edge(START, "y")       # x、y 同超步并行
    b2.add_edge("x", END)
    b2.add_edge("y", END)
    try:
        b2.compile().invoke({"text": "", "logs": [], "best": 0.0})
    except Exception as e:        # langgraph.errors.InvalidUpdateError
        print(f"  1.x 行为：直接报错 -> {type(e).__name__}")
        print("  ", str(e).splitlines()[0])
        print("  -> 框架强制你显式声明合并策略：给 text 加 reducer 才能并行写")
        print("     （0.x 时代是静默覆盖、结果看运气；1.x 改为快速失败，更安全）")


# --------------------------------------------------------------- 2. add_messages
def demo_add_messages() -> None:
    print("\n== 2. add_messages 的特殊语义 ==")

    class S(MessagesState):        # 内置 messages 字段 + 自定义字段
        round: int

    def talk(state: S) -> dict:
        # 同 id 再发一条 => 替换而不是追加
        return {"messages": [AIMessage(content="v2修正版", id="m1")], "round": 1}

    def trim(state: S) -> dict:
        # RemoveMessage => 删除
        return {"messages": [RemoveMessage(id="m1")]}

    b = StateGraph(S)
    b.add_node("seed", lambda s: {"messages": [
        HumanMessage(content="你好", id="h1"),
        AIMessage(content="v1初始版", id="m1"),
    ]})
    b.add_node("talk", talk)
    b.add_node("trim", trim)
    b.add_edge(START, "seed")
    b.add_edge("seed", "talk")
    b.add_edge("talk", "trim")
    b.add_edge("trim", END)
    out = b.compile().invoke({"messages": [], "round": 0})
    for m in out["messages"]:
        print(f"  剩余消息: [{m.type}] {m.content}")
    # m1 被"替换"为 v2 后又被"删除"，最终只剩 h1


# --------------------------------------------------------------- 3. input/output
def demo_input_output() -> None:
    print("\n== 3. input/output schema 接口裁剪 ==")

    class Internal(State):        # 内部还有脏字段
        debug: str

    class In(TypedDict):
        text: str

    class Out(TypedDict):
        logs: list[str]

    b = StateGraph(Internal, input=In, output=Out)
    b.add_node("work", lambda s: {"logs": [f"处理了{s['text']}"], "debug": "内部细节"})
    b.add_edge(START, "work")
    b.add_edge("work", END)
    graph = b.compile()
    out = graph.invoke({"text": "hello"})
    print("  对外输出:", out)
    print("  -> debug 字段被 output schema 挡住；多余输入字段也会被 input 挡住")


if __name__ == "__main__":
    demo_reducers()
    demo_add_messages()
    demo_input_output()

    # 动手实验：
    # 1) 写 keep_last_n(old, new) 只保留最近 N 条日志
    # 2) 给 text 也加上 Annotated[str, lambda o, n: f"{o}|{n}"]，看串并差异
    # 3) 在 node_x 里原地 state["logs"].append("hack")，观察图外列表是否被污染
