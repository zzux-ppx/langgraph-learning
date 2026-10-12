"""
09_human_in_the_loop.py — interrupt 人工审批（笔记09 配套）
===========================================================
演示：
  1. interrupt() 暂停 + Command(resume=...) 恢复（必须配 checkpointer）
  2. get_state(...).tasks 查看待审批 payload
  3. resume 携带 update（审批人顺手改数据）
  4. interrupt_before 给既有图无侵入加闸门
  5. 同一节点多个 interrupt 逐个应答

运行:  python code/09_human_in_the_loop.py
"""

from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.types import Command, interrupt


class State(MessagesState):
    plan: str
    approved: str


def make_plan(state: State) -> dict:
    return {"plan": "计划: 1)调研 2)写报告 3)发布"}


def human_approval(state: State) -> dict:
    # 打断点：payload 是展示给审批者的任意 JSON
    answer = interrupt({
        "question": "以下计划是否批准执行?",
        "plan": state["plan"],
        "risk": "low",
    })
    # ↓ 只有 resume 之后才会走到这里，answer 即 Command(resume=...) 的值
    print("    [审批节点] 收到人工回复:", repr(answer))
    verdict = "通过" if answer == "yes" else "否决"
    return {"approved": verdict, "messages": [("assistant", "审批结果已记录")]}


def take_action(state: State) -> dict:
    if state["approved"] != "通过":
        return {"messages": [("assistant", "计划被否，流程结束")]}
    summary = "已按既定计划完成执行: {}".format(state["plan"])
    return {"messages": [("assistant", summary)]}


def build() -> StateGraph:
    b = StateGraph(State)
    b.add_node("make_plan", make_plan)
    b.add_node("approval", human_approval)
    b.add_node("take_action", take_action)
    b.add_edge(START, "make_plan")
    b.add_edge("make_plan", "approval")
    b.add_edge("approval", "take_action")
    b.add_edge("take_action", END)
    return b


def demo_basic() -> None:
    print("== 1. interrupt + resume 基本流 ==")
    graph = build().compile(checkpointer=InMemorySaver())
    cfg = {"configurable": {"thread_id": "hitl-1"}}

    graph.invoke({"messages": [HumanMessage("帮我做个方案")], "plan": "", "approved": ""}, cfg)

    snap = graph.get_state(cfg)
    payload = snap.tasks[0].interrupts[0].value      # 待审批内容
    print("  ⏸ 图已暂停。审批队列 payload:", payload)

    print("  -- 模拟人工回复 yes --")
    out = graph.invoke(Command(resume="yes"), cfg)
    print("  最终:", out["messages"][-1].content, "| approved =", out["approved"])


def demo_reject_with_update() -> None:
    print("\n== 2. 否决 + resume 时顺手改数据 ==")
    graph = build().compile(checkpointer=InMemorySaver())
    cfg = {"configurable": {"thread_id": "hitl-2"}}
    graph.invoke({"messages": [HumanMessage("做个方案")], "plan": "", "approved": ""}, cfg)

    out = graph.invoke(
        Command(resume="no", update={"plan": "计划: 1)调研 2)写报告 3)领导复审 4)发布"}),
        cfg)
    print("  approved =", out["approved"])
    print("  修订后的 plan 已生效:", "领导复审" in out["plan"])


def demo_interrupt_before() -> None:
    print("\n== 3. interrupt_before 无侵入闸门 ==")
    # 单独搭一个不含 interrupt() 节点的图，演示"纯图级"暂停
    b = StateGraph(State)
    b.add_node("make_plan", make_plan)
    b.add_node("take_action", take_action)
    b.add_edge(START, "make_plan")
    b.add_edge("make_plan", "take_action")
    b.add_edge("take_action", END)
    graph = b.compile(checkpointer=InMemorySaver(),
                      interrupt_before=["take_action"])
    cfg = {"configurable": {"thread_id": "hitl-3"}}
    graph.invoke({"messages": [], "plan": "原始计划", "approved": "通过"}, cfg)

    snap = graph.get_state(cfg)
    print("  暂停在:", snap.next, "(take_action 之前)")
    graph.update_state(cfg, {"plan": "人工修订版计划"})   # 人工改数据
    out = graph.invoke(None, cfg)                          # None = 放行继续
    print("  放行后:", out["messages"][-1].content)


def demo_multi_interrupt() -> None:
    print("\n== 4. 同一节点两个 interrupt，逐个应答 ==")

    class S(MessagesState):
        ok1: str
        ok2: str

    def double_check(state: S) -> dict:
        a = interrupt({"q": "第一关: 数据源对吗?"})
        b = interrupt({"q": "第二关: 金额对吗?"})
        summary = "{}/{}".format(a, b)
        return {"ok1": str(a), "ok2": str(b), "messages": [("assistant", summary)]}

    b = StateGraph(S)
    b.add_node("check", double_check)
    b.add_edge(START, "check")
    b.add_edge("check", END)
    graph = b.compile(checkpointer=InMemorySaver())
    cfg = {"configurable": {"thread_id": "hitl-4"}}

    graph.invoke({"messages": [], "ok1": "", "ok2": ""}, cfg)
    print("  待答:", graph.get_state(cfg).tasks[0].interrupts[0].value)
    graph.invoke(Command(resume="yes"), cfg)
    print("  待答:", graph.get_state(cfg).tasks[0].interrupts[0].value)
    out = graph.invoke(Command(resume="no"), cfg)
    print("  结果 ok1/ok2 =", out["ok1"], "/", out["ok2"])


if __name__ == "__main__":
    demo_basic()
    demo_reject_with_update()
    demo_interrupt_before()
    demo_multi_interrupt()

    # 动手实验：
    # 1) resume 传一个 dict {"decision":"yes","comment":"..."} 并在节点解包
    # 2) interrupt_before 放行前用 update_state 把 approved 改成"否决"
    # 3) 暂停期间另开进程用同 thread_id get_state（InMemory 不行，换 SqliteSaver）
