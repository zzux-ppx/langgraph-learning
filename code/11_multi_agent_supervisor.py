"""
11_multi_agent_supervisor.py — Supervisor 多Agent（笔记11 配套）
================================================================
一个"内容生产小队"：
    supervisor(路由) ──▶ researcher(子图agent) ──▶ supervisor
                    ──▶ writer(子图agent)    ──▶ supervisor
                    ──▶ FINISH
全部离线可跑：LLM 决策用规则模拟（真实接入点有注释）。

覆盖知识点：
  Supervisor路由(Command) + 子图当节点 + 共享messages + hop计数防死循环

运行:  python code/11_multi_agent_supervisor.py
"""

from typing import Annotated, Literal, TypedDict
import operator

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.types import Command


# ---------------------------------------------------------------- 全局共享状态
class TeamState(MessagesState):
    next: str                                          # supervisor 的路由决定
    notes: Annotated[list[str], operator.add]          # 团队共享笔记
    hops: Annotated[int, operator.add]                 # 防死循环计数


# ---------------------------------------------------------------- 子图A：researcher
class ResearcherState(MessagesState):
    notes: Annotated[list[str], operator.add]


def r_plan(state: ResearcherState) -> dict:
    q = next((m.content for m in reversed(state["messages"]) if m.type == "human"), "")
    return {"messages": [AIMessage("researcher: 收到任务，先拆解问题")]}


def r_collect(state: ResearcherState) -> dict:
    # 真实场景：这里挂工具调用循环（见 project/nodes/researcher.py 完整版）
    return {"notes": ["事实1: LangGraph是图编排框架", "事实2: 支持持久化"],
            "messages": [AIMessage("researcher: 资料收集完毕")]}


def build_researcher() -> StateGraph:
    g = StateGraph(ResearcherState)
    g.add_node("r_plan", r_plan)
    g.add_node("r_collect", r_collect)
    g.add_edge(START, "r_plan")
    g.add_edge("r_plan", "r_collect")
    g.add_edge("r_collect", END)
    return g


# ---------------------------------------------------------------- 子图B：writer
# 注意：writer 采用"包装函数"方式而非共享state子图 —— 这是踩坑后的选择：
# 共享state子图完成时会把它读过的所有共享字段经父图reducer回写一次，
# writer 只读 notes 不写，却会让 operator.add 的 notes 翻倍！
# （add_messages 幂等所以 messages 不受影响；详见笔记06的坑表）

class WriterState(MessagesState):
    material: list[str]                    # 显式声明入参契约


def w_draft(state: WriterState) -> dict:
    material = "; ".join(state.get("material", []))
    return {"messages": [AIMessage("writer: 基于材料成稿 -> 《" + material[:40] + "...》")]}


def build_writer() -> StateGraph:
    g = StateGraph(WriterState)
    g.add_node("w_draft", w_draft)
    g.add_edge(START, "w_draft")
    g.add_edge("w_draft", END)
    return g


_writer_subgraph = build_writer().compile()


def writer_node(state: TeamState) -> dict:
    """包装函数（方式B）：显式控制进出字段，只回写新增的 messages。"""
    n_known = len(state["messages"])
    out = _writer_subgraph.invoke({"messages": state["messages"],
                                   "material": state.get("notes", [])})
    return {"messages": out["messages"][n_known:]}   # 只取子图新增部分


# ---------------------------------------------------------------- Supervisor
SUPERVISOR_SYS = "你是队长，根据进度决定下一步: researcher / writer / FINISH"


def supervisor(state: TeamState) -> Command[Literal["researcher", "writer", "__end__"]]:
    """真实场景：把 SUPERVISOR_SYS + 进度交给 LLM 做结构化输出路由。
    这里用规则模拟 LLM 决策。"""
    hops = state.get("hops", 0)
    has_notes = bool(state.get("notes"))
    asks = sum(1 for m in state["messages"] if m.type == "human")
    drafts = sum(1 for m in state["messages"]
                 if m.type == "ai" and m.content.startswith("writer:"))

    if hops > 8:                                       # 防死循环硬闸
        return Command(goto="__end__", update={"next": "FINISH(超限)"})
    if not has_notes:
        decision = "researcher"
    elif drafts < asks:                                # 每个新需求都要新稿
        decision = "writer"
    else:
        decision = "__end__"
    return Command(goto=decision, update={"next": decision, "hops": 1})


# ---------------------------------------------------------------- 组装主图
def build_team() -> StateGraph:
    b = StateGraph(TeamState)
    b.add_node("supervisor", supervisor)
    b.add_node("researcher", build_researcher().compile())   # 子图当节点(共享state)
    b.add_node("writer", writer_node)                        # 包装函数(显式契约)
    b.add_edge(START, "supervisor")
    b.add_edge("researcher", "supervisor")                   # 干完活回队长
    b.add_edge("writer", "supervisor")
    return b


def main() -> None:
    print("== Supervisor 多Agent：researcher -> writer -> FINISH ==")
    graph = build_team().compile(checkpointer=InMemorySaver())
    cfg = {"configurable": {"thread_id": "team-1"}}

    init = {
        "messages": [SystemMessage(SUPERVISOR_SYS),
                     HumanMessage("写一篇LangGraph简介")],
        "notes": [], "hops": 0, "next": "",
    }
    out = graph.invoke(init, cfg)

    print("\n-- 执行轨迹(messages) --")
    for m in out["messages"]:
        who = m.type
        print(f"  [{who}] {m.content[:60]}")
    print("\n最终 next =", out["next"], "| hops =", out["hops"])
    print("共享笔记:", out["notes"])

    print("\n== 续跑：追加需求（同thread记忆 + hops继续累计）==")
    out2 = graph.invoke({"messages": [HumanMessage("再补充一版结论")]}, cfg)
    print("  新稿:", out2["messages"][-1].content[:50])
    print("  追加后 hops =", out2["hops"], "| next =", out2["next"])


if __name__ == "__main__":
    main()

    # 动手实验：
    # 1) supervisor 规则里加 "reviewer" 角色（挑毛病->回researcher），形成评审环
    # 2) 把 researcher 换成 Command(goto=[Send(...)]) 并行双检索，notes 用 reducer 汇
    # 3) 在 supervisor 前加 interrupt()：每次路由前人工确认（深度HITL）
