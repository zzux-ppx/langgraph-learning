"""
graph.py — 主图组装：深度研究多Agent系统
=========================================

拓扑总览：

                         ┌──────────────┐
    START ─▶ load_memory ─▶  supervisor │◀─────────────────┐
                         └──┬───┬───┬───┘                   │
              Command(goto) │   │   │                       │
                 ┌──────────┘   │   └──────────┐            │
                 ▼              ▼ Send xN      ▼            │
             planner        researcher      writer ─────────┤
                 │          (工具循环子图)      (汇总成稿)     │
                 └──回supervisor─┘    │          │           │
                                     ▼          ▼           │
                                 findings汇合  reviewer(AI初审+人工interrupt)
                                                             │
                                                    通过 ▶ save_memory ▶ END

LangGraph 能力清单（本图全覆盖）：
  ✔ StateGraph+自定义reducer  ✔ Command导航  ✔ Send动态并行
  ✔ 子图(researcher/reviewer)  ✔ ToolNode工具循环  ✔ 条件边循环
  ✔ checkpointer持久化/断点续跑  ✔ interrupt人工审批  ✔ Store长期记忆
  ✔ input/output契约裁剪        ✔ recursion_limit/防环计数
"""

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.store.memory import InMemoryStore

from project.nodes.memory import load_memory, save_memory
from project.nodes.planner import planner
from project.nodes.researcher import build_researcher
from project.nodes.reviewer import build_reviewer
from project.nodes.supervisor import supervisor
from project.nodes.writer import writer
from project.state import ResearchInput, ResearchOutput, ResearchState


def build_graph():
    b = StateGraph(ResearchState, input=ResearchInput, output=ResearchOutput)

    b.add_node("load_memory", load_memory)
    b.add_node("supervisor", supervisor)
    b.add_node("planner", planner)
    b.add_node("researcher", build_researcher().compile())   # 子图当节点
    b.add_node("writer", writer)
    b.add_node("reviewer", build_reviewer().compile())       # 子图当节点
    b.add_node("save_memory", save_memory)

    b.add_edge(START, "load_memory")
    b.add_edge("load_memory", "supervisor")
    # 各角色干完活都回 supervisor（Command 已处理 planner 的去向）
    b.add_edge("planner", "supervisor")
    b.add_edge("researcher", "supervisor")
    b.add_edge("writer", "supervisor")
    b.add_edge("reviewer", "supervisor")
    b.add_edge("save_memory", END)

    return b.compile(
        checkpointer=InMemorySaver(),      # 持久化：多轮记忆/中断恢复/时间旅行
        store=InMemoryStore(),             # 长期记忆：跨会话用户档案
    )


def default_config(thread_id: str = "research-1", user_id: str = "u1") -> dict:
    return {
        "configurable": {"thread_id": thread_id, "user_id": user_id},
        "recursion_limit": 60,             # 多层子图循环，留足超步余量
    }
