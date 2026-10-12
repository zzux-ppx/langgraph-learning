"""
researcher.py — 研究员：一个完整的 ReAct 式工具调用 Agent 子图
================================================================
独立子图（有自己的 state），被主图通过 Send 动态并行调用 N 次。
内部结构 = 教科书式 agent 循环：

    START ─▶ agent ─┬─(有tool_calls)─▶ tools(ToolNode) ─▶ agent ...
                    └─(没有)──────────────────────▶ END

演示能力：子图 + 工具节点 + 条件边循环 + Send 并行实例化。
注意：schema 里的 turn 字段不进父图（父图无此 key，写回时被过滤）。
"""

from typing import Annotated, TypedDict

from langchain_core.messages import AIMessage
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
import operator

from project.llm import get_brain
from project.tools.research_tools import TOOLS


class ResearcherState(TypedDict, total=False):
    messages: Annotated[list, add_messages]
    subtopic: str
    turn: Annotated[int, operator.add]        # 子图内部计数（不回写父图）
    findings: Annotated[list[dict], operator.add]  # 与父图共享 => 汇合关键


MAX_TURNS = 4                                  # 研究员自身的循环安全阀


def agent_step(state: ResearcherState) -> dict:
    """LLM 一步：要么发起工具调用，要么给出最终结论。"""
    brain = get_brain()
    turn = state.get("turn", 0)
    writer = get_stream_writer()

    if turn >= MAX_TURNS:                      # 超限强制收敛
        return _finalize(state, "（达到工具调用上限，强制汇总）")

    writer({"phase": "research", "subtopic": state["subtopic"], "turn": turn})
    step = brain.research_step(state["subtopic"], state["messages"], turn)

    update = {"messages": [step["message"]], "turn": 1}
    if "finding" in step:
        update["findings"] = [step["finding"]]
        writer({"phase": "research-done", "subtopic": state["subtopic"]})
    return update


def _finalize(state: ResearcherState, note: str) -> dict:
    finding = {"subtopic": state["subtopic"], "summary": note, "sources": []}
    return {"messages": [AIMessage(content=note)],
            "findings": [finding], "turn": 1}


def should_continue(state: ResearcherState) -> str:
    last = state["messages"][-1]
    return "tools" if getattr(last, "tool_calls", None) else END


def build_researcher() -> StateGraph:
    g = StateGraph(ResearcherState)
    g.add_node("agent", agent_step)
    g.add_node("tools", ToolNode(TOOLS))       # 官方预置：执行tool_calls写ToolMessage
    g.add_edge(START, "agent")
    g.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    g.add_edge("tools", "agent")               # 工具结果回流 -> 再决策
    return g
