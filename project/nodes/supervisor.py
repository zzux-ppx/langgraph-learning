"""
supervisor.py — 监督者：整个系统的"交通指挥"
=============================================
用到：Command 导航（update+goto 原子绑定）、Send 动态并行派发、防环计数。
决策逻辑委托给 brain.supervisor_route（可换成 LLM 结构化输出）。
"""

from typing import Literal

from langgraph.types import Command, Send

from project.llm import get_brain

MAX_HOPS = 20          # 防死循环硬闸
MAX_REVIEW_ROUNDS = 2  # 审阅最多回炉次数


def supervisor(state: dict) -> Command[Literal["planner", "researcher", "writer",
                                               "reviewer", "save_memory", "__end__"]]:
    brain = get_brain()
    hops = state.get("hops", 0)

    if hops > MAX_HOPS:
        return Command(goto="__end__", update={"route": "end(超限)"})

    phase = brain.supervisor_route(state)

    if phase == "plan":
        return Command(goto="planner", update={"route": "plan", "hops": 1})

    if phase == "research":
        # 增量派发：只 Send 尚未完成的子课题（已完成的结论在 findings 里）
        done = {f.get("subtopic") for f in state.get("findings") or []}
        remaining = [s for s in state["subtopics"] if s not in done]
        sends = [Send("researcher", {
            "subtopic": s,
            "messages": [("user", f"请深入研究: {s}")],
            "turn": 0,
        }) for s in remaining]
        return Command(goto=sends, update={"route": f"research(x{len(sends)})", "hops": 1})

    if phase == "write":
        return Command(goto="writer", update={"route": "write", "hops": 1})

    if phase == "review":
        rounds = state.get("review_rounds", 0)
        if rounds >= MAX_REVIEW_ROUNDS:
            # 回炉次数用尽：强制通过（生产可改为转人工/降级发布）
            return Command(goto="save_memory",
                           update={"route": "save(审阅超限强制通过)",
                                   "approved": "yes", "hops": 1})
        # 轮次计数在这里累加（Command update 走 add reducer），
        # 并通过 round_hint 告知子图（子图不回写 review_rounds，防漂移）
        return Command(goto="reviewer",
                       update={"route": "review", "hops": 1,
                               "review_rounds": 1, "round_hint": rounds + 1})

    if phase == "save":
        return Command(goto="save_memory", update={"route": "save", "hops": 1})

    return Command(goto="__end__", update={"route": "end", "hops": 1})
