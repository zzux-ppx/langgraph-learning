"""
reviewer.py — 审阅子图：AI初审 + 人工终审(interrupt)
====================================================
两层审阅：
    START ─▶ ai_review ─▶ human_gate(interrupt) ─▶ END

· ai_review   大脑给出机器审阅意见
· human_gate  interrupt() 暂停整个图，等待 Command(resume=...) 
              （演示"埋在子图深处的 HITL"，父图无需感知细节）

注意（踩坑经验，见学习笔记06）：子图完成时会把它读过的共享字段
经父图 reducer 回写，review_rounds 若放在子图里会被 operator.add
重复累计 —— 所以轮次计数由父图 supervisor 管理，本子图只写
approved / verdict_comment / report（LastValue 字段，幂等安全）。
"""

from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from project.llm import get_brain


class ReviewerState(TypedDict, total=False):
    report: str
    approved: str
    verdict_comment: str
    # 注意：这里【不能】声明 review_rounds（Annotated[add]）——
    # 子图完成回写会把继承值再加一次导致轮次漂移（见笔记06坑表）。
    # 轮次由父图 supervisor 维护，经 round_hint（LastValue，幂等）传入。
    round_hint: int


def ai_review(state: ReviewerState) -> dict:
    brain = get_brain()
    rounds = state.get("round_hint", 1) - 1        # 0-based
    verdict = brain.review(state.get("report", ""), rounds)
    comment = f"[AI初审·{verdict['ai_verdict']}] {verdict['comment']}"
    print(f"    {comment}")
    return {"verdict_comment": comment}


def human_gate(state: ReviewerState) -> dict:
    """人工终审：interrupt 暂停，resume 值即审批指令。"""
    answer = interrupt({
        "role": "最终审阅",
        "ai_opinion": state.get("verdict_comment", ""),
        "report_preview": (state.get("report", "") or "")[:300] + "...",
        "round": state.get("round_hint", 1),
        "hint": "回复 yes 通过 / no 打回重写",
    })
    approved = "yes" if str(answer).lower() in ("yes", "y", "是", "ok") else "no"
    print(f"    [human_gate] 人工终审: {approved}")
    return {"approved": approved}


def build_reviewer() -> StateGraph:
    g = StateGraph(ReviewerState)
    g.add_node("ai_review", ai_review)
    g.add_node("human_gate", human_gate)
    g.add_edge(START, "ai_review")
    g.add_edge("ai_review", "human_gate")
    g.add_edge("human_gate", END)
    return g
