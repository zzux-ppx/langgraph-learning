"""
planner.py — 规划节点：主题 -> 子课题（结构化输出的典型用法）
"""

from langgraph.config import get_stream_writer

from project.llm import get_brain


def planner(state: dict) -> dict:
    writer = get_stream_writer()
    writer({"phase": "planning", "msg": f"拆解课题: {state['topic']}"})

    brain = get_brain()
    subtopics = brain.plan(state["topic"], state.get("user_context") or {})

    writer({"phase": "planning", "msg": f"拆出 {len(subtopics)} 个子课题"})
    return {
        "subtopics": subtopics,
        "messages": [("assistant", f"[planner] 课题拆解: {subtopics}")],
    }
