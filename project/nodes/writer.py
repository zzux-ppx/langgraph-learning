"""
writer.py — 撰稿节点：汇总 findings 成报告
"""

from langgraph.config import get_stream_writer

from project.llm import get_brain


def writer(state: dict) -> dict:
    w = get_stream_writer()
    n = len(state.get("findings") or [])
    w({"phase": "writing", "msg": f"汇总 {n} 份研究结论，撰写报告"})

    brain = get_brain()
    report = brain.write(state["topic"], state.get("findings") or [],
                         state.get("user_context") or {})

    w({"phase": "writing", "msg": "报告初稿完成，送审"})
    return {"report": report,
            "messages": [("assistant", f"[writer] 报告已生成({len(report)}字)，待审阅")]}
