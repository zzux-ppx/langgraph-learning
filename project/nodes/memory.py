"""
memory.py — 长期记忆节点：入口召回 / 出口沉淀（Store 能力）
============================================================
load_memory : START 后第一站，把该用户的长期记忆注入 user_context
save_memory : END 前最后一站，把本次研究摘要沉淀进 Store
"""

from langchain_core.runnables import RunnableConfig
from langgraph.store.base import BaseStore

from project.llm import get_brain


def load_memory(state: dict, config: RunnableConfig, *, store: BaseStore) -> dict:
    uid = config["configurable"].get("user_id", "anon")
    hits = store.search(("users", uid, "research"), query=state.get("topic", ""))
    ctx = {f"mem{i}": h.value["text"] for i, h in enumerate(hits)}
    if ctx:
        print(f"    [load_memory] 召回 {len(ctx)} 条长期记忆")
    return {"user_context": ctx or {}}


def save_memory(state: dict, config: RunnableConfig, *, store: BaseStore) -> dict:
    uid = config["configurable"].get("user_id", "anon")
    digest = {
        "text": f"研究过《{state['topic']}》，报告{len(state.get('report', ''))}字，"
                f"结论{len(state.get('findings') or [])}份",
        "topic": state["topic"],
    }
    store.put(("users", uid, "research"), state["topic"].replace(" ", "_")[:40], digest)
    print(f"    [save_memory] 已沉淀研究记录: {digest['text']}")
    return {"messages": [("assistant", "[memory] 研究记录已归档")]}
