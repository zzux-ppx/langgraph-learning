"""
08_store_memory.py — Store 长期记忆（笔记08 配套）
==================================================
演示：
  1. store.put/get/search/delete + namespace 设计
  2. 节点注入 BaseStore（参数名 store + 类型注解）
  3. 跨 thread（跨会话）记忆：A 会话写入，B 会话召回
  4. 会话结束"提炼记忆"的收尾节点模式

运行:  python code/08_store_memory.py
"""

from typing import Annotated, TypedDict
import operator

from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.store.base import BaseStore
from langgraph.store.memory import InMemoryStore


class State(MessagesState):
    recalled: Annotated[list[str], operator.add]


def agent(state: State, config, *, store: BaseStore) -> dict:
    """读记忆：会话开始时召回长期记忆。注意 store 参数：名字+注解缺一不可。"""
    uid = config["configurable"]["user_id"]
    hits = store.search(("users", uid, "facts"), query=state["messages"][-1].content)
    recalled = [f"[记忆] {h.value['text']}" for h in hits]
    reply = f"(结合{len(recalled)}条记忆作答)" if recalled else "(无历史记忆)"
    return {"recalled": recalled, "messages": [("assistant", reply)]}


def save_memory(state: State, config, *, store: BaseStore) -> dict:
    """写记忆：会话结束时提炼。真实场景由 LLM 判断'什么值得记'。"""
    uid = config["configurable"]["user_id"]
    last_user = next((m.content for m in reversed(state["messages"])
                      if m.type == "human"), "")
    if "喜欢" in last_user or "叫" in last_user:      # 演示用关键词'提炼'
        store.put(("users", uid, "facts"), f"note_{len(last_user)}",
                  {"text": f"用户曾说过: {last_user}", "src": "session-end"})
        print(f"    [save_memory] 已存: {last_user!r}")
    return {}


def build() -> StateGraph:
    b = StateGraph(State)
    b.add_node("agent", agent)
    b.add_node("save_memory", save_memory)
    b.add_edge(START, "agent")
    b.add_edge("agent", "save_memory")
    b.add_edge("save_memory", END)
    return b


def main() -> None:
    store = InMemoryStore()
    graph = build().compile(checkpointer=InMemorySaver(), store=store)

    print("== 会话1（thread=chat-1）：首次自我介绍 ==")
    cfg1 = {"configurable": {"thread_id": "chat-1", "user_id": "alice"}}
    graph.invoke({"messages": [HumanMessage("我叫小明，我喜欢简洁的中文回答")],
                  "recalled": []}, cfg1)

    print("\n== 会话2（thread=chat-2，同 user=alice）：跨会话召回 ==")
    cfg2 = {"configurable": {"thread_id": "chat-2", "user_id": "alice"}}
    out = graph.invoke({"messages": [HumanMessage("我之前说过我喜欢什么来着?")],
                        "recalled": []}, cfg2)
    print("  回答:", out["messages"][-1].content)
    for r in out["recalled"]:
        print("  ", r)

    print("\n== 直接操作 store（管理视角） ==")
    for item in store.search(("users", "alice", "facts")):
        print(f"  key={item.key} value={item.value}")
    got = store.get(("users", "alice", "facts"), list(
        store.search(("users", "alice", "facts")))[0].key)
    print("  get =>", got.value["text"][:20], "...")

    print("\n== 换用户 bob（namespace 隔离） ==")
    cfg3 = {"configurable": {"thread_id": "chat-3", "user_id": "bob"}}
    out3 = graph.invoke({"messages": [HumanMessage("我喜欢什么来着?")], "recalled": []}, cfg3)
    print("  bob 的回答:", out3["messages"][-1].content, "(应为'无历史记忆')")

    # 动手实验：
    # 1) 给 InMemoryStore 配 index={"embed": 函数, "dims": n}，验证语义排序
    # 2) save_memory 改成 LLM 提炼（'这句话有长期价值吗？'）
    # 3) namespace 加一层 ("users", uid, "facts", "2026-10") 按月归档


if __name__ == "__main__":
    main()
