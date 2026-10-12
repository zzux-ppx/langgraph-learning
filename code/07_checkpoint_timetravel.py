"""
07_checkpoint_timetravel.py — 持久化与时间旅行（笔记07 配套）
=============================================================
演示：
  1. checkpointer + thread_id => 多轮记忆
  2. get_state / get_state_history / update_state
  3. 时间旅行：回到历史快照 fork 新时间线
  4. 断点续跑：input=None 继续执行

运行:  python code/07_checkpoint_timetravel.py
"""

from typing import Annotated, TypedDict
import operator

from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph, START, END, MessagesState


class State(MessagesState):
    counter: Annotated[int, operator.add]
    note: str


def agent(state: State) -> dict:
    # 真实场景这里调 LLM；我们只做拼接以观察记忆效果
    seen = len(state["messages"])
    return {
        "messages": [("assistant", f"(第{seen}条消息已收到) 你上一句说: "
                     + (state["messages"][-1].content if state["messages"] else "无"))],
        "counter": 1,
    }


def build() -> StateGraph:
    b = StateGraph(State)
    b.add_node("agent", agent)
    b.add_edge(START, "agent")
    b.add_edge("agent", END)
    return b


def demo_memory() -> None:
    print("== 1. thread_id 即'会话记忆' ==")
    graph = build().compile(checkpointer=InMemorySaver())
    cfg = {"configurable": {"thread_id": "alice-1"}}

    graph.invoke({"messages": [HumanMessage("我叫小明，喜欢蓝色")]}, cfg)
    out = graph.invoke({"messages": [HumanMessage("我叫什么?")]}, cfg)
    print("  回答:", out["messages"][-1].content)
    out = graph.invoke({"messages": [HumanMessage("再问一遍?")]}, cfg)
    print("  回答:", out["messages"][-1].content)
    print("  -> 同 thread 记得全部历史；counter 每轮 +1 =", out["counter"])

    print("\n  [换 thread_id => 完全失忆]")
    cfg2 = {"configurable": {"thread_id": "bob-1"}}
    out2 = graph.invoke({"messages": [HumanMessage("我叫什么?")]}, cfg2)
    print("  bob 的回答:", out2["messages"][-1].content)


def demo_state_history() -> None:
    print("\n== 2. get_state / get_state_history / update_state ==")
    graph = build().compile(checkpointer=InMemorySaver())
    cfg = {"configurable": {"thread_id": "hist-1"}}
    graph.invoke({"messages": [HumanMessage("你好")]}, cfg)
    graph.invoke({"messages": [HumanMessage("第二句")]}, cfg)

    snap = graph.get_state(cfg)
    print("  当前 counter:", snap.values["counter"], "| next:", snap.next)
    print("  历史快照数:", len(list(graph.get_state_history(cfg))), "(每轮对话2个:输入态+输出态)")

    # 手动改状态：给消息里硬塞一条 assistant 发言
    graph.update_state(cfg, {"messages": [("assistant", "我被人手动改过")]})
    snap2 = graph.get_state(cfg)
    print("  update_state 后最后一条:", snap2.values["messages"][-1].content)


def demo_timetravel() -> None:
    print("\n== 3. 时间旅行：同线程回放 + update_state 搬运fork ==")
    graph = build().compile(checkpointer=InMemorySaver())
    cfg = {"configurable": {"thread_id": "tt-1"}}
    graph.invoke({"messages": [HumanMessage("第一句")]}, cfg)
    graph.invoke({"messages": [HumanMessage("第二句")]}, cfg)

    hist = list(graph.get_state_history(cfg))          # 新 -> 旧
    mid = next(s for s in hist if s.next == ("agent",))    # 挑一个中间快照
    print(f"  共{len(hist)}个快照，选中: 消息数={len(mid.values['messages'])}")

    # 方式1：同线程回放 —— 用快照自带 config + input=None 继续（覆盖后续"未来"）
    out = graph.invoke(None, mid.config)
    print("  回放后最后回答:", out["messages"][-1].content)

    # 方式2：真 fork —— 把快照状态搬运到新线程（InMemorySaver 按 thread 寻址，
    #         跨线程 + checkpoint_id 取不到旧快照，会被当成全新运行）
    fork_cfg = {"configurable": {"thread_id": "tt-fork"}}
    graph.update_state(fork_cfg, mid.values, as_node="agent")   # 种入历史状态
    out2 = graph.invoke({"messages": [HumanMessage("另一条世界线")]}, fork_cfg)
    print("  fork线最后回答:", out2["messages"][-1].content)
    print("  原线不受影响:", graph.get_state(cfg).values["messages"][-1].content)


def demo_resume() -> None:
    print("\n== 4. input=None：从暂停处继续 ==")
    # 构造一个'跑到一半停'的场景：update_state(as_node=...) 后图知道下一步是谁
    graph = build().compile(checkpointer=InMemorySaver())
    cfg = {"configurable": {"thread_id": "resume-1"}}
    graph.invoke({"messages": [HumanMessage("第一句")]}, cfg)

    graph.update_state(cfg, {"counter": 100}, as_node="agent")
    snap = graph.get_state(cfg)
    print("  伪装 agent 写入 counter=100, now next =", snap.next)
    out = graph.invoke(None, cfg)          # None = 不加新输入，继续执行
    print("  继续后 counter =", out["counter"], "(100+1，agent 又跑了一次)")


if __name__ == "__main__":
    demo_memory()
    demo_state_history()
    demo_timetravel()
    demo_resume()

    # 动手实验：
    # 1) 换 SqliteSaver（pip install langgraph-checkpoint-sqlite）重跑，重启进程验证落盘
    # 2) 时间旅行后改 input 再跑，对比两条线
    # 3) 把一个大 dict 放进 State，观察 checkpoint 体积膨胀
