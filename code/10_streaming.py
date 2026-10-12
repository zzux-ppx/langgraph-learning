"""
10_streaming.py — 五种流式模式（笔记10 配套）
==============================================
演示：
  1. values / updates / messages / custom / debug 五种 stream_mode
  2. get_stream_writer() 自定义进度事件
  3. 组合模式 stream_mode=[...] + subgraphs=True

说明：messages 模式需要真实流式 LLM 才有 token 级效果，这里用
      FakeStreamingModel 模拟逐 token 输出（langchain_core 自带假模型）。

运行:  python code/10_streaming.py
"""

import time
from typing import Annotated, TypedDict
import operator

from langgraph.config import get_stream_writer
from langgraph.graph import StateGraph, START, END, MessagesState


class State(MessagesState):
    progress: Annotated[list[str], operator.add]


def heavy_step(state: State) -> dict:
    writer = get_stream_writer()               # 无 stream 上下文时是 no-op，安全
    for i in range(3):
        time.sleep(0.1)
        writer({"phase": "heavy", "i": i})     # 自定义事件：业务进度
    return {"progress": ["heavy完成"], "messages": [("assistant", "重活干完了")]}


def llm_step(state: State) -> dict:
    # 真实场景：model = ChatOpenAI(...); return {"messages": [model.invoke(...)]}
    # messages 流式要求模型支持 streaming —— FakeListChatModel 支持
    return {"progress": ["llm完成"]}


def build() -> StateGraph:
    b = StateGraph(State)
    b.add_node("heavy", heavy_step)
    b.add_node("llm", llm_step)
    b.add_edge(START, "heavy")
    b.add_edge("heavy", "llm")
    b.add_edge("llm", END)
    return b


def demo_values_updates() -> None:
    graph = build().compile()
    print("== values：每超步后的完整 state ==")
    for i, snap in enumerate(graph.stream({"messages": [], "progress": []},
                                          stream_mode="values")):
        print(f"  快照{i}: progress={snap['progress']}")

    print("\n== updates：每节点的增量 ==")
    for ev in graph.stream({"messages": [], "progress": []}, stream_mode="updates"):
        print("  ", ev)


def demo_custom() -> None:
    graph = build().compile()
    print("\n== custom：自定义进度事件 ==")
    for ev in graph.stream({"messages": [], "progress": []}, stream_mode="custom"):
        print("  进度:", ev)


def demo_debug() -> None:
    graph = build().compile()
    print("\n== debug：内部事件（1.2.x 的 task 事件带 triggers）==")
    step, seen_result = 0, False
    for ev in graph.stream({"messages": [], "progress": []}, stream_mode="debug"):
        if ev["type"] == "task_result":
            seen_result = True
        elif ev["type"] == "task":
            if seen_result:
                step += 1            # 上波已出结果后又来新task => 新超步
                seen_result = False
            p = ev["payload"]
            print(f"  超步{step}: {p['name']:<6} triggers={p.get('triggers', ())}")
    print("  -> triggers 里的 branch:to:<node> 就是笔记12讲的'私有触发通道'")


def demo_messages_fake() -> None:
    """messages 模式演示：需要模型在节点内被流式调用。
    这里用 GenericFakeChatModel 展示 chunk 级输出（生产接 ChatOpenAI 即同款）。"""
    print("\n== messages：token 级流（Fake 模型模拟）==")
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
    from langchain_core.messages import AIMessage

    model = GenericFakeChatModel(messages=iter([AIMessage(content="你好，这是逐token的模拟回复")]))
    chunks = [c for c in model.stream("讲点什么")]
    print(f"  stream() 产生 {len(chunks)} 个 chunk，前3个: {[c.content for c in chunks[:3]]}")
    print("  说明：图中节点 return {'messages':[model.invoke(...)]} 时，")
    print("        graph.stream(..., stream_mode='messages') 会把这些 chunk 透传给调用方")
    print("        多Agent时用 meta['langgraph_node'] 区分是哪个节点在说话")


def demo_combined() -> None:
    graph = build().compile()
    print("\n== 组合模式 [updates, custom] ==")
    for chunk in graph.stream({"messages": [], "progress": []},
                              stream_mode=["updates", "custom"]):
        mode, payload = chunk
        print(f"  [{mode}] {payload}")


if __name__ == "__main__":
    demo_values_updates()
    demo_custom()
    demo_debug()
    demo_messages_fake()
    demo_combined()

    # 动手实验：
    # 1) 接入真实 ChatOpenAI 后跑 stream_mode="messages" 体验打字机
    # 2) 给 heavy_step 加 try/finally，验证 custom 事件在异常时也发出
    # 3) astream + asyncio 写一个并发消费两个 stream_mode 的循环
