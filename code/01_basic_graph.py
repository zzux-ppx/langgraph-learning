"""
01_basic_graph.py — 第一个 LangGraph 图（笔记01 配套）
====================================================
演示：
  1. StateGraph / State / Node / Edge / START / END
  2. 节点返回"增量更新"而非完整 state
  3. invoke / stream(values) / batch 三种运行方式
  4. get_graph() 查看图结构

运行:  python code/01_basic_graph.py
"""

from typing import TypedDict

from langgraph.graph import StateGraph, START, END


# 1) 定义状态：全图共享的"数据库"
class State(TypedDict):
    text: str
    steps: list[str]


# 2) 节点：普通函数，接收 state，返回【增量更新】
def step_a(state: State) -> dict:
    print(f"  [step_a] 收到 state: {state}")
    return {"text": state["text"] + " -> A", "steps": ["a"]}   # 只返回要改的字段


def step_b(state: State) -> dict:
    print(f"  [step_b] 收到 state: {state}")
    return {"text": state["text"] + " -> B", "steps": ["b"]}


# 3) 组装并编译
builder = StateGraph(State)
builder.add_node("a", step_a)
builder.add_node("b", step_b)
builder.add_edge(START, "a")     # 入口 -> a
builder.add_edge("a", "b")       # a -> b
builder.add_edge("b", END)       # b -> 出口
graph = builder.compile()


if __name__ == "__main__":
    print("== invoke：阻塞式运行 ==")
    result = graph.invoke({"text": "start", "steps": []})
    print("最终 state:", result)
    # 注意 steps 是 ['b'] 而不是 ['a','b']：默认 reducer 是覆盖！见 02 篇

    print("\n== stream(values)：每个超步后的完整状态 ==")
    for snap in graph.stream({"text": "start", "steps": []}, stream_mode="values"):
        print("  快照:", snap)

    print("\n== stream(updates)：每个节点的增量 ==")
    for update in graph.stream({"text": "start", "steps": []}, stream_mode="updates"):
        print("  更新:", update)

    print("\n== batch：批量并行 ==")
    outs = graph.batch([{"text": "甲", "steps": []}, {"text": "乙", "steps": []}])
    print("  批量结果:", outs)

    print("\n== 图结构（节点/边） ==")
    g = graph.get_graph()
    print("  nodes:", [n.name for n in g.nodes.values()])
    print("  edges:", [(e.source, e.target) for e in g.edges])

    # 动手实验：
    # 1) 把 add_edge("a","b") 换成 add_edge("a","END")，观察 b 不再执行
    # 2) 同时 add_edge("a","b") 和 add_edge("a","END")，观察并行分支
    # 3) 给节点加 config 参数打印 config["configurable"]
