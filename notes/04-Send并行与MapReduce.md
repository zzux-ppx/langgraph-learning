# 04 - Send 并行与 MapReduce

> 配套代码：`code/04_send_map_reduce.py`

静态并行（一个节点多条静态出边）在**编译期**就定死了分支数。
而 Agent 场景最常见的并行是**运行期才知道**分几个分支：
"先让 LLM 拆出 N 个子问题，再并行研究，最后汇总"。
这就是 `Send` 的用武之地。

## 1. Send：动态并行边

```python
from langgraph.types import Send

def fan_out(state: State) -> list[Send]:
    # 为每个子任务创建一个 Send，指到同一个节点，但带各自的"私有输入"
    return [Send("worker", {"task": t}) for t in state["tasks"]]

builder.add_conditional_edges("planner", fan_out)
```

两个关键点：

1. **`Send("worker", input)` 的 input 不是"状态更新"，而是该次节点调用的完整入参**，
   它会**完全替换**传给 worker 的 state（worker 看到的是 `{"task": t}`，不是全局 State！）
2. 多个 Send 指向**同一个节点**时，该节点在同一个超步被**并发调用多次**，
   每个分支持有一份独立的 state 视图。

### 常见误解

- ❌ "worker 节点还能读到全局 state" —— 不能，Send 的 input 就是它看到的全部。
  需要全局信息时，把它们一起塞进 Send 的 input dict。
- ❌ "Send 会创建新的 thread/checkpoint" —— 不会，仍在同一 checkpoint 线程内。

## 2. 完整 MapReduce 骨架

```python
import operator
from typing import Annotated, TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.types import Send

class State(TypedDict):
    topic: str
    tasks: list[str]                                # map 阶段产出
    results: Annotated[list[str], operator.add]     # reduce 靠 reducer 收集！

def planner(state):                       # 真实场景：LLM 结构化输出拆任务
    return {"tasks": ["子问题1", "子问题2", "子问题3"]}

def worker(state):                        # 注意：签名用 Send input 的结构
    return {"results": [f"关于「{state['task']}」的结论"]}

def fan_out(state):
    return [Send("worker", {"task": t}) for t in state["tasks"]]

builder = StateGraph(State)
builder.add_node("planner", planner)
builder.add_node("worker", worker)
builder.add_edge(START, "planner")
builder.add_conditional_edges("planner", fan_out)   # 动态扇出
builder.add_edge("worker", "aggregator")            # 所有 worker 完成后汇合
builder.add_node("aggregator", lambda s: {"summary": "\n".join(s["results"])})
builder.add_edge("aggregator", END)
```

**Reduce 的本质**：3 个 worker 各返回 `{"results": [一条]}`，
`operator.add` reducer 把三份增量拼成完整列表——**汇合逻辑写在了 State 定义里**，
这就是"数据结构决定控制流"的精髓。

## 3. 汇合（join）时机

所有指向 `aggregator` 的入边都完成后，aggregator 才在下一个超步执行。
LangGraph 的超步同步语义天然实现了 barrier 汇合，无需手写 `asyncio.gather`。

## 4. 不同 worker 不同任务？Send 指向不同节点

```python
def route(state):
    sends = []
    for t in state["tasks"]:
        target = "web_worker" if t.kind == "web" else "db_worker"
        sends.append(Send(target, {"task": t}))
    return sends
```

## 5. Send vs 静态并行 vs Command 对比

| | 分支数确定时机 | 分支看到的状态 | 典型场景 |
|---|---|---|---|
| 静态多出边 | 编译期 | 同一份全局 state | 固定流水线 |
| 条件边（返回列表） | 运行期 | 同一份全局 state | 按状态选 1..N 条已知路径 |
| **Send** | 运行期 | **每个分支独立 input** | MapReduce、多路检索、评审团 |
| Command(goto) | 运行期（节点内部） | 全局 state + update | 笔记 05 |

## 6. 常见坑

1. **worker 返回的 key 不在全局 State 里**：Send 只改入参，不改输出契约。
   worker 的返回值仍按全局 State 的 reducer 合并。
2. **worker 想读全局上下文**：必须由 fan_out 函数复制进 Send input。
3. **依赖顺序**：Send 出去的分支之间不应相互依赖（它们并行跑，顺序不定）。
4. **异常**：任一 worker 抛异常，整个图失败（可给 worker 配 retry 或在节点内 try）。
5. Send 不能指向 `START`，也不建议 Send 给有复杂入边的节点。

## 7. 动手实验清单

1. 把 worker 改成 `async def` 并 `sleep(1)`，10 个任务仍只花 ~1s（并发验证）。
2. 让 fan_out 根据任务长度决定丢弃短任务（`[]` 时直接 END 的处理）。
3. 给 results 换成 `add_messages` reducer，收集带 id 的消息。
