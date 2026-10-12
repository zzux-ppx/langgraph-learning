# 02 - State 状态与 Reducer

> 配套代码：`code/02_state_reducer.py`

State 是 LangGraph 的灵魂。理解"状态如何合并（Reducer）"是从入门到熟练的分水岭。

## 1. State 是什么

- 一个**全图共享的字典**，节点读写它；
- 框架在**每个超步结束时**按 Reducer 合并各节点的返回值；
- 结构在 `StateGraph(StateSchema)` 声明时就固定了。

三种常见写法：

```python
from typing import TypedDict, Annotated
from pydantic import BaseModel
from dataclasses import dataclass

# 写法1：TypedDict（最常用）
class State1(TypedDict):
    count: int
    items: Annotated[list, operator.add]   # 用 reducer

# 写法2：Pydantic（需要校验时）
class State2(BaseModel):
    count: int = 0

# 写法3：dataclass
@dataclass
class State3:
    count: int = 0
```

## 2. 默认合并语义：Last Value

```python
class State(TypedDict):
    text: str        # 没有 Annotated => reducer = 覆盖（last value wins）
```

两个节点都返回 `{"text": ...}`，后执行的覆盖先执行的。**看似简单，却是一切坑的源头**——
比如两个并行节点同时写 `text`，会有一个被静默丢弃（并行时谁是"last"不确定！）。

## 3. Reducer：声明"怎么合并"

用 `Annotated[类型, 函数]` 给字段指定合并函数，签名 `(旧值, 新值) -> 合并值`：

```python
import operator
from typing import Annotated
from langgraph.graph.message import add_messages

class State(TypedDict):
    # 列表拼接：适合累积"发现的问题"、"收集的文档"
    logs: Annotated[list[str], operator.add]

    # add_messages：官方提供的消息专用 reducer（见下）
    messages: Annotated[list, add_messages]

    # 字典合并
    merged: Annotated[dict, operator.or_]

    # 自定义 reducer：例如取最大
    best_score: Annotated[float, lambda old, new: max(old or 0, new)]
```

### add_messages 的特殊之处（重点）

`messages` 是 Agent 最核心的字段，它的 reducer `add_messages` 不是简单拼接：

| 新消息带 `id` 且旧列表已有同 `id` | **替换**那条旧消息（而不是追加） |
|-----------------------------------|----------------------------------|
| 新消息无 `id` | 追加 |
| 传入 `RemoveMessage(id=...)` | **删除**对应消息 |

这个"同 id 替换"机制是**工具调用修正、消息裁剪、流式去重**的基础。

官方还提供了现成的 `MessagesState`：

```python
from langgraph.graph import MessagesState

class State(MessagesState):
    extra_field: str        # messages 已内置，直接加自己的字段
# 等价于 messages: Annotated[list[AnyMessage], add_messages]
```

## 4. 自定义 Reducer 实战：投票/去重

```python
def dedup_append(old: list[str], new: list[str]) -> list[str]:
    seen = set(old)
    return old + [x for x in new if x not in seen]

class State(TypedDict):
    seen_urls: Annotated[list[str], dedup_append]
```

注意：reducer 必须是**纯函数**（同样输入永远同样输出），
因为时间旅行重放、并行合并都会反复调用它。不要在 reducer 里做 IO。

**首值不可变问题**：reducer 第一次收到 `None`（旧值不存在），
所以 reducer 内要处理 `old is None` 的分支（如上面 `max(old or 0, new)`）。

## 5. 输入/输出 Schema：隐藏中间字段

内部 State 常有脏字段（中间计数器、调试日志），不想暴露给调用方：

```python
builder = StateGraph(StateSchema, input=InputSchema, output=OutputSchema)
```

- `input`：invoke 时只接受这些字段（其余被丢弃）
- `output`：返回/输出流时只包含这些字段
- **内部仍是完整的 StateSchema 在流转**——这只是"接口裁剪"，不是隔离沙箱

这个特性在**子图状态对齐**（笔记 06）和**多Agent接口契约**中非常有用。

## 6. 动手实验清单

1. 把两个字段都改成 `Annotated[list, operator.add]`，让两个并行节点同时写，观察拼接顺序。
2. 写一个 reducer：`keep_last_n(old, new)` 只保留最近 N 条消息（消息裁剪是生产必备）。
3. 给 `add_messages` 字段发两条同 `id` 的消息，验证"替换"语义。

## 7. 常见坑

| 坑 | 现象 | 解法 |
|----|------|------|
| 并行节点写无 reducer 的字段 | **1.x 直接抛 `InvalidUpdateError`**（0.x 是静默覆盖） | 给字段加 reducer；确实想"听天由命"就串行化 |
| reducer 里原地修改 old | 时间旅行错乱 | reducer 必须返回新对象 |
| 以为返回完整 state 才有效 | 啰嗦且容易覆盖别人的更新 | 只返回增量 |
| Pydantic model 字段默认值 | 与"首值 None"混淆 | 显式处理 `old is None` |
| 在多个图之间直接共享同一个 list 对象 | 诡异串改 | State 更新永远走 reducer/返回新值 |
