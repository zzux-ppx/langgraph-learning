# 07 - 持久化 Checkpoint 与时间旅行

> 配套代码：`code/07_checkpoint_timetravel.py`

没有持久化的图是"一次性程序"；有了它，才有：
多轮对话记忆、断点续跑、人工审批（HITL）、时间旅行调试、故障恢复。

## 1. 基本用法：checkpointer + thread_id

```python
from langgraph.checkpoint.memory import InMemorySaver   # 1.x 也叫 MemorySaver

checkpointer = InMemorySaver()
graph = builder.compile(checkpointer=checkpointer)

config = {"configurable": {"thread_id": "user-42-chat-1"}}
graph.invoke({"messages": [HumanMessage("我叫小明")]}, config)
graph.invoke({"messages": [HumanMessage("我叫什么?")]}, config)  # 记得"小明"！
```

三个核心概念：

| 概念 | 含义 |
|------|------|
| **checkpointer** | 存储器（内存/SQLite/Postgres），挂在 compile 上 |
| **thread**（线程） | 一次"会话/任务"的完整历史，靠 `thread_id` 区分 |
| **checkpoint** | 某个超步后的完整状态快照（含下一步该去哪） |

同一 `thread_id` 再次 invoke：**输入会与该线程最后的 checkpoint 合并后继续**，
这就是"记忆"的全部原理——没有魔法，就是把 state 存下来了。

### 生产级 Checkpointer

```python
# pip install langgraph-checkpoint-sqlite
from langgraph.checkpoint.sqlite import SqliteSaver
with SqliteSaver.from_conn_string("checkpoints.db") as cp:
    graph = builder.compile(checkpointer=cp)

# pip install langgraph-checkpoint-postgres
from langgraph.checkpoint.postgres import PostgresSaver
```

InMemorySaver 仅用于开发/测试。

## 2. 查看/修改状态：get_state / update_state

```python
snap = graph.get_state(config)          # 当前快照
print(snap.values)                      # 完整 state dict
print(snap.next)                        # 下一步待执行的节点（元组，空=已结束）
print(snap.config)                      # 该快照的 config（含 checkpoint id）

for snap in graph.get_state_history(config):   # 全部历史快照（新→旧）
    print(snap.config["configurable"]["checkpoint_id"], snap.next)

graph.update_state(config, {"messages": [...]})  # 手动改状态（追加一个"更新"）
graph.update_state(config, values, as_node="a")  # 假装是节点 a 写的（影响 next 推导）
```

`update_state` 之后 `next` 会按该节点的出边重新计算——
这是**手动重定向执行**和**修正 LLM 输出后重跑**的基础。

## 3. 时间旅行：从历史快照恢复

```python
# 1. 找到要回到的快照
hist = list(graph.get_state_history(config))
target = hist[3]                                   # 某个历史快照

# 2. 用它的 config 作为新线程起点（fork 出新分支）
for event in graph.stream(None, target.config, stream_mode="values"):
    ...
# 或固定 thread 继续：
graph.invoke(None, {**config, "configurable": {
    "thread_id": "new-branch", "checkpoint_id": target.config["configurable"]["checkpoint_id"],
}})
```

要点：
- `input=None` 表示"不注入新输入，从当前状态继续跑"（等价 `Command(goto=...)` 语义）；
- 复用原 `thread_id` + 历史 `checkpoint_id` = **同线程回放**（官方最可靠的模式，
  快照之后的"未来"会被新执行覆盖）；
- 换新 `thread_id` + 历史 `checkpoint_id` = **fork 新时间线**——注意这依赖
  checkpointer 能按 checkpoint_id 跨线程寻址：`InMemorySaver` 按
  `(thread_id, checkpoint_id)` 隔离存储，跨线程取不到（会当成全新运行报
  `EmptyInputError`）；生产想真 fork，要么选支持全局寻址的存储，要么用
  `get_state` + `update_state` 把状态显式搬运到新线程。

## 4. checkpoint 里存了什么

每个 checkpoint 包含：
- `channel_values`：所有 channel（state 字段）的最新值
- `channel_versions`：每个 channel 的版本号（增量更新依据）
- `versions_seen`：每个节点"上次看到"的各 channel 版本（**决定该节点本超步要不要重跑**，见笔记 12）
- `pending_sends`：Send 分支的待处理任务
- 中断信息（`interrupt` 触发时的 payload，笔记 09）

## 5. 多轮对话的"记忆管理"

长对话会把整个 messages 都塞进每次调用的上下文。生产必做裁剪：

```python
from langchain_core.messages import RemoveMessage

def trim(state):
    keep = state["messages"][-20:]        # 只留最近20条
    ids = {m.id for m in keep}
    return {"messages": [RemoveMessage(id=m.id) for m in state["messages"] if m.id not in ids]}
```

（新版 `langchain_core.messages.trim_messages` 是更高层的工具。）

## 6. 常见坑

| 坑 | 说明 |
|----|------|
| 忘了 thread_id | 报错 `checkpointer` 需要线程标识；每次随机 thread_id = 每次失忆 |
| InMemorySaver 当生产存储 | 进程重启全丢 |
| 以为换 checkpointer 能迁移数据 | 不行，各存各的 |
| update_state 后直接 invoke 新输入 | 新输入会先合并再执行，注意别双重写入 |
| 时间旅行后 reducer 结果与历史不同 | reducer 必须纯函数，否则重放不一致 |
| 大对象放 State（如 base64 图片） | checkpoint 膨胀，放 Store/外部存储，State 只放引用 |

## 7. 动手实验清单

1. 同一 thread 连续 invoke 三次，观察"记忆"效果；换 thread_id 验证隔离。
2. `get_state_history` 数一数快照数与超步数的对应关系。
3. 回滚到第一个超步后 fork 新线程，对比两条时间线的 `values`。
4. `update_state(..., as_node="某节点")` 后看 `snap.next` 变化。
