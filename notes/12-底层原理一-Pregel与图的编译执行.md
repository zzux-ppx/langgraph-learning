# 12 - 底层原理（一）：Pregel 与图的编译执行

> 配套代码：`code/deep_dive/mini_langgraph.py`（手写迷你版）、
> `code/deep_dive/pregel_superstep_trace.py`（超步观察）
>
> 本文基于 **langgraph 1.2.14 源码**（`langgraph/pregel/`、`langgraph/graph/state.py`）。

## 1. LangGraph 的理论出身：Pregel

Google 2010 年论文《Pregel: A System for Large-Scale Graph Processing》提出了
**BSP（Bulk Synchronous Parallel，整体同步并行）** 模型。Apache Giraffe/Spark 的
Pregel API、Flink 的迭代算子、乃至 LangGraph，都是这一模型的后代：

```
repeat:
    1. 激活：决定本超步哪些"顶点"活跃（收到新消息/新输入）
    2. 计算：所有活跃顶点并发执行用户函数
    3. 通信：顶点的输出发给 channel/边
    4. 同步：barrier——等本超步全部完成，合并结果，进入下一超步
until 没有活跃顶点（或超过步数上限）
```

对应到 LangGraph：

| Pregel 概念 | LangGraph 中的实体 |
|-------------|--------------------|
| 顶点 (vertex) | Node |
| 边/消息 | Channel 写入（含 branch 控制通道） |
| 顶点状态 | 全局 State（由 channels 聚合而成） |
| 超步 (superstep) | recursion_limit 计的"一步" |
| 顶点投票 halt | 没有待执行任务 → 图结束 |
| 图状态快照 | Checkpoint |

**为什么选 BSP？** 天然确定性的批量执行 + 每步后全局一致的状态视图 →
才能做 checkpoint、时间旅行、中断恢复——这些"生产特性"是模型的自然推论，
不是打补丁。

## 2. Channel：State 的真身

你以为 State 是个 dict，**实际上 State 是一组 channel 的视图**。
编译时，`StateGraph` 对 schema 的每个字段创建一个 channel：

| Schema 写法 | Channel 类型 | 行为 |
|-------------|--------------|------|
| `x: str`（无 Annotated） | `LastValue` | 保留最后一次写入（覆盖） |
| `x: Annotated[list, operator.add]` | `BinaryOperatorAggregate` | 逐个应用 reducer 聚合 |
| `messages: add_messages` | `BinaryOperatorAggregate`（带消息 id 替换/删除语义） | 见笔记 02 |
| 边的控制信号 | `EphemeralValue` / `LastValueAfterFinish` | 只用于触发，不进 State |
| 多入边 join | `NamedBarrierValue` | 所有来源都写过了才放行 |

每个 channel 维护一个**单调递增的版本号**；每次成功写入版本 +1。
这就是 `get_state()` 里 `channel_versions` 的来源。

```python
# channels/binop.py（节选，1.2.14）
class BinaryOperatorAggregate(BaseChannel):
    def update(self, values: Sequence[Value]) -> bool:
        if not values:
            return False
        # 依次应用你声明的 reducer: acc = op(acc, v)
        ...
```

## 3. 边的真相：不是"指针"，是"写入"

看源码 `graph/state.py :: attach_edge`（1.2.14，节选）：

```python
def attach_edge(self, starts, end):
    if isinstance(starts, str):
        # add_edge("a","b") 等价于：给节点 a 增加一个 writer，
        # a 执行完就往 b 的私有控制通道写一个信号
        self.nodes[starts].writers.append(
            ChannelWrite((ChannelWriteEntry(_CHANNEL_BRANCH_TO.format(end), None),))
        )
    else:  # add_edge(["a","b"], "c") 多入边
        channel_name = f"join:{'+'.join(starts)}:{end}"
        self.channels[channel_name] = NamedBarrierValue(str, set(starts))
        self.nodes[end].triggers.append(channel_name)      # c 由 join 通道触发
        for s in starts:
            self.nodes[s].writers.append(...)              # 各自往 join 通道写
```

结论（这是理解 LangGraph 的钥匙）：

- **每个节点有一个私有触发通道 `branch:to:<node>`**；
- 静态边 `a→b` = "a 的 writer 往 b 的触发通道写信号"；
- **条件边** = a 执行完后由 `_control_branch` **在写入阶段调用你的路由函数**，
  把信号写到被选中节点的触发通道（或产出 `Send` PUSH 任务）；
- `START` 本身也是个节点（hidden），它的 writer 负责把你 invoke 的输入写进 state channels。

**推论 1**：路由函数是在"a 的写入阶段"执行的，所以它读到的是**已合并 a 更新后**的 state。
**推论 2**：条件边路由到的目标 = 给目标节点的触发通道写信号——
所以 1.x 中即使节点返回空 dict，静态出边照样触发（实验验证过）：

```python
# langgraph 1.2.14 实测：
def a(state): return {}        # 空更新
# a --静态边--> b ：b 依然执行！
# （0.x 时代"空更新会停图"的坑在 1.x 已不存在，因为触发已与数据通道解耦）
```

## 4. 调度核心：版本比较（源码级）

每个超步开始时，`pregel/_algo.py :: prepare_next_tasks` 决定谁跑。
节点（PULL 任务）的激活条件（`_triggers` 函数，源码节选）：

```python
def _triggers(channels, versions, seen, null_version, proc) -> bool:
    for chan in proc.triggers:                 # 该节点的每个触发通道
        if channels[chan].is_available() and \
           versions.get(chan, null_version) > seen.get(chan, null_version):
            return True                        # 有"我没看过的新版本" → 激活
    return False
```

即：**"节点上次看到的版本（versions_seen）落后于通道当前版本（channel_versions）"才会跑**。
checkpoint 里存 `versions_seen` 就是为了恢复后能精确复算"谁该跑"。

两种任务类型：

- **PULL 任务**：普通节点，由触发通道版本差驱动（被动拉）；
- **PUSH 任务**：`Send` 产生的显式任务（写入 `TASKS` Topic channel），自带完整入参。

## 5. 一个超步的完整生命周期

```
超步 N:
  ① prepare_next_tasks:
       - 消费 TASKS 通道里的 Send → PUSH 任务
       - 对每个节点做版本比较 → PULL 任务
  ② 并发执行所有任务的 proc(input)：
       input = mapper(channels 当前视图)   # 即你节点函数拿到的 state
  ③ 写入阶段（每个任务独立，先落 pending_writes）：
       - _get_updates: 节点返回的 dict 逐 key 写 state channels（走 reducer）
       - _control_branch: 若有条件边/Command，此刻评估路由，写目标触发通道/Send
  ④ 提交：pending_writes 应用到 checkpoint，版本号 bump，versions_seen 更新
  ⑤ barrier 同步 → 若还有任务回到 ①，否则 END
```

**同一超步内多个节点并发**：它们读到的是同一个（上一超步末的）状态视图；
它们的写入在 ③ 之后统一合并——这就是为什么并行写同名字段必须靠 reducer（笔记 02）。

## 6. 用官方 debug 流观察超步

```python
for ev in graph.stream(inputs, stream_mode="debug"):
    if ev["type"] == "task":
        print(ev["payload"]["metadata"])   # langgraph_step / langgraph_triggers
# langgraph_triggers 会告诉你这个任务是被哪些通道触发的！
```

配套 `pregel_superstep_trace.py` 会打出每个任务的超步号与触发通道，
把本文 §3-§5 的机制"眼见为实"。

## 7. 为什么理解 Pregel 有用（实战收益）

1. **递归限制**的本质 = 超步数；性能调优（减少超步=减少同步开销）有了方向：
   能并行的用并行、`defer` 节点控制同超步内时序；
2. **时间旅行**为什么可靠：重放 = 从旧 checkpoint 恢复 channels + versions_seen，
   调度是版本比较的纯函数，结果确定；
3. **interrupt** 的实现 = 节点抛 `GraphInterrupt` → 该任务的写入不提交，
   状态停在上一完整超步；resume 时重放该任务并注入返回值；
4. 排错时看 `langgraph_triggers` 立刻知道"它为什么跑了/为什么没跑"。
