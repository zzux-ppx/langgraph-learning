# 09 - Human-in-the-Loop（人工介入）

> 配套代码：`code/09_human_in_the_loop.py`

Agent 要进生产，必须有"刹车"：执行危险工具前确认、结果发布前审批、
低置信度时转人工。LangGraph 的方案是 `interrupt` + checkpoint 恢复，
**比 breakpoints 更主流、更可控**（1.x 官方首推）。

## 1. interrupt()：声明式暂停

```python
from langgraph.types import interrupt, Command

def human_approval(state: State) -> dict:
    plan = state["draft_plan"]
    # 打断执行，payload 是"给人看/给前端展示"的任意 JSON
    answer = interrupt({"question": "以下计划是否通过？", "plan": plan})
    # ↓ 只有 resume 之后才会执行到这里，answer 就是 Command(resume=...) 的值
    return {"approved": answer == "yes"}
```

运行方（CLI/服务端）的处理循环：

```python
graph = builder.compile(checkpointer=InMemorySaver())   # interrupt 必须有 checkpointer！
config = {"configurable": {"thread_id": "t1"}}

result = graph.invoke(inputs, config)
# 图在 interrupt 处"停住"了 —— invoke 返回的是当前状态（含 __interrupt__ 信息）
snap = graph.get_state(config)
print(snap.tasks[0].interrupts)          # 里面是上面 interrupt() 的 payload

# 拿到人工决策后恢复：
graph.invoke(Command(resume="yes"), config)   # resume 值会成为 interrupt() 的返回值
# 或拒绝： graph.invoke(Command(resume="no"), config)
```

四个关键点：

1. `interrupt()` **必须在节点内部调用**（不能在路由函数/工具函数里）；
2. **必须配 checkpointer**——暂停的原理就是把状态存盘后抛 `GraphInterrupt`；
3. `Command(resume=值)` 恢复后，`值` 就是 `interrupt()` 调用的**返回值**，代码从断点继续；
4. 同一节点可以多次 `interrupt()`，按顺序逐个恢复。

## 2. 恢复时换状态：resume + update

```python
graph.invoke(Command(resume="yes", update={"plan": revised_plan}), config)
```

审批人顺手改了方案 → 恢复时一并注入更新。

## 3. 经典模式：工具调用前审批（interrupt_before）

不想改节点代码，可以在工具节点前整图级打断：

```python
graph = builder.compile(
    checkpointer=cp,
    interrupt_before=["tools"],    # tools 节点执行前暂停
    # interrupt_after=["agent"],   # 或某节点后暂停
)
```

暂停后人工检查 `get_state(config).values`（或 `.tasks` 里的待执行写入），
- 放行：`graph.invoke(None, config)`（input=None = 继续）
- 修改后放行：`graph.update_state(config, {...}); graph.invoke(None, config)`

对比：

| | interrupt() | interrupt_before |
|---|---|---|
| 粒度 | 节点内任意代码处，可携带结构化问题 | 节点边界 |
| 恢复值 | 有（resume→返回值） | 无（只能 None 继续） |
| 灵活性/可读性 | 高，推荐 | 适合"给既有图无侵入加闸门" |

## 4. NodeInterrupt：条件性中断

```python
from langgraph.errors import NodeInterrupt

def quality_gate(state):
    if state["score"] < 0.7:
        raise NodeInterrupt(f"质量分 {state['score']} < 0.7，需人工复核")
    return {}
```

与 `interrupt()` 的区别：`NodeInterrupt` 抛出后该节点**失败并停在那**，
主要用于"异常熔断"，一般不靠 resume 续跑而是 update_state 修数据后重试。

## 5. 生产架构建议

```
后端跑图 → interrupt 暂停 → 把 payload 推给审批端（Web/IM）
   ▲                                        │
   └── Command(resume) 恢复 ←── 审批人操作 ──┘
```

- thread_id 就是"待办工单号"；暂停期间进程重启也没事（checkpointer 落盘）；
- 审批超时：由业务侧定时器决定（比如 24h 后自动 resume 拒绝值）；
- 权限：审批接口必须校验 thread 归属，别把 resume 能力裸暴露。

## 6. 常见坑

| 坑 | 说明 |
|----|------|
| 没 checkposter 就 interrupt | 报错：interrupt 需要持久化 |
| 在工具函数里 interrupt() | 无效/报错，必须在节点里（工具逻辑包一层节点） |
| resume 后变量丢失 | interrupt() 前 state 是快照，恢复后从断点继续，节点局部变量仍在 |
| 用 `input dict` 而不是 `Command(resume=)` | 会当成"新输入"，interrupt 未被应答，报 GraphInterrupt |
| 多个 interrupt 顺序搞混 | `snap.tasks[*].interrupts` 按序号对应 |
| subgraph 里的 interrupt | 支持！恢复粒度直达子图内部（笔记 06） |

## 7. 动手实验清单

1. 跑配套代码：观察 `interrupt()` → 打印问题 → `resume("yes")` 全流程。
2. resume 一个字典 `{"ok": True, "comment": "..."}`，在节点里解包。
3. 同一节点放两个 interrupt，体会"逐个应答"。
4. 用 `interrupt_before=["tools"]` 给 ReAct 图加闸门，人工改掉危险 tool_call 再放行。
