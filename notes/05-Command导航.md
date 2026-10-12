# 05 - Command 导航

> 配套代码：`code/05_command_navigation.py`

条件边把"路由"放在节点**外面**。但有时节点需要在**产生状态更新的同时**决定下一跳
（典型：多 Agent 里 Supervisor 节点）。
`Command` 让节点一次性完成"改状态 + 指路"，甚至跨子图跳转、恢复中断。

```python
from langgraph.types import Command

def my_node(state: State) -> Command:
    return Command(
        update={"foo": "bar"},   # 状态更新（等价于普通节点返回 dict）
        goto="other_node",       # 下一跳：str / list[str] / Send / GraphCommand
    )
```

## 1. Command vs 条件边

| | 条件边 | Command |
|---|---|---|
| 路由逻辑位置 | 独立路由函数（节点外） | 节点内部 |
| 能否带状态更新 | 否（路由函数返回值只用于选路） | 是（update + goto 原子绑定） |
| 跨子图跳转 | 否 | 是（`goto` 支持父图节点名） |
| 代码内聚性 | 路由可复用、可单测 | 逻辑集中在节点，适合"决策即节点" |

**选择建议**：路由规则简单、纯看 state → 条件边；
路由决策依赖节点内部计算结果（如 LLM 输出）→ Command 更顺手。

## 2. 有效用法（官方列举）

```python
Command(goto="node")                          # 1. 去某节点
Command(goto=["a", "b"])                      # 2. 并行去多节点
Command(goto="node", update={"k": "v"})       # 3. 更新+导航
Command(update={"k": "v"})                    # 4. 只更新，等价返回 dict
def parent_node(state) -> Command:
    return Command(goto="child_node")         # 5. 子图内跳父图节点（多Agent核心技巧）
    # Command(goto=Send("node", input))       # 6. 动态并行（结合 Send）
```

还有第 7 种：`Command(resume=...)`——人工审批后恢复中断（笔记 09 详解）。

## 3. Supervisor 模式里的典型用法

```python
def supervisor(state: State) -> Command:
    decision = call_llm(state)             # LLM 决定下一步交给谁
    return Command(
        goto=decision.next_agent,          # "researcher" / "writer" / FINISH
        update={"route_reason": decision.reason},
    )
```

与条件边版的区别：不需要为 supervisor 单写一个 route 函数，
决策逻辑与决策依据（写入 state 的 reason）在同一步内完成。

## 4. 在 Command 里使用 Send（动态并行 + 更新）

```python
def planner(state) -> Command:
    tasks = split_tasks(state)
    return Command(
        update={"phase": "research"},
        goto=[Send("worker", {"task": t}) for t in tasks],
    )
```

## 5. goto 的 END

```python
Command(goto=END)          # 结束图
Command(goto="__end__")    # 等价字符串
```

## 6. 注意事项

1. **用了 Command 返回就不能再返回 dict**（二选一）。
2. Command 与该节点的静态出边**会同时生效**：`goto` 是"追加"而不是"覆盖"静态边，
   为避免意外双跳，用 Command 的节点不要再挂静态出边。
3. 条件边与 Command 也能共存（都指向的节点会执行），一般别这么写，可读性差。
4. `Command` 也可作为 `update_state` 的参数手动注入导航（笔记 07 时间旅行会用到）。
5. 子图节点 `goto` 父图节点名时，要求子图是以**共享状态**方式嵌入（笔记 06）。

## 7. 动手实验清单

1. 把笔记 03 的 ReAct 循环改写成 `should_continue` 逻辑内联进 llm 节点，返回 `Command(goto=...)`。
2. 实现一个三路由 Supervisor（用 `Command(goto=[...])` 同时派两个节点再汇合）。
3. 观察"Command 节点 + 静态出边"导致的双跳现象。
