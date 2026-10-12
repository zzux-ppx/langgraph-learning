# 11 - 多 Agent 架构

> 配套代码：`code/11_multi_agent_supervisor.py`

单个 Agent 上下文塞不下所有工具和职责时，就拆成多 Agent。
LangGraph 本身不强制任何多 Agent 范式——**每种范式都只是图的一种拓扑**。
这也是它比"多Agent框架"更强的原因：范式可以混合、嵌套、定制。

## 1. 三大基础范式

### 1.1 Supervisor（监督者/中心化路由）

```
                 ┌──▶ researcher ──┐
supervisor ──路由─┼──▶ coder ───────┼──▶ supervisor（循环决策）
                 └──▶ writer ──────┘
supervisor 决定 FINISH 时 → END
```

- supervisor 是一个普通节点，返回 `Command(goto=..., update=...)`（笔记 05）；
- 每个 agent 是子图（笔记 06）；
- 全局状态里放 `messages`（add_messages reducer）+ 共享笔记 `notes`。
- **优点**：控制集中、易于加审批/日志；**缺点**：supervisor 是瓶颈与单点。

### 1.2 Swarm / Handoff（去中心化交接棒）

```
researcher ──handoff──▶ writer ──handoff──▶ reviewer ──▶ END
```

- 没有 supervisor，每个 agent 自己决定"接下来交给谁"；
- 实现：子图节点里 `Command(goto="别的agent", update={...})` 直接跳父图兄弟节点；
- handoff 常做成一个**工具**（`transfer_to_writer`），LLM 调用即交接；
- **优点**：少一跳、延迟低；**缺点**：容易绕圈，需在 state 里计数防环。

### 1.3 Hierarchical（层级，supervisor 套 supervisor）

大团队拆小组：顶层 supervisor 路由到"小组 supervisor"（本身是个含子 agent 的图）。
就是 1.1 的递归形式，子图复用同一套机制。

## 2. Supervisor 最小实现骨架

```python
from langgraph.types import Command
from typing import Literal

class State(MessagesState):
    next: str

def supervisor(state) -> Command[Literal["researcher", "writer", "__end__"]]:
    # 真实场景：LLM 结构化输出决定 next
    decision = route_by_llm(state)
    return Command(goto=decision, update={"next": decision})

builder = StateGraph(State)
builder.add_node("supervisor", supervisor)
builder.add_node("researcher", researcher_subgraph)
builder.add_node("writer", writer_subgraph)
builder.add_edge(START, "supervisor")
# 每个 agent 干完活都回 supervisor
for ag in ("researcher", "writer"):
    builder.add_edge(ag, "supervisor")
graph = builder.compile(checkpointer=cp)
```

`Command[Literal[...]]` 返回类型注解会让静态检查器帮你看住 goto 的目标集合。

## 3. 关键工程问题

### 3.1 Agent 之间怎么通信
- **共享 state**（最常用）：messages 共享对话史，`notes`/`artifacts` 放中间产物；
- 显式输入输出：每个子图定义自己的 input/output schema（笔记 02 §5），契约清晰。

### 3.2 怎么防死循环
- state 加 `Annotated[int, operator.add]` 的 hop 计数，supervisor 里超限即 END；
- 或依赖 `recursion_limit` 兜底（粗暴但有效）。

### 3.3 并行 fan-out（组合笔记 04）
supervisor 用 `Command(goto=[Send(...), Send(...)])` 一次派多个 agent 并行研究，
reducer 汇总。**生产深度研究系统的标准内构**。

### 3.4 人在环上（组合笔记 09）
supervisor 路由到 `human_approval` 节点，内部 `interrupt()`，审批后继续。

### 3.5 流式（组合笔记 10）
`subgraphs=True` 才能看到各子 agent 的 token 流与进度。

## 4. 高层封装 vs 手搭

```python
# 新版高层 API（langchain 1.x）：
from langchain.agents import create_agent
agent = create_agent(model, tools, checkpointer=..., store=...)
```

高层 API（`create_agent` / LangGraph Platform 的 agent 抽象）适合标准 ReAct；
需要**自定义控制流**（评审循环、并行、审批、多角色协作）时回到手搭 StateGraph——
本仓库综合项目就是手搭示范。手搭吃透后，高层封装只是"别人替你搭的图"。

## 5. 选型速查

| 需求 | 推荐 |
|------|------|
| 任务能清晰分工、需要统一调度 | Supervisor |
| 对话式流转、agent 自主性高 | Swarm/handoff |
| 大规模团队（>5 agent） | 层级 |
| 独立子任务批量处理 | Send 并行 fan-out（不需要"多agent"） |
| 标准 ReAct 单 agent | `create_agent` / `create_react_agent` 预置 |

## 6. 动手实验清单

1. 跑配套 supervisor 示例，改路由规则让顺序变化。
2. 给 researcher 加 hop 计数，超 3 跳强制 FINISH。
3. 把其中一个 agent 改成 handoff 风格（直接 goto 兄弟节点），做成混合架构。
