# 深度研究多 Agent 系统（LangGraph 综合实战）

输入一个研究主题，系统自动完成：**拆解课题 → 并行研究（工具调用循环）→
汇总成稿 → AI 初审 + 人工终审（可打回重写）→ 记忆沉淀**。

默认使用内置 Mock 大脑（离线可跑、结果确定）；配置 `OPENAI_API_KEY`
后零改图切换真实 LLM。

## 架构

```
                        ┌───────────────┐
 START ─▶ load_memory ─▶│  supervisor   │◀──────────────────┐
          (Store召回)    └─┬────┬────┬──┘                   │
            Command(goto)  │    │    │ Send x N             │
                ┌──────────┘    │    └────────────┐         │
                ▼               ▼                 ▼         │
            planner        researcher          writer ─────┤
          (拆解子课题)   (ReAct工具循环子图)   (汇总成稿)    │
                │          web_search/fetch                  │
                └──────────────┬────────────────────────────┘
                               ▼ (findings 经 reducer 汇合)
                          reviewer 子图
                          ai_review → human_gate(interrupt!)
                               │ 通过
                               ▼
                          save_memory ─▶ END
                          (Store沉淀)
```

## LangGraph 能力清单（每项对应代码位置）

| 能力 | 位置 |
|------|------|
| StateGraph + 多种 Reducer（add_messages/operator.add/自定义dict合并） | `state.py` |
| Command 导航（update+goto 原子绑定） | `nodes/supervisor.py` |
| Send 动态并行（增量派发未完成子课题） | `nodes/supervisor.py` |
| 子图当节点（researcher / reviewer） | `graph.py` |
| ToolNode + 工具调用循环（ReAct） | `nodes/researcher.py`, `tools/research_tools.py` |
| 条件边循环 + 循环安全阀（MAX_TURNS） | `nodes/researcher.py` |
| Checkpointer 持久化 + 同thread续跑 | `graph.py`, `main.py` 场景2 |
| interrupt() 人工审批（埋在子图深处）+ Command(resume) | `nodes/reviewer.py`, `main.py` |
| 打回-重写-再审 回炉循环（上限2轮） | `nodes/supervisor.py` |
| Store 长期记忆（跨会话） | `nodes/memory.py`, `main.py` 场景3 |
| 流式输出 updates+custom+subgraphs | `main.py::stream_until_done` |
| input/output 契约裁剪 | `state.py` |
| recursion_limit + hops 双重防环 | `graph.py`, `nodes/supervisor.py` |
| 时间旅行（同线程回放重走另一条路） | `main.py::demo_timetravel` |

## 两个真实踩坑记录（已在代码中规避）

1. **只读子图回写翻倍**：共享 state 的子图完成时，会把它读过的所有共享字段
   经父图 reducer 再应用一次。`review_rounds` 若放进 reviewer 子图，
   `operator.add` 会让轮次漂移。→ 轮次由 supervisor 维护，经 `round_hint`
   （LastValue，幂等）传入子图。见 `nodes/reviewer.py` 注释。
2. **时间旅行续跑配置**：回放后的后续 resume 不能继续用带旧 `checkpoint_id`
   的 config（会反复回到旧快照死循环）；InMemorySaver 按 thread 寻址，
   跨线程 fork 需 `update_state` 搬运状态。见 `main.py::demo_timetravel`。

## 运行

```bash
cd langgraph-learning
python -m project.main                    # 完整演示（自动审批）
python -m project.main --interactive      # 审批处人工输入 yes/no
python -m project.main --topic "多模态RAG" --timetravel
```

## 接入真实 LLM

```bash
pip install langchain-openai
set OPENAI_API_KEY=sk-xxx
set OPENAI_BASE_URL=https://api.xxx.com/v1   # 任何 OpenAI 兼容接口
set OPENAI_MODEL=gpt-4o-mini
python -m project.main
```

图结构、节点、工具全部不变 —— 这正是"LLM 只是节点的实现细节"的体现。
