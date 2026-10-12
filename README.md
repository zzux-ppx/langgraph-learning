# LangGraph 学习仓库

> 基于 **langgraph 1.2.14**（2026-10）整理。官方文档已迁移至
> [docs.langchain.com/oss/python/langgraph/overview](https://docs.langchain.com/oss/python/langgraph/overview)

这是一个"笔记 + 配套代码 + 底层原理 + 综合项目"四位一体的 LangGraph 学习仓库。
所有示例代码都**不依赖真实 LLM API Key** 即可运行（纯图机制演示），
接入真实模型的方法在各文件中用注释标出。

## 目录结构

```
langgraph-learning/
├── README.md                 # 本文件：学习路线总览
├── requirements.txt          # 依赖（仅 langgraph，可选装 langchain-openai）
│
├── notes/                    # 学习笔记（按序阅读）
│   ├── 00-学习指南.md
│   ├── 01-核心概念与快速上手.md
│   ├── 02-State状态与Reducer.md
│   ├── 03-节点、边与循环.md
│   ├── 04-Send并行与MapReduce.md
│   ├── 05-Command导航.md
│   ├── 06-子图与多图组合.md
│   ├── 07-持久化Checkpoint与时间旅行.md
│   ├── 08-长期记忆Store.md
│   ├── 09-Human-in-the-Loop.md
│   ├── 10-流式输出Streaming.md
│   ├── 11-多Agent架构.md
│   ├── 12-底层原理一-Pregel与图的编译执行.md
│   └── 13-底层原理二-源码走读.md
│
├── code/                     # 与笔记一一对应的可运行代码
│   ├── 01_basic_graph.py             # 笔记01：第一个图
│   ├── 02_state_reducer.py           # 笔记02：状态/reducer/私有State
│   ├── 03_conditional_edges_loop.py  # 笔记03：条件边/循环/递归限制
│   ├── 04_send_map_reduce.py         # 笔记04：Send 动态并行
│   ├── 05_command_navigation.py      # 笔记05：Command 导航与恢复
│   ├── 06_subgraph.py               # 笔记06：子图
│   ├── 07_checkpoint_timetravel.py   # 笔记07：Checkpoint与时间旅行
│   ├── 08_store_memory.py           # 笔记08：Store 长期记忆
│   ├── 09_human_in_the_loop.py      # 笔记09：interrupt 人工审批
│   ├── 10_streaming.py             # 笔记10：五种流式模式
│   ├── 11_multi_agent_supervisor.py # 笔记11：Supervisor 多Agent
│   └── deep_dive/
│       ├── mini_langgraph.py        # 笔记12：手写200行迷你LangGraph
│       └── pregel_superstep_trace.py# 笔记12：超步执行轨迹观察
│
└── project/                  # 综合项目：深度研究多Agent系统
    ├── README.md             # 项目架构图与能力清单
    ├── state.py              # 全局状态定义（多种Reducer）
    ├── llm.py                # LLM大脑（Mock/OpenAI 双实现，环境变量切换）
    ├── graph.py              # 主图组装（Supervisor+子图+Send+interrupt）
    ├── main.py               # CLI 入口（流式/审批/断点续跑/记忆演示）
    ├── nodes/
    │   ├── planner.py        # 规划节点（结构化输出拆子课题）
    │   ├── supervisor.py     # 监督者路由（Command+Send增量派发）
    │   ├── researcher.py     # 研究Agent子图（工具调用循环+ToolNode）
    │   ├── writer.py         # 写作节点
    │   ├── reviewer.py       # 审阅子图（interrupt人工审批）
    │   └── memory.py         # Store长期记忆读写节点
    └── tools/
        └── research_tools.py # @tool 工具集（离线Mock搜索）
```

## 推荐学习路线

| 阶段 | 内容 | 产出 |
|------|------|------|
| 第 1 阶段：入门 | 笔记 00-03 + 对应代码 | 理解图/状态/边，能搭带循环的工作流 |
| 第 2 阶段：进阶 | 笔记 04-06 + 对应代码 | 掌握并行(Send)、Command导航、子图 |
| 第 3 阶段：生产能力 | 笔记 07-10 + 对应代码 | 持久化、记忆、人机协同、流式输出 |
| 第 4 阶段：架构 | 笔记 11 + 对应代码 | 多Agent模式（Supervisor/Swarm） |
| 第 5 阶段：原理 | 笔记 12-13 + deep_dive 代码 | 手写mini版，吃透Pregel执行模型 |
| 第 6 阶段：实战 | project/ | 综合运用全部能力的深度研究Agent |

## 环境准备

```bash
cd langgraph-learning
pip install -r requirements.txt

# 验证安装
python -c "from langgraph.graph import StateGraph; print('OK')"

# 运行第一个示例
python code/01_basic_graph.py
```

接入真实 LLM（可选，综合项目用）：

```bash
pip install langchain-openai
export OPENAI_API_KEY=sk-xxx        # Windows: set OPENAI_API_KEY=sk-xxx
export OPENAI_BASE_URL=https://...  # 任何 OpenAI 兼容接口
```

不设置环境变量时，综合项目自动使用内置 Mock 大脑，离线可完整跑通。

## 核心心智模型（一页速览）

```
        ┌─────────────────────────────────────────────┐
        │              StateGraph(State)               │
        │                                             │
START ─▶│  node_a ──edge──▶ node_b ──cond_edge──┬──▶ END
        │    ▲                 ▲                │
        │    └──── loop ◀──────┴──── (可循环)   │
        │                                             │
        │  · State: 各节点共享的"数据库"，Reducer 决定合并策略 │
        │  · Node:   普通函数 state -> state更新          │
        │  · Edge:   静态边 / 条件边 / Send动态边          │
        │  · Command: 节点内部"既改状态又指路"             │
        └─────────────────────────────────────────────┘
   编译 compile() ──▶ Pregel 运行时（超步循环执行）
   + checkpointer ──▶ 断点续跑/时间旅行
   + store      ──▶ 跨线程长期记忆
   + interrupt()──▶ 人工审批
   + stream()   ──▶ 流式输出
```
