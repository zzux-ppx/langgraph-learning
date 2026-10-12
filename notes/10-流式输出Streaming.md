# 10 - 流式输出 Streaming

> 配套代码：`code/10_streaming.py`

`invoke` 只给你最终结果；`stream` 给你过程。五种 stream_mode 各有用途，
生产中通常**组合使用**（`stream_mode=["updates", "messages"]`）。

## 1. 五种模式总览

| stream_mode | 每个事件是 | 典型用途 |
|-------------|-----------|----------|
| `"values"` | 每个超步后的**完整 state** | 调试、时间线回放 |
| `"updates"` | 每个节点的**增量更新**（谁写了什么） | 进度展示、审计日志 |
| `"messages"` | **LLM token 级**流 + 元信息 | 打字机效果（前端体验核心） |
| `"custom"` | 节点里 `get_stream_writer()` 发的自定义事件 | 业务进度（"正在搜索第2页…"） |
| `"debug"` | 一切内部事件（含 task/channel 级） | 深度排错、观察 Pregel 内部 |

```python
for chunk in graph.stream(inputs, config, stream_mode="updates"):
    print(chunk)
# {'node_a': {'logs': ['...']}}      <- updates：节点名 -> 它返回的更新
```

## 2. messages 模式（token 流）

```python
for msg, meta in graph.stream(inputs, config, stream_mode="messages"):
    print(msg.content, end="", flush=True)
# msg: AIMessageChunk（LangChain 标准分片）
# meta: {"langgraph_node": "agent", "langgraph_step": 3, ...}
```

- 只要有节点用 LangChain 标准的 `chat_model.stream`/`.bind_tools`，
  token 就会被自动逐个推送，**不需要你显式写流式代码**；
- `meta["langgraph_node"]` 可以区分是哪个节点在说话（多 Agent 场景分屏显示）。

## 3. custom 模式（自定义事件）

```python
from langgraph.config import get_stream_writer

def search_node(state):
    writer = get_stream_writer()
    for i, kw in enumerate(state["keywords"]):
        writer({"progress": f"搜索 {kw}", "i": i})    # 推给消费者
        ...
    return {...}

# 消费侧：
for ev in graph.stream(inputs, config, stream_mode="custom"):
    print(ev)      # {'progress': '搜索 xx', 'i': 0}
```

`get_stream_writer()` 在没有 stream 上下文时也能安全调用（no-op），
所以节点代码不需要感知调用方式。

## 4. 组合模式

```python
for chunk in graph.stream(
    inputs, config,
    stream_mode=["updates", "messages", "custom"],
    subgraphs=True,        # 透传子图内部事件（多Agent必备）
):
    mode, payload = chunk  # 组合模式下每个事件变成 (mode, payload)
```

`subgraphs=True` 后事件带命名空间前缀，能看到子图里每个节点的动作；
不加它，子图内部（除 messages 外）基本是黑盒。

## 5. 异步流式（服务端标准姿势）

```python
async for chunk in graph.astream(inputs, config, stream_mode="messages"):
    ...

# SSE/WS 后端骨架：
async def sse(request):
    async def gen():
        async for msg, meta in graph.astream(inputs, config, stream_mode="messages"):
            yield f"data: {json.dumps({'t': msg.content})}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")
```

## 6. 流式与持久化的关系

- 每个**超步结束都会写 checkpoint**，所以流到一半崩了，
  用同 thread_id 可以从最后一个完整超步恢复（input=None 继续）；
- "messages" 是节点内部的细粒度，**不落盘**——重启后不会有半个 token。

## 7. 常见坑

| 坑 | 说明 |
|----|------|
| 以为 values 模式事件少 | 每个超步都发一份完整 state，大 state 时很重 |
| updates 里看不到 messages 内容 | updates 给的是该节点返回的增量（含 AIMessage 完整对象），不是 token |
| 自定义节点手写 token 流 | 应返回流式生成器或直接用 LangChain model 的流式；框架才能透传 |
| 忘了 `subgraphs=True` 看不到子图进度 | 多 Agent 界面一片安静 |
| 在 interrupt 暂停期间 stream | 正常，会收到当前状态后停止 |
| Windows 终端中文卡顿 | 用 `flush=True` + UTF-8 终端 |

## 8. 动手实验清单

1. 分别用五种模式跑同一个图，对比事件数量与内容。
2. 给某节点加 `get_stream_writer()` 上报进度，前端模拟消费。
3. `stream_mode=["updates","messages"]` 组合消费，写一个简易"执行过程面板"。
