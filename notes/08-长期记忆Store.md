# 08 - 长期记忆 Store

> 配套代码：`code/08_store_memory.py`

Checkpoint（笔记 07）解决的是"**同一会话内**的状态留存"；
Store 解决的是"**跨会话**的长期记忆"——用户偏好、事实档案、经验教训。

## 1. 概念模型

```
Checkpointer：按 thread_id 存"执行快照"（短期记忆，运行时机制）
Store：       按 (namespace, key) 存"记忆条目"（长期记忆，业务数据）
```

```python
from langgraph.store.memory import InMemoryStore

store = InMemoryStore()
graph = builder.compile(checkpointer=..., store=store)

# namespace 是个 tuple，通常编码"维度"，最常见第一段是 user_id
store.put(("users", "alice", "facts"), "pref_style", {"text": "喜欢简洁中文回答"})
item = store.get(("users", "alice", "facts"), "pref_style")
items = store.search(("users", "alice", "facts"), query="回答风格")
store.delete(("users", "alice", "facts"), "pref_style")
```

- `put(namespace, key, value)`：value 是任意 JSON dict，同 key 覆盖（可含 `updatedAt` 自动维护）
- `search(namespace, query=..., filter=..., limit=...)`：**语义检索**或过滤

## 2. 在图中读写 Store

节点通过 `config` 拿到 store（注入式，便于换实现）：

```python
from langgraph.store.base import BaseStore

def remember(state: State, config, *, store: BaseStore):
    uid = config["configurable"]["user_id"]
    store.put(("users", uid, "facts"), f"note_{i}", {"text": ...})

def recall(state: State, config, *, store: BaseStore):
    uid = config["configurable"]["user_id"]
    hits = store.search(("users", uid, "facts"), query=state["question"])
    return {"context": [h.value["text"] for h in hits]}
```

要点：
- 参数名必须叫 `store`，类型注解 `BaseStore`，框架按注解注入（和 config 一样的机制）；
- 写记忆的常见时机：对话结束时由一个"记忆压缩节点"提炼；或每轮用 LLM 判断"有没有值得记的"。

## 3. 语义检索：给 Store 配 embedding

InMemoryStore 支持配置向量检索：

```python
from langgraph.store.memory import InMemoryStore
# 任选一个 embedding（真实场景用 OpenAI/本地模型）
store = InMemoryStore(index={"embed": my_embed_fn, "dims": 384})
# 之后 search 会自动：把 query 向量化 -> 余弦相似度排序
```

生产对应实现：PostgresStore（pgvector）等。语义记忆的典型玩法——
"用户上次说过他不吃辣"存成一条，几周后问"推荐晚餐"时 `search(query="饮食偏好")` 召回。

## 4. 记忆的三种形态（官方 taxonomy）

| 形态 | 存哪 | 例 |
|------|------|----|
| 短期（工作记忆） | State / messages | 本轮对话上下文 |
| 长期（跨会话） | Store | 用户偏好、事实、经验 |
| 程序性（"怎么做事"） | Store / 系统提示词模板 | 学到的 workflow 技巧 |

## 5. 设计建议

1. **namespace 分层设计**：`("users", uid, "facts")` / `("users", uid, "episodes")` /
   `("org", team_id, "playbook")`，别一锅炖。
2. **记忆要"提炼后"再存**，别把原始对话直接塞进去（检索质量差、体积大）。
3. **读记忆在图早期节点做一次**（如 load_memory → agent），
   写记忆在 END 前的节点做（agent → save_memory → END）。
4. 记忆条目带时间戳与来源，便于去重与审计。
5. 冲突处理：新记忆覆盖旧记忆时，可在 value 里合并（如"最新住址"字段）。

## 6. 常见坑

- 节点参数写 `store` 但没加类型注解 → 不会注入，拿到 None。
- 忘了 compile 时传 store → 节点里 `store` 参数为 None。
- `search` 无 query 且无 filter → 返回 namespace 下全部（limit 内）。
- InMemoryStore 同样不落盘；语义检索需要 index 配置，否则 query 只做简单匹配。
- 把敏感信息直接存明文——生产要考虑加密与权限边界。

## 7. 动手实验清单

1. 两个 thread_id 同一 user_id：A 会话存偏好，B 会话召回——验证"跨会话记忆"。
2. 给 store 配一个假 embedding（如字符串长度向量），观察 search 排序变化。
3. 实现"会话结束→提炼记忆→写入 Store"的收尾节点。
