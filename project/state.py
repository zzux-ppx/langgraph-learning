"""
state.py — 深度研究系统的全局状态定义
=====================================
Reducer 全家桶演示：
  messages     add_messages   官方消息reducer（同id替换/删除，幂等）
  findings     operator.add   并行研究员的结论汇总（Send汇合的关键）
  review_rounds operator.add  审阅轮次计数（回炉循环）
  hops         operator.add   supervisor 跳数（防死循环）
  user_context 自定义merge     Store召回的长期记忆合并
"""

from typing import Annotated, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
import operator


def merge_dict(old: dict | None, new: dict | None) -> dict:
    """自定义 reducer：字典浅合并（新键覆盖旧键）。"""
    return {**(old or {}), **(new or {})}


class ResearchState(TypedDict, total=False):
    """主图状态（total=False: 字段随流程逐步出现）。"""
    # 对话轨迹（所有 agent 的消息都汇聚于此）
    messages: Annotated[list[AnyMessage], add_messages]

    topic: str                                    # 研究主题
    subtopics: list[str]                          # planner 拆出的子课题

    # N 个并行研究员的产出（Send 汇合靠这个 reducer）
    findings: Annotated[list[dict], operator.add]

    report: str                                   # writer 产出
    approved: str                                 # 审批结果 yes/no
    verdict_comment: str                          # 审阅意见

    review_rounds: Annotated[int, operator.add]   # 审阅轮次（由supervisor累加）
    round_hint: int                               # 给reviewer子图的只读轮次提示
                                                 # (LastValue: 子图回写同值，幂等安全)
    hops: Annotated[int, operator.add]            # supervisor 跳数

    # Store 召回的长期记忆（user偏好、历史研究）
    user_context: Annotated[dict, merge_dict]


class ResearchInput(TypedDict, total=False):
    """对外输入契约：只暴露 topic。"""
    topic: str


class ResearchOutput(TypedDict, total=False):
    """对外输出契约：只暴露最终产物。"""
    report: str
    findings: list
    approved: str
