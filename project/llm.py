"""
llm.py — LLM「大脑」抽象：Mock / OpenAI 双实现
================================================
设计要点：图结构（graph.py/nodes/*）完全不感知 LLM 的存在，
所有"智能决策"都通过 Brain 接口注入 —— 换模型零改图。

默认使用 MockBrain（离线可跑、结果确定，适合学习图机制）；
设置 OPENAI_API_KEY（可选 OPENAI_BASE_URL / OPENAI_MODEL）且安装
langchain-openai 后自动切换 OpenAIBrain。
"""

from __future__ import annotations

import os
from typing import Any, Protocol

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, SystemMessage


# ---------------------------------------------------------------------------
# 接口定义
# ---------------------------------------------------------------------------

class Brain(Protocol):
    """所有智能决策的抽象接口。"""

    def plan(self, topic: str, memory: dict) -> list[str]:
        """把主题拆成 2~4 个子课题。"""

    def research_step(self, subtopic: str, messages: list[AnyMessage],
                      turn: int) -> dict:
        """研究员的一步。返回:
        {"message": AIMessage(带tool_calls)}  -> 继续调工具
        {"message": AIMessage(纯文本), "finding": {...}} -> 结束并交结论
        """

    def write(self, topic: str, findings: list[dict], memory: dict) -> str:
        """汇总所有 findings 写成报告。"""

    def review(self, report: str, rounds: int) -> dict:
        """审阅报告。返回 {"ai_verdict": "pass|fail", "comment": str}"""

    def supervisor_route(self, state: dict) -> str:
        """supervisor 的阶段决策: plan/research/write/review/save/end"""


# ---------------------------------------------------------------------------
# MockBrain：规则驱动，离线可跑
# ---------------------------------------------------------------------------

class MockBrain:
    """确定性大脑：行为随输入可预测，便于验证图机制。"""

    def __init__(self) -> None:
        self._call_seq = 0

    def plan(self, topic: str, memory: dict) -> list[str]:
        style = memory.get("style_hint")
        if style:
            print(f"    [planner] (应用用户偏好: {style})")
        return [
            f"{topic}的核心概念",
            f"{topic}的优势与局限",
            f"{topic}的典型应用场景",
        ]

    def research_step(self, subtopic: str, messages: list[AnyMessage],
                      turn: int) -> dict:
        if turn == 0:                      # 第1步：先搜索
            self._call_seq += 1
            return {"message": AIMessage(
                content=f"我需要检索「{subtopic}」的资料",
                tool_calls=[{"name": "web_search",
                             "args": {"query": subtopic},
                             "id": f"call_{self._call_seq}",
                             "type": "tool_call"}],
            )}
        if turn == 1:                      # 第2步：精读最佳结果
            self._call_seq += 1
            return {"message": AIMessage(
                content="搜索结果不错，我深入读一篇",
                tool_calls=[{"name": "fetch_page",
                             "args": {"url": f"https://mock.dev/{turn}"},
                             "id": f"call_{self._call_seq}",
                             "type": "tool_call"}],
            )}
        # 第3步：总结成 finding
        tool_evidence = "\n".join(
            m.content[:120] for m in messages if m.type == "tool")
        summary = (f"关于「{subtopic}」的调研结论：综合检索与精读，"
                   f"要点来自 {len([m for m in messages if m.type == 'tool'])} 次工具调用。"
                   f"证据摘录：{tool_evidence[:150]}")
        return {"message": AIMessage(content=summary),
                "finding": {"subtopic": subtopic, "summary": summary,
                            "sources": ["https://mock.dev/1", "https://mock.dev/2"]}}

    def write(self, topic: str, findings: list[dict], memory: dict) -> str:
        lines = [f"# {topic} 研究报告", ""]
        if memory.get("style_hint"):
            lines.append(f"> 按用户偏好（{memory['style_hint']}）成稿")
            lines.append("")
        for f in sorted(findings, key=lambda x: x["subtopic"]):
            lines.append(f"## {f['subtopic']}")
            lines.append(f["summary"])
            src = "; ".join(f.get("sources", []))
            lines.append(f"参考: {src}")
            lines.append("")
        lines.append(f"（共 {len(findings)} 份并行研究结论汇总）")
        return "\n".join(lines)

    def review(self, report: str, rounds: int) -> dict:
        if rounds == 0 and "参考:" not in report:
            return {"ai_verdict": "fail", "comment": "初稿缺少参考来源，请补充"}
        if rounds == 0:
            return {"ai_verdict": "fail", "comment": "第一轮例行挑刺：建议补充局限性小节"}
        return {"ai_verdict": "pass", "comment": f"第{rounds + 1}稿达标，建议通过"}

    def supervisor_route(self, state: dict) -> str:
        subtopics = state.get("subtopics") or []
        findings = state.get("findings") or []
        done = {f.get("subtopic") for f in findings}
        remaining = [s for s in subtopics if s not in done]
        if not subtopics:
            return "plan"
        if remaining:
            return "research"
        if not state.get("report"):
            return "write"
        if state.get("approved") != "yes":
            return "review"
        return "save"


# ---------------------------------------------------------------------------
# OpenAIBrain：接入任意 OpenAI 兼容接口（需 pip install langchain-openai）
# ---------------------------------------------------------------------------

class OpenAIBrain:
    """与 MockBrain 同构的真实实现。"""

    def __init__(self) -> None:
        from langchain_openai import ChatOpenAI     # 延迟导入
        self.llm = ChatOpenAI(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            base_url=os.getenv("OPENAI_BASE_URL") or None,
            temperature=0.3,
        )

    def plan(self, topic: str, memory: dict) -> list[str]:
        sys = SystemMessage("把研究主题拆成2~4个互补的子课题，每行一个，不要编号。"
                            + (f"用户偏好: {memory}" if memory else ""))
        resp = self.llm.invoke([sys, HumanMessage(topic)])
        items = [ln.strip("-* ") for ln in str(resp.content).splitlines() if ln.strip()]
        return items[:4]

    def research_step(self, subtopic: str, messages: list[AnyMessage],
                      turn: int, tools: list | None = None) -> dict:
        from project.tools.research_tools import TOOLS
        ai = self.llm.bind_tools(TOOLS).invoke(messages)
        if getattr(ai, "tool_calls", None):
            return {"message": ai}
        return {"message": ai,
                "finding": {"subtopic": subtopic, "summary": ai.content,
                            "sources": []}}

    def write(self, topic: str, findings: list[dict], memory: dict) -> str:
        material = "\n\n".join(f"[{f['subtopic']}]\n{f['summary']}" for f in findings)
        sys = SystemMessage("你是研究撰稿人。基于材料写结构化中文报告(Markdown)。"
                            + (f"遵循偏好: {memory}" if memory else ""))
        resp = self.llm.invoke([sys, HumanMessage(f"主题:{topic}\n材料:\n{material}")])
        return str(resp.content)

    def review(self, report: str, rounds: int) -> dict:
        sys = SystemMessage('你是严格审阅人。输出JSON: {"ai_verdict":"pass|fail","comment":"..."}')
        resp = self.llm.invoke([sys, HumanMessage(report[-3000:])])
        import json
        try:
            return json.loads(str(resp.content).strip("`json \n"))
        except Exception:
            return {"ai_verdict": "pass", "comment": str(resp.content)[:100]}

    def supervisor_route(self, state: dict) -> str:
        # 生产实现：把进度摘要交给 LLM 结构化输出；这里复用 Mock 的规则
        return MockBrain().supervisor_route(state)


# ---------------------------------------------------------------------------
# 工厂
# ---------------------------------------------------------------------------

def get_brain() -> Brain:
    if os.getenv("OPENAI_API_KEY"):
        try:
            return OpenAIBrain()
        except ImportError:
            print("[llm] 检测到 OPENAI_API_KEY 但缺少 langchain-openai，回退 Mock")
    return MockBrain()
