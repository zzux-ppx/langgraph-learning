"""
research_tools.py — 研究员工具集（离线 Mock 数据源）
====================================================
用 LangChain 标准 @tool 装饰 => 可直接喂给 ToolNode，
真实场景把函数体换成搜索引擎/HTTP 抓取即可，图代码零改动。
"""

from langchain_core.tools import tool


# Mock 知识库：按关键词命中，查不到就用 query 生成"看起来合理"的内容
_KB: dict[str, str] = {
    "langgraph": "LangGraph 是基于 Pregel(BSP) 模型的图编排运行时，"
                 "核心抽象是 StateGraph/Channel/Superstep，支持持久化、中断恢复与时间旅行。",
    "概念": "图(graph)由节点(步骤)和边(流转)组成；状态(state)经 reducer 合并；"
            "超步(superstep)是批量同步执行的单位。",
    "优势": "优势：显式控制流、天然支持循环与并行、checkpoint 带来断点续跑与人工审批。"
            "局限：学习曲线比线性链陡，简单任务略显重。",
    "应用": "典型场景：深度研究(多路检索汇总)、代码 agent、客服工单、"
            "需要审批的办公自动化、多 agent 协作系统。",
}


def _lookup(query: str) -> str:
    for kw, text in _KB.items():
        if kw.lower() in query.lower():
            return text
    return (f"关于「{query}」的模拟检索结果：该领域近期发展活跃，"
            f"主要方向包括基础能力、工程实践与生态整合三大块。")


@tool
def web_search(query: str) -> str:
    """搜索引擎查询。query: 检索关键词，返回结果列表的摘要文本。"""
    return ("结果1(mock.dev/1): " + _lookup(query) +
            "\n结果2(mock.dev/2): 相关工程实践与案例分析。"
            "\n结果3(mock.dev/3): 社区讨论与替代方案对比。")


@tool
def fetch_page(url: str) -> str:
    """抓取网页正文。url: 目标地址，返回页面主要文本内容。"""
    return (f"[{url} 正文] " +
            _lookup(url.split("/")[-1] or "langgraph") +
            " 本页还包含基准测试数据、常见坑清单与官方文档链接。")


TOOLS = [web_search, fetch_page]
