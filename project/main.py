"""
main.py — 深度研究系统 CLI 入口（端到端演示）
=============================================
用法（在 langgraph-learning/ 目录下）：
    python -m project.main                     # 完整演示：研究->并行->写作->审批->记忆
    python -m project.main --interactive       # 审批处人工输入 yes/no
    python -m project.main --topic "多模态RAG"  # 自定义主题
    python -m project.main --timetravel        # 额外演示时间旅行fork

演示的 LangGraph 能力：
  流式(updates+custom+subgraphs) / interrupt暂停 / Command(resume)恢复 /
  同thread续跑(会话记忆) / Store跨会话记忆 / 时间旅行fork
"""

import argparse
import sys

from langgraph.types import Command

from project.graph import build_graph, default_config


# ---------------------------------------------------------------- 可视化辅助

def fmt_update(ev: dict) -> str:
    parts = []
    for node, update in ev.items():
        if update is None:
            parts.append(node)
        elif isinstance(update, dict):
            keys = [k for k in update if k != "messages"] or ["messages"]
            parts.append(f"{node}{keys}")
    return " ".join(parts)


def stream_until_done(graph, inputs, cfg, interactive: bool):
    """流式跑到图结束或 interrupt 暂停；返回 (是否暂停, 最后状态)。

    组合模式 + subgraphs=True 时 chunk 为三元组 (namespace, mode, payload)。
    """
    for chunk in graph.stream(inputs, cfg, stream_mode=["updates", "custom"],
                              subgraphs=True):
        ns, mode, data = chunk
        tag = "/".join(ns) if ns else "主图"
        if mode == "custom":
            print(f"    · [{tag}] 进度 {data}")
        else:
            line = fmt_update(data)
            if line:
                print(f"  ▣ [{tag}] {line}")
    snap = graph.get_state(cfg)
    paused = bool(snap.next)               # 还有待办 => 被 interrupt 暂停
    return paused, snap


def resolve_approval(snap, interactive: bool) -> str:
    payload = snap.tasks[0].interrupts[0].value
    print("\n  ⏸ 图已暂停（人工审阅）:")
    for k, v in payload.items():
        print(f"     {k}: {str(v)[:80]}")
    if interactive:
        return input("     你的审批 (yes/no): ").strip()
    print("     -- 自动审批: yes --\n")
    return "yes"


# ---------------------------------------------------------------- 主流程

def run_research(graph, topic: str, thread: str, user: str, interactive: bool) -> dict:
    cfg = default_config(thread, user)
    print(f"\n========== 新研究任务: 《{topic}》 thread={thread} ==========")
    paused, snap = stream_until_done(graph, {"topic": topic}, cfg, interactive)

    rounds = 0
    while paused and rounds < 5:                     # 打回->重写->再审 循环
        answer = resolve_approval(snap, interactive)
        paused, snap = stream_until_done(graph, Command(resume=answer), cfg, interactive)
        rounds += 1

    out = graph.get_state(cfg).values
    print(f"\n  ✔ 完成: findings={len(out.get('findings') or [])}份 "
          f"approved={out.get('approved')} rounds={out.get('review_rounds')}")
    print("  ---- 报告预览 ----")
    print("\n".join((out.get("report") or "").splitlines()[:8]))
    return out


def demo_timetravel(graph, base_thread: str, user: str) -> None:
    """时间旅行：回到'写作之前'的快照重放，走出另一条审批路径。

    说明：InMemorySaver 的快照按 thread_id 寻址，跨线程 fork 取不到
    （换新 thread_id + 旧 checkpoint_id 会被当成全新运行），
    因此这里用官方"同线程回放"模式：直接使用快照自带的 config。
    """
    print("\n========== 时间旅行演示（回放重走）==========")
    cfg = default_config(base_thread, user)
    orig = graph.get_state(cfg).values
    hist = list(graph.get_state_history(cfg))
    # 找"即将进入 writer"的快照（report 尚未生成）
    before_write = next(s for s in hist
                        if not (s.values or {}).get("report") and "writer" in (s.next or ()))
    print(f"  历史快照 {len(hist)} 个; 回到写作前 (next={before_write.next})")

    replay_cfg = before_write.config          # 快照自带 config（同thread+ckpt_id）
    # 注意：只有"第一次续跑"用 replay_cfg（含旧checkpoint_id）；
    # 之后的 resume 必须用线程常规 config，否则会反复回到旧快照死循环
    paused, snap = stream_until_done(graph, None, replay_cfg, interactive=False)
    answers = iter(["no", "yes"])             # 先打回一次，再通过
    while paused:
        try:
            ans = next(answers)
        except StopIteration:
            ans = "yes"
        paused, snap = stream_until_done(graph, Command(resume=ans), cfg,
                                         interactive=False)
    new = graph.get_state(cfg).values
    print(f"  原时间线: approved={orig.get('approved')} rounds={orig.get('review_rounds')}")
    print(f"  回放后  : approved={new.get('approved')} rounds={new.get('review_rounds')}"
          f"（走了 打回->重写->通过 的另一条路）")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="LangGraph")
    ap.add_argument("--interactive", action="store_true")
    ap.add_argument("--timetravel", action="store_true")
    args = ap.parse_args()

    graph = build_graph()

    # 场景1：完整研究流（含 interrupt 审批与可能的打回循环）
    run_research(graph, args.topic, "research-1", "u1", args.interactive)

    # 场景2：同 thread 续跑 => 会话内记忆（checkpoint）+ 跨会话记忆（store）
    print("\n========== 追加研究（同thread，验证持久化记忆）==========")
    out2 = {}
    cfg2 = default_config("research-1", "u1")
    paused, snap = stream_until_done(graph, {"topic": args.topic + "（续）"}, cfg2,
                                     args.interactive)
    while paused:
        answer = resolve_approval(snap, args.interactive)
        paused, snap = stream_until_done(graph, Command(resume=answer), cfg2,
                                         args.interactive)
    out2 = graph.get_state(cfg2).values
    print(f"  ✔ 续跑完成: 第二次研究的findings="
          f"{len(out2.get('findings') or [])}份（第一条消息数={len(graph.get_state(cfg2).values.get('messages') or [])}）")

    # 场景3：换 thread 但同 user => Store 长期记忆跨会话生效
    print("\n========== 新会话（新thread同user，验证Store长期记忆）==========")
    cfg3 = default_config("research-2", "u1")
    paused, snap = stream_until_done(graph, {"topic": "Agent编排框架对比"}, cfg3,
                                     args.interactive)
    while paused:
        answer = resolve_approval(snap, args.interactive)
        paused, snap = stream_until_done(graph, Command(resume=answer), cfg3,
                                         args.interactive)
    print("  ✔ 新会话完成（load_memory 应能召回历史研究记录）")

    if args.timetravel:
        demo_timetravel(graph, "research-1", "u1")

    print("\n全部演示结束。更多玩法: python -m project.main --interactive --timetravel")


if __name__ == "__main__":
    sys.exit(main())
