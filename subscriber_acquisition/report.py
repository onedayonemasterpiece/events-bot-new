from __future__ import annotations

import html
import re
from typing import Any

from telegraph import Telegraph

from runtime import require_main_attr
from models import AcqDiscoveryRun, AcqOpportunity, AcqSurface

_PHONE_RE = re.compile(r"(?<!\d)(?:\+?7|8)?[\s\-()]*(?:\d[\s\-()]*){10}(?!\d)")


def _safe_snippet(value: str | None, limit: int = 240) -> str:
    text = " ".join(str(value or "").split())
    text = _PHONE_RE.sub("[телефон скрыт]", text)
    if len(text) > limit:
        text = text[: limit - 1].rstrip() + "…"
    return text


def _a(url: str | None, label: str) -> str:
    if not url:
        return html.escape(label)
    return f'<a href="{html.escape(url, quote=True)}">{html.escape(label)}</a>'


def render_report_html(run: AcqDiscoveryRun, surfaces: list[AcqSurface], opportunities: list[AcqOpportunity], *, feedback_summary: dict[str, Any] | None = None) -> str:
    lines: list[str] = []
    stats = run.stats_json or {}
    lines.append("<h3>Сводка</h3>")
    lines.append("<ul>")
    lines.append(f"<li>run id: {run.id}</li>")
    lines.append(f"<li>status: {html.escape(run.status)}</li>")
    lines.append(f"<li>surfaces scanned: {stats.get('surfaces', len(surfaces))}</li>")
    lines.append(f"<li>opportunities found: {stats.get('opportunities', len(opportunities))}</li>")
    lines.append(f"<li>review cards posted: {stats.get('review_cards_posted', 0)}</li>")
    lines.append("</ul>")

    lines.append("<h3>Лучшие кандидаты</h3>")
    if not opportunities:
        lines.append("<p>Кандидатов нет.</p>")
    for opp in opportunities[:50]:
        target = _a(opp.link_target_url, opp.link_target_label or opp.link_target_kind or "target")
        context = _a(opp.context_url, "контекст")
        lines.append("<p>")
        lines.append(f"<b>#{opp.id or '—'} · {html.escape(opp.topic_cluster or opp.matched_intent or 'topic')}</b><br>")
        lines.append(f"{context} · target: {target}<br>")
        lines.append(f"Охват: ~{int(opp.reach_low or 0)} ({html.escape(opp.reach_confidence or 'low')}); риск: spam={html.escape(opp.spam_risk)}, safety={html.escape(opp.safety_risk)}<br>")
        lines.append(f"Почему: {html.escape(_safe_snippet(opp.context_text_snippet))}")
        lines.append("</p>")

    lines.append("<h3>Поверхности</h3>")
    for s in surfaces[:80]:
        lines.append(f"<p>{_a(s.url, s.title or s.handle or s.url)} · {html.escape(s.platform)}/{html.escape(s.surface_type)} · {html.escape(s.status)}</p>")

    lines.append("<h3>Sticker observations</h3>")
    sticker = [o for o in opportunities if o.sticker_fit and o.sticker_fit != "no"]
    if not sticker:
        lines.append("<p>Сильных sticker-кандидатов нет.</p>")
    for opp in sticker[:25]:
        lines.append(f"<p>#{opp.id or '—'} · {html.escape(opp.topic_cluster or '')}: {html.escape(opp.sticker_fit)} · {_a(opp.context_url, 'контекст')}</p>")

    lines.append("<h3>Feedback summary</h3>")
    fb = feedback_summary or {}
    lines.append(f"<p>approve={fb.get('approve', 0)} reject={fb.get('reject', 0)} keep={fb.get('keep', 0)} comments={fb.get('comment', 0)}</p>")
    return "\n".join(lines)


async def publish_telegraph_report(run: AcqDiscoveryRun, surfaces: list[AcqSurface], opportunities: list[AcqOpportunity]) -> str | None:
    html_content = render_report_html(run, surfaces, opportunities)
    token_fn = require_main_attr("get_telegraph_token")
    token = token_fn()
    if not token:
        return None
    tg = Telegraph(access_token=token)
    create_page = require_main_attr("telegraph_create_page")
    normalize = require_main_attr("normalize_telegraph_url")
    from telegraph.utils import html_to_nodes

    data = await create_page(
        tg,
        title=f"Subscriber Acquisition Discovery #{run.id}",
        content=html_to_nodes(html_content),
        return_content=False,
        caller="acq_discovery",
    )
    if not isinstance(data, dict):
        return None
    return normalize(data.get("url")) or data.get("url")
