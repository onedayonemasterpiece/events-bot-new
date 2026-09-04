from __future__ import annotations

import re
from pathlib import Path

ROOT = Path.cwd()
PAGE_PATH = ROOT / "site/src/components/DesktopEventPage.astro"
PANEL_PATH = ROOT / "site/src/components/DesktopEventActionPanel.astro"
TEST_PATH = ROOT / "site/tests/desktop-event-actions-single-style-owner.test.mjs"
DOC_PATH = ROOT / "docs/features/static-site-pages/event-page-product-design.md"
CHANGELOG_PATH = ROOT / "CHANGELOG.md"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one source match, found {count}")
    return text.replace(old, new, 1)


def find_matching(text: str, open_index: int, opening: str = "{", closing: str = "}") -> int:
    depth = 1
    index = open_index + 1
    quote: str | None = None
    while index < len(text):
        char = text[index]
        if quote:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if text.startswith("/*", index):
            end = text.find("*/", index + 2)
            if end < 0:
                raise RuntimeError("unterminated CSS comment")
            index = end + 2
            continue
        if char in {'"', "'"}:
            quote = char
        elif char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise RuntimeError(f"unbalanced {opening}{closing} block")


def find_statement_delimiter(text: str, start: int) -> tuple[int, str] | None:
    index = start
    quote: str | None = None
    paren = 0
    bracket = 0
    while index < len(text):
        char = text[index]
        if quote:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if text.startswith("/*", index):
            end = text.find("*/", index + 2)
            if end < 0:
                raise RuntimeError("unterminated CSS comment")
            index = end + 2
            continue
        if char in {'"', "'"}:
            quote = char
        elif char == "(":
            paren += 1
        elif char == ")":
            paren = max(0, paren - 1)
        elif char == "[":
            bracket += 1
        elif char == "]":
            bracket = max(0, bracket - 1)
        elif paren == 0 and bracket == 0 and char in {"{", ";"}:
            return index, char
        index += 1
    return None


def split_selector_list(selector: str) -> list[str]:
    parts: list[str] = []
    start = 0
    index = 0
    quote: str | None = None
    paren = 0
    bracket = 0
    while index < len(selector):
        char = selector[index]
        if quote:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if selector.startswith("/*", index):
            end = selector.find("*/", index + 2)
            if end < 0:
                raise RuntimeError("unterminated selector comment")
            index = end + 2
            continue
        if char in {'"', "'"}:
            quote = char
        elif char == "(":
            paren += 1
        elif char == ")":
            paren = max(0, paren - 1)
        elif char == "[":
            bracket += 1
        elif char == "]":
            bracket = max(0, bracket - 1)
        elif char == "," and paren == 0 and bracket == 0:
            parts.append(selector[start:index])
            start = index + 1
        index += 1
    parts.append(selector[start:])
    return parts


def unwrap_global(selector: str) -> str:
    marker = ":global("
    while marker in selector:
        start = selector.index(marker)
        open_index = start + len(":global")
        end = find_matching(selector, open_index, "(", ")")
        selector = selector[:start] + selector[open_index + 1:end] + selector[end + 1:]
    return selector


ACTION_TOKEN_RE = re.compile(
    r"\.desktop-prototype__(?:action(?:--[\w-]+)?|primary-action|icon-action|action-row)(?![\w-])"
)

BASELINE_DUPLICATE_SELECTORS = {
    ".desktop-prototype__action",
    ".desktop-prototype__action>p",
    ".desktop-prototype__action>pspan",
    ".desktop-prototype__action>pstrong",
    ".desktop-prototype__primary-action",
    ".desktop-prototype__primary-action.icon",
    ".desktop-prototype__primary-action .icon",
    ".desktop-prototype__primary-action.is-disabled",
    ".desktop-prototype__action-row",
    ".desktop-prototype__icon-action",
    ".desktop-prototype__icon-action .icon",
    ".desktop-prototype__icon-action strong",
}


def selector_key(selector: str) -> str:
    plain = unwrap_global(re.sub(r"/\*.*?\*/", "", selector, flags=re.S)).strip()
    return re.sub(r"\s+", "", plain)


BASELINE_DUPLICATE_KEYS = {re.sub(r"\s+", "", item) for item in BASELINE_DUPLICATE_SELECTORS}


def is_action_selector(selector: str) -> bool:
    plain = unwrap_global(re.sub(r"/\*.*?\*/", "", selector, flags=re.S))
    return bool(ACTION_TOKEN_RE.search(plain))


def rewrite_action_selector(selector: str) -> str | None:
    plain = unwrap_global(selector).strip()
    if ".desktop-prototype__action--media" in plain:
        return None
    plain = plain.replace(
        ".desktop-prototype__action--side",
        '[data-desktop-action-panel][data-action-variant="editorial-side"]',
    )
    plain = plain.replace(
        ".desktop-prototype__action--inline",
        '[data-desktop-action-panel][data-action-variant="split-inline"]',
    )
    plain = plain.replace(
        ".desktop-prototype__action--flow",
        '[data-desktop-action-panel][data-action-variant$="-flow"]',
    )
    plain = re.sub(
        r"\.desktop-prototype__action(?![-\w])",
        "[data-desktop-action-panel]",
        plain,
    )
    return f":global({plain})"


def split_css_ownership(block: str) -> tuple[str, str]:
    page_output: list[str] = []
    panel_output: list[str] = []
    position = 0

    while position < len(block):
        delimiter = find_statement_delimiter(block, position)
        if delimiter is None:
            page_output.append(block[position:])
            break

        index, kind = delimiter
        prelude = block[position:index]
        if kind == ";":
            page_output.append(prelude + ";")
            position = index + 1
            continue

        close = find_matching(block, index)
        body = block[index + 1:close]
        leading_match = re.match(r"\s*(?:/\*.*?\*/\s*)*", prelude, re.S)
        leading = leading_match.group(0) if leading_match else ""
        code = prelude[len(leading):]

        if code.lstrip().startswith("@"):
            page_body, panel_body = split_css_ownership(body)
            if page_body.strip():
                page_output.append(leading + code + "{" + page_body + "}")
            else:
                page_output.append(leading)
            if panel_body.strip():
                panel_output.append(code + "{" + panel_body + "}")
        else:
            page_selectors: list[str] = []
            action_selectors: list[str] = []
            for raw_selector in split_selector_list(code):
                selector = raw_selector.strip()
                if not selector:
                    continue
                if is_action_selector(selector):
                    if selector_key(selector) in BASELINE_DUPLICATE_KEYS:
                        continue
                    rewritten = rewrite_action_selector(selector)
                    if rewritten:
                        action_selectors.append(rewritten)
                else:
                    page_selectors.append(selector)

            if page_selectors:
                page_output.append(leading + ",\n".join(page_selectors) + "{" + body + "}")
            else:
                page_output.append(leading)
            if action_selectors:
                panel_output.append(",\n".join(action_selectors) + "{" + body + "}")

        position = close + 1

    return "".join(page_output), "\n".join(panel_output)


panel = PANEL_PATH.read_text(encoding="utf-8")
panel = replace_once(
    panel,
    """interface Props {
  event: PreviewEvent;
  shareImage?: string;
  shareImageTextMode?: PreviewEvent['image_text_mode'];
  className?: string;
  family?: 'split' | 'editorial';
}

const { event, shareImage = event.image_url || '', shareImageTextMode = event.image_text_mode || 'unknown', className = '', family = 'editorial' } = Astro.props;""",
    """type DesktopActionVariant = 'editorial-side' | 'split-inline' | 'editorial-flow' | 'split-flow';
type DesktopActionState = 'non-ocr' | 'ocr';

interface Props {
  event: PreviewEvent;
  shareImage?: string;
  shareImageTextMode?: PreviewEvent['image_text_mode'];
  variant: DesktopActionVariant;
  state: DesktopActionState;
}

const { event, shareImage = event.image_url || '', shareImageTextMode = event.image_text_mode || 'unknown', variant, state } = Astro.props;
const family = variant.startsWith('editorial') ? 'editorial' : 'split';""",
    "panel public props",
)
panel = replace_once(
    panel,
    """  class:list={['desktop-prototype__action', className]}
  data-feedback-scope
  data-desktop-action-panel
  data-action-family={family}
  data-action-layout={family === 'split' ? 'inline' : 'stacked'}""",
    """  class="desktop-prototype__action"
  data-feedback-scope
  data-desktop-action-panel
  data-action-variant={variant}
  data-action-state={state}
  data-action-family={family}
  data-action-layout={family === 'split' ? 'inline' : 'stacked'}""",
    "panel root boundary",
)
panel = replace_once(
    panel,
    "  :global(.desktop-prototype__primary-action .icon) { width: 1.5rem; height: 1.5rem; }",
    "  :global(.desktop-prototype__primary-action .icon) { width: var(--ke-icon-size-action); height: var(--ke-icon-size-action); }",
    "primary icon role",
)
panel = replace_once(
    panel,
    "  :global(.desktop-prototype__icon-action .icon) { width: 29px; height: 29px; }",
    "  :global(.desktop-prototype__icon-action .icon) { width: var(--ke-icon-size-action); height: var(--ke-icon-size-action); }",
    "secondary icon role",
)
panel = panel.replace(
    """  /* The success toast intentionally escapes the rounded CTA; a later page-level
     primary-action rule otherwise clips it even though it does not affect layout. */""",
    """  /* The success toast intentionally escapes the rounded CTA. This component
     owns both the action anatomy and the clipping exception. */""",
)

page = PAGE_PATH.read_text(encoding="utf-8")
page = replace_once(
    page,
    '<DesktopEventActionPanel event={event} shareImage={preferredUrl} shareImageTextMode={selectedMediaMode} className="desktop-prototype__action--side" family="editorial" />',
    '<DesktopEventActionPanel event={event} shareImage={preferredUrl} shareImageTextMode={selectedMediaMode} variant="editorial-side" state={mediaPolicy} />',
    "editorial action placement",
)
page = replace_once(
    page,
    '<DesktopEventActionPanel event={event} shareImage={preferredUrl} shareImageTextMode={selectedMediaMode} className="desktop-prototype__action--inline" family="split" />',
    '<DesktopEventActionPanel event={event} shareImage={preferredUrl} shareImageTextMode={selectedMediaMode} variant="split-inline" state={mediaPolicy} />',
    "split action placement",
)
page = replace_once(
    page,
    '<DesktopEventActionPanel event={event} shareImage={preferredUrl} shareImageTextMode={selectedMediaMode} className="desktop-prototype__action--flow" family={candidate} />',
    "<DesktopEventActionPanel event={event} shareImage={preferredUrl} shareImageTextMode={selectedMediaMode} variant={candidate === 'editorial' ? 'editorial-flow' : 'split-flow'} state={mediaPolicy} />",
    "flow action placement",
)
page = replace_once(
    page,
    "const editorialAction = editorialSide?.querySelector<HTMLElement>(':scope > .desktop-prototype__action');",
    """const editorialAction = editorialSide?.querySelector<HTMLElement>(
        ':scope > [data-desktop-action-panel][data-action-variant="editorial-side"]',
      );""",
    "editorial action runtime query",
)

style_start = page.rfind("<style>")
style_end = page.rfind("</style>")
if style_start < 0 or style_end < style_start:
    raise RuntimeError("DesktopEventPage style block not found")
page_style = page[style_start + len("<style>"):style_end]
page_style, migrated_action_css = split_css_ownership(page_style)
remaining_action_rules = [
    selector.strip()
    for selector in re.findall(r"([^{}]+)\{", page_style)
    if is_action_selector(selector)
]
if remaining_action_rules:
    raise RuntimeError(f"consumer CSS still owns action selectors: {remaining_action_rules[:8]}")
if not migrated_action_css.strip():
    raise RuntimeError("no page-context action CSS was migrated to the component")

page = page[:style_start + len("<style>")] + page_style + page[style_end:]
PAGE_PATH.write_text(page, encoding="utf-8")

panel_css_entry = f"""

  /*
   * Public placement variants migrated from DesktopEventPage.astro.
   * The page supplies only variant/state; this component remains the sole
   * anatomy and CSS owner for its root and descendants.
   */
{migrated_action_css}
"""
panel = replace_once(panel, "\n</style>", panel_css_entry + "\n</style>", "panel migrated CSS")
PANEL_PATH.write_text(panel, encoding="utf-8")

test_source = r'''import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function read(relativePath) {
  return readFile(path.join(siteRoot, relativePath), 'utf8');
}

function lastStyle(source) {
  const start = source.lastIndexOf('<style>');
  const end = source.lastIndexOf('</style>');
  assert.ok(start >= 0 && end > start, 'expected a terminal style block');
  return source.slice(start + '<style>'.length, end);
}

function stripComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//gu, '');
}

test('DesktopEventActionPanel is the single desktop action anatomy and CSS owner', async () => {
  const page = await read('src/components/DesktopEventPage.astro');
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const pageStyle = stripComments(lastStyle(page));
  const panelStyle = stripComments(lastStyle(panel));

  assert.equal((page.match(/<DesktopEventActionPanel\b/gu) || []).length, 3);
  assert.equal((page.match(/state=\{mediaPolicy\}/gu) || []).length, 3);
  assert.match(page, /variant="editorial-side" state=\{mediaPolicy\}/u);
  assert.match(page, /variant="split-inline" state=\{mediaPolicy\}/u);
  assert.match(page, /variant=\{candidate === 'editorial' \? 'editorial-flow' : 'split-flow'\} state=\{mediaPolicy\}/u);
  assert.doesNotMatch(page, /className="desktop-prototype__action--/u);
  assert.doesNotMatch(page, /<DesktopEventActionPanel[^>]+\bfamily=/u);

  assert.match(panel, /type DesktopActionVariant = 'editorial-side' \| 'split-inline' \| 'editorial-flow' \| 'split-flow';/u);
  assert.match(panel, /type DesktopActionState = 'non-ocr' \| 'ocr';/u);
  assert.match(panel, /data-action-variant=\{variant\}/u);
  assert.match(panel, /data-action-state=\{state\}/u);
  assert.match(panel, /data-action-family=\{family\}/u);
  assert.doesNotMatch(panel, /className\?: string/u);
  assert.doesNotMatch(panel, /family\?: 'split' \| 'editorial'/u);

  for (const selector of [
    'desktop-prototype__action',
    'desktop-prototype__primary-action',
    'desktop-prototype__icon-action',
    'desktop-prototype__action-row',
  ]) {
    assert.doesNotMatch(pageStyle, new RegExp(`\\.${selector}(?:--|(?=[\\s>.:#\\[]))`, 'u'));
    assert.match(panelStyle, new RegExp(selector, 'u'));
  }
  assert.match(panelStyle, /data-desktop-action-panel/u);
  assert.match(panelStyle, /data-action-variant="editorial-side"/u);
  assert.match(panelStyle, /data-action-variant="split-inline"/u);
});

test('desktop action controls keep accepted targets and central icon roles', async () => {
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const panelStyle = stripComments(lastStyle(panel));

  assert.match(panelStyle, /:global\(\.desktop-prototype__primary-action\) \{[\s\S]*?min-height: 56px;/u);
  assert.match(panelStyle, /:global\(\.desktop-prototype__icon-action\) \{[\s\S]*?min-width: 52px;[\s\S]*?min-height: 52px;/u);
  assert.match(panelStyle, /desktop-prototype__icon-action\)[^{]*\{[\s\S]*?min-width:56px;[\s\S]*?min-height:56px;/u);
  assert.equal((panelStyle.match(/var\(--ke-icon-size-action\)/gu) || []).length, 4);
  assert.doesNotMatch(panelStyle, /desktop-prototype__primary-action \.icon\)[^{]*\{[^}]*1\.5rem/u);
  assert.doesNotMatch(panelStyle, /desktop-prototype__icon-action \.icon\)[^{]*\{[^}]*29px/u);
});
'''
TEST_PATH.write_text(test_source, encoding="utf-8")

doc = DOC_PATH.read_text(encoding="utf-8")
doc_marker = '''- share/calendar/ticket clicks must attach `event_id`, `surface`, `viewport_class`, `layout_mode`, `served_list_id/hash` when applicable.

## 6. Mobile layout'''
doc_entry = '''- share/calendar/ticket clicks must attach `event_id`, `surface`, `viewport_class`, `layout_mode`, `served_list_id/hash` when applicable.

### Desktop action-panel ownership

`DesktopEventActionPanel.astro` is the single anatomy and CSS owner for the
primary CTA, calendar/share/like row, counters, control targets and icon sizing.
`DesktopEventPage.astro` places the component and passes one explicit named pair:

- `variant`: `editorial-side`, `split-inline`, `editorial-flow` or `split-flow`;
- `state`: `non-ocr` or `ocr`.

The page must not restyle `.desktop-prototype__primary-action`,
`.desktop-prototype__icon-action`, `.desktop-prototype__action-row` or duplicate
the component root. Desktop control targets remain at least 52px (therefore
above the 44px floor), and visible action glyphs consume
`--ke-icon-size-action`. This boundary is structural only: the accepted palette,
share/like/calendar behavior, phone reveal/copy lifecycle and responsive fit
measurement remain unchanged.

## 6. Mobile layout'''
doc = replace_once(doc, doc_marker, doc_entry, "canonical desktop action owner doc entry")
DOC_PATH.write_text(doc, encoding="utf-8")

changelog = CHANGELOG_PATH.read_text(encoding="utf-8")
changelog_marker = "# Changelog\n\n## [Unreleased]\n\n"
changelog_entry = '''# Changelog

## [Unreleased]

- Changed: `DesktopEventActionPanel` is now the sole desktop event-action
  anatomy and CSS owner. `DesktopEventPage` passes explicit placement variants
  and media states instead of duplicating root/primary/action-row selectors;
  the accepted palette and interactions are preserved, control targets remain
  at least 52px, and action glyphs use the central action icon role.

'''
changelog = replace_once(changelog, changelog_marker, changelog_entry, "changelog entry")
CHANGELOG_PATH.write_text(changelog, encoding="utf-8")
