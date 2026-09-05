from pathlib import Path
import json

def replace(path, old, new):
    p=Path(path)
    s=p.read_text()
    assert s.count(old)==1, (path,s.count(old))
    p.write_text(s.replace(old,new))

replace('site/src/styles/design-system.css', '.ke-popular-behavior__head h2 { margin: 0; font-size: 1.35rem; line-height: 1.1; }', '.ke-popular-behavior__head h2 { margin: 0; font: var(--ke-type-h2); letter-spacing: var(--ke-type-h2-letter); }')
replace('site/src/styles/design-system.css', '    font-size: 1.5rem;\n    font-weight: 900;\n    line-height: 1.05;\n    letter-spacing: -.025em;', '    font: var(--ke-type-h2);\n    letter-spacing: var(--ke-type-h2-letter);')
replace('site/src/layouts/EventLayout.astro', 'max-width:min(25vw,20rem); pointer-events:auto; }', 'max-width:min(25vw,20rem); pointer-events:auto; background:var(--ke-color-background-header); }')
p=Path('site/src/components/design-system/f0-typography-authority.v1.json')
d=json.loads(p.read_text())
role=d['owner_review_roles']['section_title']
role['consumers'].append('PopularBehaviorSections')
role['viewports']={str(w):{'fontSize':s,'fontWeight':900,'lineHeight':round(s*1.05,3),'letterSpacing':round(-s*.035,3)} for w,s in [(1440,40),(390,28),(1920,40)]}
role['excluded_roles']=['time-marker','festival-month-index','card-title','sticky-compact-context','footer']
p.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n')
replace('site/scripts/check-browser-release-gate.mjs', 'export function assertRegularGridWidths(grid) {', '''export function assertOwnerSectionMetrics(section, width) {
  const expected = ownerTypography.owner_review_roles.section_title.viewports[String(width)];
  invariant(expected, `no section-title expectation for ${width}`);
  for (const [key,value] of Object.entries(expected)) invariant(Math.abs(Number.parseFloat(section.style?.[key])-value)<0.12, `same-role H2 ${key} mismatch: ${section.text}`);
}

export function assertRegularGridWidths(grid) {''')
replace('site/scripts/check-browser-release-gate.mjs', 'tag:e.tagName,text:e.textContent.trim(),class:e.className,box:box(e),style:style(e),', '''tag:e.tagName,text:e.textContent.trim(),class:e.className,box:box(e),style:style(e),
      sectionRole:e.matches('.ke-popular-behavior__head:not(.is-stuck) h2,.free-collection__results > h2,.ex-group__heading h2,.ex-tail-callout h2,.unusual-page__empty h2'),''')
replace('site/scripts/check-browser-release-gate.mjs', '        assertOwnerTitleMetrics(titles[0], width);', '''        assertOwnerTitleMetrics(titles[0], width);
        for (const section of report.headings.filter(h=>h.sectionRole)) assertOwnerSectionMetrics(section,width);
        const sectionNode=page.locator('.ke-popular-behavior__head:not(.is-stuck) h2,.free-collection__results > h2,.ex-group__heading h2,.ex-tail-callout h2,.unusual-page__empty h2').first();
        if(await sectionNode.count()) {
          const previous=await sectionNode.getAttribute('style');
          await sectionNode.evaluate(e=>e.style.setProperty('font-size','17px','important'));
          const poisoned=(await measureOwnerReviewPage(page)).headings.find(h=>h.sectionRole);
          let rejected=false;try {assertOwnerSectionMetrics(poisoned,width);} catch {rejected=true;}
          await sectionNode.evaluate((e,old)=>old===null?e.removeAttribute('style'):e.setAttribute('style',old),previous);
          invariant(rejected,'section gate accepted an overriding local font size');
          report.negativeSectionOverrideRejected=true;
        }''')
replace('site/scripts/check-browser-release-gate.mjs', "    const state=await page.locator('[data-floating-page-context]').evaluate(e=>{", "    await page.waitForTimeout(400); // Document the settled shell transition.\n    const state=await page.locator('[data-floating-page-context]').evaluate(e=>{")
replace('site/scripts/check-browser-release-gate.mjs', "label:button.getAttribute('aria-label'),home:e.querySelector('a')?.getAttribute('href'),", "label:button.getAttribute('aria-label'),home:e.querySelector('a')?.getAttribute('href'),background:getComputedStyle(e).backgroundColor,")
replace('site/scripts/check-browser-release-gate.mjs', "    invariant(!state.overlapsMenu,'Floating context covers the mobile menu');", "    invariant(!state.overlapsMenu,'Floating context covers the mobile menu');\n    invariant(/^rgb\\(/.test(state.background),'Schematic context must have an opaque canonical backing');")
replace('site/scripts/browser-release-gate.behavior.test.mjs', '  assertOwnerTitleMetrics,', '  assertOwnerTitleMetrics,\n  assertOwnerSectionMetrics,')
p=Path('site/scripts/browser-release-gate.behavior.test.mjs')
p.write_text(p.read_text()+'''
// Same section role, irrespective of route palette or archetype composition.
test('same-role sections reject locally shrunken type despite intact markers',()=>{
 const section={text:'Раздел',style:{fontSize:'40px',fontWeight:'900',lineHeight:'42px',letterSpacing:'-1.4px'}};
 assert.doesNotThrow(()=>assertOwnerSectionMetrics(section,1440));
 assert.throws(()=>assertOwnerSectionMetrics({...section,style:{...section.style,fontSize:'21.6px'}},1440),/same-role H2/);
});
''')
