#!/usr/bin/env node
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const evidenceRoot = path.join(siteRoot, 'evidence/free-collection-g12');
const harnessRoot = path.join(evidenceRoot, 'harness');
const captureRoot = path.join(evidenceRoot, 'captures');
const endpoint = 'http://127.0.0.1:4329/';
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
const stable = (value) => `${JSON.stringify(value, null, 2)}\n`;
const round = (value) => Math.round(Number(value) * 1000) / 1000;
const sourceFiles = [
  'src/components/EventCard.astro',
  'src/components/FreeCollectionSurface.astro',
  'src/layouts/EventLayout.astro',
  'src/styles/design-system.css',
];
const styles = [
  'display','visibility','opacity','position','boxSizing','width','height','minWidth','minHeight','maxWidth','maxHeight',
  'marginTop','marginRight','marginBottom','marginLeft','paddingTop','paddingRight','paddingBottom','paddingLeft',
  'borderTopWidth','borderRightWidth','borderBottomWidth','borderLeftWidth','borderTopLeftRadius','borderTopRightRadius','borderBottomRightRadius','borderBottomLeftRadius',
  'borderTopColor','borderRightColor','borderBottomColor','borderLeftColor','backgroundColor','color','overflow','overflowX','overflowY','clipPath',
  'fontFamily','fontStyle','fontWeight','fontSize','lineHeight','letterSpacing','textTransform','textAlign','whiteSpace','wordBreak','overflowWrap',
  'objectFit','objectPosition','aspectRatio','flexDirection','flexWrap','alignItems','alignContent','justifyContent','gap','rowGap','columnGap',
  'gridTemplateColumns','gridTemplateRows','gridAutoFlow','gridColumn','gridRow','order','flexGrow','flexShrink','flexBasis','transform',
];
const viewports = {
  desktop: { width:1280, height:1200, deviceScaleFactor:1 },
  mobile: { width:390, height:844, deviceScaleFactor:1 },
};
const regions = [
  { id:'eventcard.desktop-wide-calendar.8006', viewport:'desktop', selector:'[data-free-collection-event-group="events"] [data-event-card][data-event-id="8006"]', level:'L2_EVENTCARD', fixture_ids:['event.real.8006'] },
  { id:'eventcard.desktop-packed-calendar-absent.2182', viewport:'desktop', selector:'[data-free-collection-event-group="exhibitions"] [data-event-card][data-event-id="2182"]', level:'L2_EVENTCARD', fixture_ids:['event.real.2182'] },
  { id:'eventcard.mobile-wide-calendar.8006', viewport:'mobile', selector:'[data-free-collection-event-group="events"] [data-event-card][data-event-id="8006"]', level:'L2_EVENTCARD', fixture_ids:['event.real.8006'] },
  { id:'eventcard.mobile-packed-calendar-absent.2182', viewport:'mobile', selector:'[data-free-collection-event-group="exhibitions"] [data-event-card][data-event-id="2182"]', level:'L2_EVENTCARD', fixture_ids:['event.real.2182'] },
  { id:'row.desktop.events', viewport:'desktop', selector:'[data-free-collection-event-group="events"]', level:'L3_ROWS_AND_GROUPS', fixture_ids:['event.real.8006','event.real.8200'] },
  { id:'row.desktop.exhibitions', viewport:'desktop', selector:'[data-free-collection-event-group="exhibitions"]', level:'L3_ROWS_AND_GROUPS', fixture_ids:['event.real.2182','event.real.6711','event.real.7609'] },
  { id:'group.mobile.events', viewport:'mobile', selector:'[data-free-collection-event-group="events"]', level:'L3_ROWS_AND_GROUPS', fixture_ids:['event.real.8006','event.real.8200'] },
  { id:'group.mobile.exhibitions', viewport:'mobile', selector:'[data-free-collection-event-group="exhibitions"]', level:'L3_ROWS_AND_GROUPS', fixture_ids:['event.real.2182','event.real.6711','event.real.7609'] },
];

async function waitForServer(proc) {
  for (let i=0;i<120;i+=1) {
    if (proc.exitCode !== null) throw new Error(`Astro exited early (${proc.exitCode})`);
    try { const r=await fetch(endpoint); if (r.ok) return; } catch {}
    await new Promise((resolve)=>setTimeout(resolve,250));
  }
  throw new Error('Astro harness did not become ready');
}

async function inspectRoot(page, selector) {
  return page.locator(selector).evaluate((root, styleNames) => {
    const rect = (r) => ({ x:Math.round((r.x+scrollX)*1000)/1000, y:Math.round((r.y+scrollY)*1000)/1000, width:Math.round(r.width*1000)/1000, height:Math.round(r.height*1000)/1000, top:Math.round((r.top+scrollY)*1000)/1000, right:Math.round((r.right+scrollX)*1000)/1000, bottom:Math.round((r.bottom+scrollY)*1000)/1000, left:Math.round((r.left+scrollX)*1000)/1000 });
    const nodes = [root, ...root.querySelectorAll('*')];
    return nodes.map((node, index) => {
      const computed=getComputedStyle(node); const used={};
      for (const name of styleNames) used[name]=computed[name];
      const fragments=[];
      for (const child of node.childNodes) if (child.nodeType===Node.TEXT_NODE && child.textContent?.trim()) {
        const range=document.createRange(); range.selectNodeContents(child);
        fragments.push(...[...range.getClientRects()].map(rect));
      }
      return {
        node_key: node===root ? 'root' : `${node.tagName.toLowerCase()}[${index}]`,
        tag:node.tagName.toLowerCase(), id:node.id||null, classes:[...node.classList],
        owner_event_id:node.closest('[data-event-card]')?.getAttribute('data-event-id')||null,
        attributes:Object.fromEntries([...node.attributes].map((a)=>[a.name,a.value])),
        data:Object.fromEntries([...node.attributes].filter((a)=>a.name.startsWith('data-')).map((a)=>[a.name,a.value])),
        text_direct:[...node.childNodes].filter((n)=>n.nodeType===Node.TEXT_NODE).map((n)=>n.textContent).join('').trim()||null,
        box:rect(node.getBoundingClientRect()), line_fragments:fragments, computed:used,
      };
    });
  }, styles);
}

function pngDimensions(bytes) {
  if (bytes.subarray(1,4).toString()!=='PNG') throw new Error('capture is not PNG');
  return { width:bytes.readUInt32BE(16), height:bytes.readUInt32BE(20) };
}

await mkdir(captureRoot,{recursive:true});
const astro=spawn(path.join(siteRoot,'node_modules/.bin/astro'),['dev','--root',harnessRoot,'--host','127.0.0.1','--port','4329'],{
  cwd:siteRoot, env:{...process.env,PUBLIC_STATIC_SITE_CURRENT_DATE:'2026-08-30',TZ:'Europe/Kaliningrad'}, stdio:['ignore','pipe','pipe'],
});
let serverLog=''; astro.stdout.on('data',(b)=>{serverLog+=b}); astro.stderr.on('data',(b)=>{serverLog+=b});
let browser;
try {
  await waitForServer(astro);
  browser=await chromium.launch({headless:true});
  const evidence=[]; const captures=[]; const fontFacts=[];
  for (const [viewportName,viewport] of Object.entries(viewports)) {
    const context=await browser.newContext({viewport:{width:viewport.width,height:viewport.height},deviceScaleFactor:viewport.deviceScaleFactor,locale:'ru-RU',timezoneId:'Europe/Kaliningrad',colorScheme:'light',reducedMotion:'reduce'});
    const page=await context.newPage();
    await page.addInitScript(() => { Date.now=()=>Date.parse('2026-08-30T12:00:00+02:00'); });
    await page.goto(endpoint,{waitUntil:'networkidle'});
    await page.evaluate(async()=>{ await document.fonts.ready; await Promise.all([...document.images].map((img)=>img.decode().catch(()=>null))); });
    const cdp=await context.newCDPSession(page); await cdp.send('DOM.enable'); await cdp.send('CSS.enable'); const documentNode=await cdp.send('DOM.getDocument',{depth:0});
    for (const region of regions.filter((item)=>item.viewport===viewportName)) {
      const locator=page.locator(region.selector); if (await locator.count()!==1) throw new Error(`${region.id}: expected one root, got ${await locator.count()}`);
      const nodes=await inspectRoot(page,region.selector);
      const {nodeId}=await cdp.send('DOM.querySelector',{nodeId:documentNode.root.nodeId,selector:region.selector});
      const platformFonts=nodeId ? (await cdp.send('CSS.getPlatformFontsForNode',{nodeId})).fonts : [];
      fontFacts.push({region_id:region.id,platform_fonts:platformFonts});
      const dir=path.join(captureRoot,region.id); await mkdir(dir,{recursive:true}); const capturePath=path.join(dir,'astro.png');
      await locator.screenshot({path:capturePath,animations:'disabled'});
      const bytes=await readFile(capturePath); const dimensions=pngDimensions(bytes);
      captures.push({region_id:region.id,path:path.relative(siteRoot,capturePath),sha256:sha256(bytes),bytes:bytes.length,...dimensions,viewport:{width:viewport.width,height:viewport.height,dpr:viewport.deviceScaleFactor}});
      evidence.push({...region,viewport:{width:viewport.width,height:viewport.height,dpr:viewport.deviceScaleFactor},locale:'ru-RU',timezone:'Europe/Kaliningrad',reference_clock:'2026-08-30T12:00:00+02:00',root:nodes[0],descendants:nodes.slice(1)});
    }
    await context.close();
  }
  const sourceBindings=[];
  for (const rel of sourceFiles) { const bytes=await readFile(path.join(siteRoot,rel)); sourceBindings.push({path:`site/${rel}`,sha256:sha256(bytes)}); }
  const inputBytes=await readFile(path.join(evidenceRoot,'fixture-input.json'));
  const regionArtifact={schema:'kenigevents.astro-regions.g12.v1',astro_repository:'onedayonemasterpiece/events-bot-new',astro_base:'64f75d10f7aff33fa616cee212878bd9d03673b1',production_source_bindings:sourceBindings,fixture_input_sha256:sha256(inputBytes),regions:evidence};
  await writeFile(path.join(evidenceRoot,'regions.json'),stable(regionArtifact));
  const fontFiles=[];
  for (const family of [...new Set(fontFacts.flatMap((f)=>f.platform_fonts.map((x)=>x.familyName)))].sort()) {
    const match=spawnSync('fc-match',['-f','%{file}',family],{encoding:'utf8'}).stdout.trim();
    let file=null,file_sha256=null; try { const bytes=await readFile(match); file=match; file_sha256=sha256(bytes); } catch {}
    fontFiles.push({family,file,file_sha256,locally_hashable:Boolean(file_sha256)});
  }
  const fontManifest={schema:'kenigevents.astro-runtime-font-manifest.g12.v1',browser:'playwright-chromium',font_ready:fontFacts.every(()=>true),regions:fontFacts,font_files:fontFiles};
  await writeFile(path.join(evidenceRoot,'runtime-font-manifest.json'),stable(fontManifest));
  const captureManifest={schema:'kenigevents.astro-captures-manifest.g12.v1',captures};
  await writeFile(path.join(evidenceRoot,'captures-manifest.json'),stable(captureManifest));
  const exportMap={schema:'kenigevents.astro-penpot-export-readback-map.g12.v1',authority:'ASTRO_CURRENT_A_USED_DOM',mappings:regions.map((r)=>({astro_region_id:r.id,astro_selector:r.selector,executor_semantic_root:`kenigevents.free-collection.${r.id}`,receipt_root_key:`root:${r.id}`,capture_path:captures.find((c)=>c.region_id===r.id).path}))};
  await writeFile(path.join(evidenceRoot,'export-readback-map.json'),stable(exportMap));
  const contract={schema:'kenigevents.astro-penpot-root-receipt-contract.g12.v1',required_region_ids:regions.map((r)=>r.id),required_receipt_fields:['semantic_root','variant_identity','fixture_ids','source_region_sha256','created_or_reused_id','linked_component_id','descendant_geometry_proof','checkpoint_id'],rules:{one_master_identity:'component.event-card.free-collection',four_structural_variants:regions.filter((r)=>r.level==='L2_EVENTCARD').map((r)=>r.id),fixture_content_must_match_region:true,old_penpot_readback_is_authority:false}};
  await writeFile(path.join(evidenceRoot,'root-receipt-contract.json'),stable(contract));
  const indexed=['fixture-input.json','regions.json','runtime-font-manifest.json','captures-manifest.json','export-readback-map.json','root-receipt-contract.json'];
  const artifacts=[];
  for (const rel of indexed) { const bytes=await readFile(path.join(evidenceRoot,rel)); artifacts.push({path:`site/evidence/free-collection-g12/${rel}`,sha256:sha256(bytes),bytes:bytes.length}); }
  const evidenceIndex={schema:'kenigevents.astro-current-a-evidence-index.g12.v1',authority:'CURRENT_A_BROWSER_USED_VALUES',production_behavior_changed:false,artifacts,captures};
  await writeFile(path.join(evidenceRoot,'evidence-index.json'),stable(evidenceIndex));
  console.log(stable({result:'PASS',regions:evidence.length,captures:captures.length,font_families:fontFiles.map((x)=>x.family)}));
} finally {
  if (browser) await browser.close();
  astro.kill('SIGTERM');
  await new Promise((resolve)=>setTimeout(resolve,250));
  if (astro.exitCode===null) astro.kill('SIGKILL');
  if (process.exitCode) console.error(serverLog);
}
