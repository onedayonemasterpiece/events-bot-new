#!/usr/bin/env node
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadAndEnrichBreakpointMatrix } from './breakpoint-source.mjs';
import { buildBreakpointProbePlans } from './breakpoint-plan.mjs';
import { executeBreakpointProbesWithExactPlaywright, verifyImmutablePriorSupplement } from './breakpoint-runtime.mjs';
import { captureRailKeyboardWithExactPlaywright } from './rail-keyboard.mjs';
import { PINNED_SOURCE_SHA } from './registry.mjs';

function argument(argv,name,fallback=null){const at=argv.indexOf(name);return at>=0?argv[at+1]??fallback:fallback;}
function flag(argv,name){return argv.includes(name);}
export async function runProbeRuntimeClosure({matrixPath,sourceRoot,dist,nodeModules,outputDir,priorSupplementRoot=null,priorManifestSha256=null,onlyIds=null,maxRasters=24,breakpoints=true,rail=true,requireFullClosure=true}){
  const root=resolve(outputDir);mkdirSync(root,{recursive:true});let prior=null;if(priorSupplementRoot||priorManifestSha256){if(!priorSupplementRoot||!priorManifestSha256)throw new Error('Prior supplement root and manifest SHA-256 must be supplied together');prior=verifyImmutablePriorSupplement({supplementRoot:priorSupplementRoot,expectedManifestSha256:priorManifestSha256});}
  let breakpointResult=null;if(breakpoints){const rows=loadAndEnrichBreakpointMatrix({matrixPath,sourceRoot,sourceSha:PINNED_SOURCE_SHA});const plans=buildBreakpointProbePlans(rows);breakpointResult=await executeBreakpointProbesWithExactPlaywright({nodeModules,dist,plans,outputDir:root,maxRasters,onlyIds,requireFullClosure:requireFullClosure&&!onlyIds});}
  let railResult=null;if(rail)railResult=await captureRailKeyboardWithExactPlaywright({nodeModules,dist,sourceRoot,outputDir:root});
  const receipt={schema_version:'current_ui_behavioral_probe_runtime_closure_v1_1',status:'TERMINAL_EVIDENCE_COMPLETE',source_sha:PINNED_SOURCE_SHA,incremental_append_only:true,prior_reviewed_supplement:prior,breakpoint_receipt:breakpointResult?.receipt||null,rail_receipt:railResult?.receipt||null,production_ui_mutated:false,normalization_allowed:false,decision:'NOT_MERGED'};writeFileSync(join(root,'probe-runtime-closure-receipt.json'),`${JSON.stringify(receipt,null,2)}\n`);return {receipt,breakpointResult,railResult};
}

async function main(){
  const argv=process.argv.slice(2);const required=['--matrix','--source-root','--dist','--node-modules','--output'];for(const name of required)if(!argument(argv,name))throw new Error(`Missing ${name}`);
  const only=argument(argv,'--only-ids');const result=await runProbeRuntimeClosure({matrixPath:resolve(argument(argv,'--matrix')),sourceRoot:resolve(argument(argv,'--source-root')),dist:resolve(argument(argv,'--dist')),nodeModules:resolve(argument(argv,'--node-modules')),outputDir:resolve(argument(argv,'--output')),priorSupplementRoot:argument(argv,'--prior-supplement')?resolve(argument(argv,'--prior-supplement')):null,priorManifestSha256:argument(argv,'--prior-manifest-sha256'),onlyIds:only?only.split(',').filter(Boolean):null,maxRasters:Number(argument(argv,'--max-rasters','24')),breakpoints:!flag(argv,'--rail-only'),rail:!flag(argv,'--breakpoints-only'),requireFullClosure:!flag(argv,'--allow-partial')});process.stdout.write(`${JSON.stringify(result.receipt)}\n`);
}
if(process.argv[1]&&resolve(process.argv[1])===resolve(fileURLToPath(import.meta.url)))main().catch((error)=>{process.stderr.write(`${error.stack||error}\n`);process.exitCode=1;});
