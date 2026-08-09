#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { buildBehaviorHarness, materializeBehaviorHarness } from './v1/behavioral/harness.mjs';
import { materializeBehavioralClosure, PINNED_SOURCE_SHA, PRIOR_REVIEWED_MANIFEST_SHA256 } from './v1/behavioral/closure-materialize.mjs';
import { runProbeRuntimeClosure } from './v1/behavioral/probe-runtime.mjs';
import { assertBehavioralClosure } from './v1/behavioral/closure-validate.mjs';

const sha=(value)=>createHash('sha256').update(value).digest('hex');
const arg=(name,fallback=null)=>{const at=process.argv.indexOf(name);return at>=0?process.argv[at+1]??fallback:fallback;};
const candidateRepo=resolve(arg('--candidate-repo')),priorSupplement=resolve(arg('--prior-supplement')),output=resolve(arg('--output')),harnessRoot=resolve(arg('--harness-root')),nodeModules=resolve(arg('--node-modules'));
if(!existsSync(join(candidateRepo,'site/src'))||!existsSync(join(priorSupplement,'manifest.json')))throw new Error('Behavior closure immutable inputs missing');if(sha(readFileSync(join(priorSupplement,'manifest.json')))!==PRIOR_REVIEWED_MANIFEST_SHA256)throw new Error('Prior reviewed supplement manifest mismatch');
mkdirSync(output,{recursive:true});const probeRoot=join(output,'probe-runtime'),supplementRoot=join(output,'supplement');const harness=materializeBehaviorHarness({candidateRepo,harnessRoot,nodeModules});const build=buildBehaviorHarness({harnessRoot});if(!build.ok)throw new Error(`Behavior closure harness build failed: ${build.stderr_tail}`);
const runtime=await runProbeRuntimeClosure({matrixPath:join(priorSupplement,'breakpoint-and-container-matrix.jsonl'),sourceRoot:join(candidateRepo,'site'),dist:build.dist,nodeModules,outputDir:probeRoot,priorSupplementRoot:priorSupplement,priorManifestSha256:PRIOR_REVIEWED_MANIFEST_SHA256,maxRasters:12,breakpoints:true,rail:true,requireFullClosure:true});const materialized=materializeBehavioralClosure({priorSupplementRoot:priorSupplement,probeRoot,outputRoot:supplementRoot});const validation=assertBehavioralClosure(supplementRoot,{allowIncomplete:true});
const receipt={schema_version:'current_ui_behavioral_closure_capture_run_v1_1',status:'CAPTURE_COMPLETE_NO_GO_PENDING_REVIEW',source_sha:PINNED_SOURCE_SHA,prior_reviewed_manifest_sha256:PRIOR_REVIEWED_MANIFEST_SHA256,harness:{source_copy_mode:harness.source_copy_mode,production_source_mutated:harness.production_source_mutated,generated_test_only_pages:harness.generated_test_only_pages,build_ok:build.ok},counts:{terminal_probes:validation.terminal,pass:validation.pass,mismatch:validation.mismatch,unreachable:validation.unreachable,new_rasters:validation.new_rasters,new_reviews:0,total_observations:validation.observations},semantic_terminal_sha256:runtime.breakpointResult.receipt.counts.deterministic_sha256,normalization_allowed:false,production_ui_mutated:false,immutable_v1_modified:false,decision:'NOT_MERGED'};writeFileSync(join(output,'closure-capture-receipt.json'),`${JSON.stringify(receipt,null,2)}\n`);process.stdout.write(`${JSON.stringify(receipt)}\n`);
