#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { buildBehavioralSupplement } from './v1/behavioral.mjs';
import { captureBehaviorWithExactPlaywright } from './v1/behavioral/capture.mjs';
import { buildBehaviorHarness, materializeBehaviorHarness } from './v1/behavioral/harness.mjs';
import { materializeBehavioralEvidence } from './v1/behavioral/materialize.mjs';

const PINNED_SOURCE_SHA='ef7aa62e45c60f7a12da6160f490719c0721ec03';
const BASE_MANIFEST_SHA256='f7740f7f533c3f0cda5d4d0b8ebe98b565d7f521368b96462daecbd26522d5cc';
const sha=(value)=>createHash('sha256').update(value).digest('hex');
const arg=(name,fallback=null)=>{const at=process.argv.indexOf(name);return at>=0?process.argv[at+1]??fallback:fallback;};
const candidateRepo=resolve(arg('--candidate-repo'));const baseSnapshotRoot=resolve(arg('--base-snapshot-root'));const outputRoot=resolve(arg('--output'));const harnessRoot=resolve(arg('--harness-root'));const nodeModules=resolve(arg('--node-modules'));
if(!existsSync(join(candidateRepo,'site/src'))||!existsSync(join(baseSnapshotRoot,'manifest.json')))throw new Error('Behavior capture immutable inputs missing');
if(sha(readFileSync(join(baseSnapshotRoot,'manifest.json')))!==BASE_MANIFEST_SHA256)throw new Error('Behavior capture base Decoder v1 manifest mismatch');
mkdirSync(outputRoot,{recursive:true});const sourcePassRoot=join(outputRoot,'source-pass');const captureRoot=join(outputRoot,'capture');const supplementRoot=join(outputRoot,'supplement');
const sourcePass=buildBehavioralSupplement({sourceRoot:join(candidateRepo,'site'),baseSnapshotRoot,outputRoot:sourcePassRoot,sourceSha:PINNED_SOURCE_SHA});
const harness=materializeBehaviorHarness({candidateRepo,harnessRoot,nodeModules});const build=buildBehaviorHarness({harnessRoot});if(!build.ok)throw new Error(`Behavior harness build failed: ${build.stderr_tail}`);
const captured=await captureBehaviorWithExactPlaywright({nodeModules,dist:build.dist,outputDir:captureRoot});const materialized=materializeBehavioralEvidence({sourceSupplementRoot:sourcePassRoot,captureRoot,outputRoot:supplementRoot});
const receipt={schema_version:'current_ui_behavioral_capture_run_v1_1',status:'CAPTURE_COMPLETE_NO_GO_PENDING_REVIEW',source_sha:PINNED_SOURCE_SHA,base_manifest_sha256:BASE_MANIFEST_SHA256,harness:{source_copy_mode:harness.source_copy_mode,production_source_mutated:harness.production_source_mutated,generated_test_only_pages:harness.generated_test_only_pages,build_ok:build.ok},counts:{source_plans:sourcePass.records.plans.length,behavior_packets:materialized.plans.length,executable_packets:materialized.plans.filter((row)=>row.capture_status!=='explicit-blocker').length,explicit_blockers:captured.blockers.length,observations:captured.observations.length,rasters:captured.observations.length,reviews:0},supplement_manifest_sha256:sha(readFileSync(join(supplementRoot,'manifest.json'))),normalization_allowed:false,production_ui_mutated:false,immutable_v1_modified:false};writeFileSync(join(outputRoot,'capture-run-receipt.json'),`${JSON.stringify(receipt,null,2)}\n`);process.stdout.write(`${JSON.stringify(receipt)}\n`);
