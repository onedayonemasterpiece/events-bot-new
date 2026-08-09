#!/usr/bin/env node
import { resolve } from 'node:path';
import { buildBehavioralSupplement } from './v1/behavioral.mjs';
function arg(name, fallback=null){const at=process.argv.indexOf(name);return at>=0?process.argv[at+1]??fallback:fallback;}
const sourceRoot=resolve(arg('--source-root','site'));
const baseSnapshotRoot=resolve(arg('--base-snapshot-root'));
const outputRoot=resolve(arg('--output'));
const sourceSha=arg('--source-sha','ef7aa62e45c60f7a12da6160f490719c0721ec03');
const result=buildBehavioralSupplement({sourceRoot,baseSnapshotRoot,outputRoot,sourceSha,requestedStatus:arg('--status')});
process.stdout.write(`${JSON.stringify({root:result.root,status:result.receipt.final_status,plans:result.records.plans.length})}\n`);
