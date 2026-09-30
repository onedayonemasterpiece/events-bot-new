import assert from 'node:assert/strict';
import test from 'node:test';
import {readFile} from 'node:fs/promises';
import {createLiveEventSearchController} from '../src/lib/liveEventSearch.js';

function harness(socketUrl='/api/live-search/live_test/socket') {
 let options,startOptions,normalized;
 let sessionId=null;
 const calls=[];
 const controller=createLiveEventSearchController({
  endpoint:'https://api.example.test/api/live-search',
  getAccessToken:async()=> 'fixture-author',
  fetchImpl:async(url,init)=>{calls.push({url,init});return {ok:true,json:async()=>({session_id:'live_test',socket_url:socketUrl})};},
  clientFactory: opts=>{options=opts;return {
   get sessionId(){return sessionId;},get starting(){return false;},get microphoneEnabled(){return false;},
   async start(args){startOptions=args;normalized=await opts.request(args.url,{method:'POST',body:'{}'});sessionId='live_test';},
   async input(){},stop(){sessionId=null;},disableMicrophone(){},async enableMicrophone(){return true;}
  };}
 });
 return {controller,calls,get options(){return options;},get startOptions(){return startOptions;},get normalized(){return normalized;}};
}

test('browser WSS targets the authenticated API origin, never the static page origin',async()=>{
 const h=harness();await h.controller.search('джаз');
 assert.equal(h.options.transport,'wss');
 assert.equal(h.startOptions.captureDuringStart,false);
 assert.equal(h.normalized.socket_url,'https://api.example.test/api/live-search/live_test/socket');
 assert.equal(new Headers(h.calls[0].init.headers).get('Authorization'),'Bearer fixture-author');
 h.controller.stop();
});

test('foreign or credential-bearing socket URL fails before a connection',async()=>{
 for(const url of ['https://another.invalid/socket','/socket?ticket=leak','https://user:secret@api.example.test/socket']){
  const h=harness(url);
  await assert.rejects(h.controller.search('джаз'),error=>error.code==='LIVE_SOCKET_ORIGIN');
  assert.equal(h.calls.length,1);h.controller.stop();
 }
});

test('Python and browser lock the same semantic shared transport release',async()=>{
 const pkg=JSON.parse(await readFile(new URL('../package.json',import.meta.url)));
 const lock=JSON.parse(await readFile(new URL('../package-lock.json',import.meta.url)));
 const installed=JSON.parse(await readFile(new URL('../node_modules/@onedayonemasterpiece/live-interaction/package.json',import.meta.url)));
 const requirements=await readFile(new URL('../../requirements.txt',import.meta.url),'utf8');
 const dependency=pkg.dependencies['@onedayonemasterpiece/live-interaction'];
 assert.equal(pkg.liveFramework.protocol,'wl-live-v1');
 assert.equal(installed.version,pkg.liveFramework.version);
 assert.ok(requirements.includes(`${dependency}#sha256=${pkg.liveFramework.archive_sha256}`));
 const entry=lock.packages['node_modules/@onedayonemasterpiece/live-interaction'];
 assert.equal(entry.version,pkg.liveFramework.version);
 assert.ok(entry.integrity?.startsWith('sha512-'));
 assert.equal(entry.resolved,dependency);
});
