// Native Chromium worklet/recorder with a synthetic Web Audio stream.
// Anonymous, no server/ASR/Auth calls; not physical speech or mobile evidence.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { build } from 'esbuild';
import { chromium } from 'playwright';
test('native silence endpoint keeps PCM and final compressed container through stop',async()=>{
 const root=new URL('../',import.meta.url).pathname;
 const bundle=(await build({stdin:{contents:"import { MicrophoneCapture } from './src/lib/assistant/microphoneCapture.ts'; window.Capture=MicrophoneCapture;",resolveDir:root,loader:'ts'},bundle:true,write:false,format:'esm'})).outputFiles[0].text;
 const worklet=await readFile(new URL('../public/voice/pcm-capture-worklet.js',import.meta.url));
 const server=createServer((req,res)=>{
  if(req.url==='/bundle.js'){res.setHeader('Content-Type','text/javascript');res.end(bundle);}
  else if(req.url==='/worklet.js'){res.setHeader('Content-Type','text/javascript');res.end(worklet);}
  else {res.setHeader('Content-Type','text/html');res.end('<!doctype html><button id="start">Start</button><script type="module" src="/bundle.js"></script>');}
 });
 let browser;
 try{
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH?undefined:'chromium',executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
  const page=await browser.newPage();await page.goto(`http://127.0.0.1:${server.address().port}`);await page.waitForFunction(()=>window.Capture);
  await page.evaluate(()=>{
   window.parts=[];window.compressed=[];window.stoppedCount=0;
   document.querySelector('#start').onclick=async()=>{
    const sourceContext=new AudioContext();await sourceContext.resume();
    const destination=sourceContext.createMediaStreamDestination();
    const buffer=sourceContext.createBuffer(1,sourceContext.sampleRate*5,sourceContext.sampleRate),pcm=buffer.getChannelData(0);
    // Initial silence must not stop. A 400 ms tone represents sound evidence,
    // then actual zero samples—not a main-thread wait—provide trailing silence.
    for(let i=Math.round(sourceContext.sampleRate*0.8);i<sourceContext.sampleRate*1.2;i++)pcm[i]=0.03*Math.sin(i*2*Math.PI*440/sourceContext.sampleRate);
    const source=sourceContext.createBufferSource();source.buffer=buffer;source.connect(destination);
    navigator.mediaDevices.getUserMedia=async()=>destination.stream;
    window.capture=new window.Capture({workletUrl:'/worklet.js',budget:{maxWireBytes:1048576,envelopeBytes:8192,encoding:'base64'},onPart:async p=>parts.push(p),onCompressedPart:async p=>compressed.push(p),onStatus:()=>{},onStopped:r=>{window.receipt=r;window.stoppedCount++;void sourceContext.close();}});
    await capture.start();source.start();
   };
  });
  await page.click('#start');await page.waitForFunction(()=>window.capture?.status==='recording');await page.waitForTimeout(600);
  assert.equal(await page.evaluate(()=>capture.status),'recording');
  await page.waitForFunction(()=>window.receipt,{},{timeout:10000});
  const result=await page.evaluate(async()=>{
   const bytes=new Blob(compressed.map(p=>p.bytes),{type:receipt.compressed.mimeType});
   const ac=new AudioContext();const audio=await ac.decodeAudioData(await bytes.arrayBuffer());await ac.close();
   return {receipt,count:stoppedCount,storedFrames:parts.reduce((n,p)=>n+p.frameCount,0),compressedBytes:bytes.size,decodedSeconds:audio.duration};
  });
  assert.equal(result.receipt.reason,'silence');assert.equal(result.receipt.complete,true);assert.equal(result.receipt.captureComplete,true);
  assert.equal(result.receipt.compressed.complete,true);assert.equal(result.count,1);
  assert.equal(result.storedFrames,result.receipt.frames);assert.equal(result.compressedBytes,result.receipt.compressed.bytes);
  const seconds=result.receipt.frames/result.receipt.sampleRate;assert.ok(seconds>=2.65&&seconds<3.5,JSON.stringify(result));
  assert.ok(result.decodedSeconds>=seconds-0.1&&result.decodedSeconds<seconds+0.3,JSON.stringify(result));
  console.log('synthetic endpoint evidence',JSON.stringify(result));
 }finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
});
