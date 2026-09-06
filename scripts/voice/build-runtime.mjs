// Bundle the existing Search/assistant core for the DevCoveer Node host.
import {createRequire} from 'node:module';
import {resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'../..');
const require=createRequire(root+'/site/package.json');
const {build}=require('esbuild');
const output=process.argv[2];if(!output)throw Error('Usage: node scripts/voice/build-runtime.mjs <output.mjs>');
await build({entryPoints:[root+'/scripts/voice/server.mjs'],outfile:resolve(output),bundle:true,platform:'node',format:'esm',target:'node22',packages:'external',plugins:[{name:'same-pinned-supabase',setup(b){b.onResolve({filter:/^https:\/\/esm\.sh\/@supabase\/supabase-js@2\.108\.2$/},()=>({path:require.resolve('@supabase/supabase-js').replace(/index\.cjs$/, 'index.mjs'),external:true}));}}]});
