import test from 'node:test';import assert from 'node:assert/strict';import{readFileSync}from'node:fs';
const source=readFileSync(new URL('../src/components/AuthorizedEventSearch.astro',import.meta.url),'utf8');
const fn=source.slice(source.indexOf('    function renderAuthState('),source.indexOf('    function normalizeSearchInput('));
function harness(){let message='Войдите, чтобы включить поиск.';const classes=new Set();const form={hidden:false},userBadge={hidden:true};
 const render=new Function('root','login','logout','form','userBadge','userName','accountMenu','renderAvatar','displayUserName','setStatus','window','input','loading',`${fn};return renderAuthState;`)(
 {classList:{contains:n=>classes.has(n),toggle:(n,on)=>on?classes.add(n):classes.delete(n)}},{},{},form,userBadge,{}, {},()=>{},()=>'',value=>message=value,{location:{search:''}},{value:''},false);
 return{render,message:()=>message,setMessage:value=>message=value,userBadge};
}
test('signed-in status becomes ready synchronously without waiting for quota RPC',()=>{const h=harness();h.render({user:{id:'owner'}});assert.match(h.message(),/Поиск подключён/);assert.equal(h.userBadge.hidden,false);});
test('repeated same signed-in snapshot does not erase search outcome; signout remains honest',()=>{const h=harness();h.render({user:{id:'owner'}});h.setMessage('Найдено 3 события');h.render({user:{id:'owner'}});assert.equal(h.message(),'Найдено 3 события');h.render(null);assert.match(h.message(),/Войдите/);});
