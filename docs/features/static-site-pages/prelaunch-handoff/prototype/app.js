const states=['sealed','dim','revealed'];
const root=document.querySelector('#mosaic');
for(let index=0;index<72;index+=1){const tile=document.createElement('span');tile.className='tile';tile.dataset.state=states[(index*5+Math.floor(index/8)*3)%3];tile.style.setProperty('--speed',`${4.5+(index%7)*.45}s`);root.append(tile)}
const motion=matchMedia('(prefers-reduced-motion: reduce)');
let previous=new Set();
function cycle(){if(motion.matches||document.hidden)return;const tiles=[...root.children].map((tile,index)=>({tile,index})).filter(({index})=>!previous.has(index)).sort(()=>Math.random()-.5).slice(0,2+Math.floor(Math.random()*4));previous=new Set(tiles.map(({index})=>index));for(const {tile} of tiles){const choices=states.filter(state=>state!==tile.dataset.state);tile.dataset.state=choices[Math.floor(Math.random()*choices.length)]}}
setInterval(cycle,2600);
