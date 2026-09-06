import { mkdirSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
export const specimens=[
 ['/populyarnoe/','Полки · Популярное','Принятый образец: города и полный заголовок полки.'],
 ['/segodnya/','По времени · Сегодня','Мобильная лента и действующий выбор городов.'],
 ['/date-2026-07-23/','Конкретная дата · 23 июля','Тот же механизм на явно указанной исторической дате.'],
 ['/vyhodnye/','Выходные','Две колонки на десктопе, последовательные дни на телефоне.'],
 ['/vystavki/','Каталог · Выставки','Контекст раздела без выдуманного выбора городов.'],
 ['/festivali/','Календарь · Фестивали','Длинный заголовок, месяцы и фестивальные карточки.'],
 ['/sobytiya/tochka-i-liniya-kaliningrad-5370/','Событие · Точка и линия','Заголовок и разделы, существующие действия события.'],
];
if(process.argv[1]===fileURLToPath(import.meta.url)){
 const prefix=process.env.SITE_BASE_PATH;
 if(!/^\/preview-islands-[a-z0-9-]+-archetypes(?:-date-b)?\d+$/.test(prefix||''))throw new Error('Explicit isolated archetype prefix required');
 const routes=process.env.STATIC_SITE_FOCUSED_ROUTES?JSON.parse(process.env.STATIC_SITE_FOCUSED_ROUTES).filter(x=>x.endsWith('/')&&x!=='/__preview/').map(route=>specimens.find(x=>x[0]===route)||[route,route.startsWith('/date-')?'Дата '+route.slice(6,-1):route.startsWith('/vyhodnye/')?'Выходные '+route.split('/')[2]:'Завтра','Действующий выбор дат, общий нижний блок Б.']):specimens;
 const dir=new URL('../dist/__preview/',import.meta.url);mkdirSync(dir,{recursive:true});
 writeFileSync(new URL('index.html',dir),`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>Архетипы · Плавающие острова</title><style>body{margin:0;background:#fbf7ef;color:#241d17;font:16px/1.5 system-ui}main{max-width:820px;margin:48px auto;padding:20px}h1{font-size:32px;line-height:1.15}a{display:block;margin:14px 0;padding:22px;color:inherit;text-decoration:none;border:1px solid white;border-radius:22px;background:#fffcf6;box-shadow:0 12px 26px -10px #37251840}a:focus-visible{outline:3px solid #a54821}strong{display:block}small{display:block;margin-top:6px;color:#786e64}aside{padding:16px;border:1px solid #dccbb8;border-radius:14px}</style><main><h1>Плавающие острова<br>Примеры страниц</h1><p>Откройте страницу и прокрутите вниз и обратно. На телефоне города плавно собираются в «…». На десктопе видимыми остаются пункты, которые помещаются.</p><aside>Это выборочная дизайн-сборка, не полная афиша. Данные — исторический срез на 23 июля 2026. Проверяем интерфейс, не актуальность событий. Остальные маршруты в эту сборку не включены.</aside>${routes.map(([url,title,desc])=>`<a href="${prefix}${url}"><strong>${title} →</strong><small>${desc}</small></a>`).join('')}</main></html>`);
}
