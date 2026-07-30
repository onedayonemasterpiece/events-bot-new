export interface HomeHeroTalkEditorialFragment {
  text: string;
  link?: boolean;
  href?: string;
  accent?: boolean;
  breakAfter?: boolean;
}

export interface HomeHeroTalkEditorial {
  id: string;
  eventId?: number;
  fragments: HomeHeroTalkEditorialFragment[];
}

/**
 * Curated launch bank. Every phrase is grounded in the exported event row and
 * is intentionally stored as semantic fragments: only the marked words link.
 * A Smart Update/LLM authoring pipeline is a follow-up, not browser runtime.
 */
export const HOME_HERO_TALK_EDITORIAL: HomeHeroTalkEditorial[] = [
  { id:'greeting-day', fragments:[
    { text:'Добрый день!', accent:true, breakAfter:true }, { text:'Что сегодня' }, { text:'вас удивит?', href:'/segodnya/' },
  ] },
  { id:'local-keska', fragments:[
    { text:'Мы говорим', breakAfter:true }, { text:'по-калининградски.', accent:true, breakAfter:true },
    { text:'И скажем' }, { text:'«кеска».', href:'/segodnya/' },
  ] },
  { id:'city-does-not-wait', fragments:[
    { text:'Город' }, { text:'не ждёт.', accent:true, breakAfter:true }, { text:'Что удивит' }, { text:'сегодня?', href:'/segodnya/' },
  ] },
  { id:'ask-with-child', fragments:[
    { text:'Можно просто спросить:', breakAfter:true }, { text:'«Куда с ребёнком?»', href:'/poisk/', accent:true },
  ] },
  { id:'sea-inside', eventId:4211, fragments:[
    { text:'Море', accent:true }, { text:'внутри.', link:true, accent:true, breakAfter:true }, { text:'И прямо на Променаде.' },
  ] },
  { id:'still-life-passions', eventId:5459, fragments:[
    { text:'Натюрморт.', breakAfter:true }, { text:'Но со', accent:true }, { text:'страстями.', link:true, accent:true },
  ] },
  { id:'expelliarmus-cinema', eventId:7031, fragments:[
    { text:'Кино, магические сладости', breakAfter:true }, { text:'и немного Экспеллиармуса.', link:true, accent:true },
  ] },
  { id:'ramm-live', eventId:7296, fragments:[
    { text:'Rammstein.', link:true, accent:true, breakAfter:true }, { text:'Живым голосом.' }, { text:'В Бастионе.' },
  ] },
  { id:'industrial-memory', eventId:7276, fragments:[
    { text:'Город помнит заводы.', breakAfter:true }, { text:'Поговорим?', link:true, accent:true },
  ] },
  { id:'akmal-svetlogorsk', eventId:7011, fragments:[
    { text:'Акмаль.', link:true, accent:true }, { text:'Летний Светлогорск.' }, { text:'Совпало.' },
  ] },
  { id:'vikings-return', eventId:6191, fragments:[
    { text:'Викинги вернулись.', breakAfter:true }, { text:'В Большой Кауп.', link:true, accent:true },
  ] },
  { id:'pushkin-time-junction', eventId:6652, fragments:[
    { text:'Пушкин.', accent:true }, { text:'Замок.' }, { text:'Стык времён.', link:true, breakAfter:true }, { text:'Всё сходится.' },
  ] },
  { id:'windows-of-time', eventId:6871, fragments:[
    { text:'Окна времени', link:true, accent:true }, { text:'открываются', breakAfter:true }, { text:'на Променаде.' },
  ] },
  { id:'horror-punk-sky', eventId:7133, fragments:[
    { text:'Хоррор-панк.', accent:true }, { text:'Под открытым небом.', link:true },
  ] },
  { id:'hoi-fest', eventId:4962, fragments:[
    { text:'Хой.', link:true, accent:true }, { text:'Фест.' }, { text:'Бастион.' }, { text:'Громко.' },
  ] },
  { id:'promenade-art', eventId:6983, fragments:[
    { text:'Променад —', breakAfter:true }, { text:'теперь выставка.', link:true, accent:true },
  ] },
  { id:'summer-cutter', eventId:4961, fragments:[
    { text:'Балтийский', accent:true }, { text:'леторуб.', link:true, accent:true, breakAfter:true }, { text:'Да, так и называется.' },
  ] },
  { id:'giant-mosaic', eventId:7102, fragments:[
    { text:'Одну мозаику.', breakAfter:true }, { text:'Зато гигантскую.', link:true, accent:true }, { text:'Вместе.' },
  ] },
  { id:'breakfast-with-greats', eventId:7242, fragments:[
    { text:'Чай.', accent:true }, { text:'Великие художники.' }, { text:'Арт-завтрак.', link:true, breakAfter:true }, { text:'Утро удалось.' },
  ] },
  { id:'songs-about-road', eventId:6936, fragments:[
    { text:'Песни', accent:true }, { text:'о поиске себя', link:true }, { text:'и дальних странах.' },
  ] },
  { id:'organ-two-worlds', eventId:6917, fragments:[
    { text:'Бах встречает Уэббера.', link:true, accent:true, breakAfter:true }, { text:'Орган всё выдержит.' },
  ] },
  { id:'pianissimo-yoon', eventId:5264, fragments:[
    { text:'Тише.', breakAfter:true }, { text:'Pianissimo.', link:true, accent:true }, { text:'За роялем Моён Юн.' },
  ] },
  { id:'grape-festival', eventId:6994, fragments:[
    { text:'Виноделы.', accent:true }, { text:'Фермеры.' }, { text:'ГРОЗДЬ.', link:true, breakAfter:true }, { text:'Такой союз.' },
  ] },
  { id:'crab-stick-day', eventId:6898, fragments:[
    { text:'У крабовой палочки', breakAfter:true }, { text:'тоже есть день.', link:true, accent:true }, { text:'Вот он.' },
  ] },
  { id:'business-universe', eventId:6357, fragments:[
    { text:'Бизнес —', accent:true }, { text:'тоже вселенная.', link:true }, { text:'В Светлогорске.' },
  ] },
  { id:'michelle-classics', eventId:3000, fragments:[
    { text:'Моя Мишель', link:true, accent:true }, { text:'встречает Баха,' }, { text:'Моцарта и Бетховена.' },
  ] },
  { id:'cinema-songs', eventId:6938, fragments:[
    { text:'Кино закончилось.', breakAfter:true }, { text:'Песни остались.', link:true, accent:true },
  ] },
  { id:'miklukho-maclay', eventId:6937, fragments:[
    { text:'Океания.', accent:true }, { text:'Лодки.' }, { text:'Миклухо-Маклай.', link:true }, { text:'По рисункам.' },
  ] },
  { id:'baroque-costumes', eventId:7001, fragments:[
    { text:'Барокко.', link:true, accent:true }, { text:'В костюмах.' }, { text:'Со скрипкой и клавесином.' },
  ] },
  { id:'first-world-war', eventId:7280, fragments:[
    { text:'Личные коллекции', accent:true }, { text:'помнят Первую мировую.', link:true },
  ] },
  { id:'women-standup', eventId:7129, fragments:[
    { text:'Семья.' }, { text:'Дети.' }, { text:'Отношения.', breakAfter:true }, { text:'Женский стендап.', link:true, accent:true },
  ] },
  { id:'sports-day', eventId:7283, fragments:[
    { text:'ГТО и соревнования.', breakAfter:true }, { text:'День физкультурника.', link:true, accent:true }, { text:'Для всех.' },
  ] },
];
