export type InfoPartnerLogoShape = 'square' | 'wide' | 'tall' | 'compact';

export type InfoPartner = {
  id: string;
  name: string;
  shortName: string;
  description: string;
  compactLabel: string;
  href: string;
  accent: string;
  logoText: string;
  logoUrl?: string;
  logoFallbackUrl?: string;
  logoAlt?: string;
  logoShape: InfoPartnerLogoShape;
  logoSurface?: 'light' | 'dark' | 'transparent';
  priority?: 'primary' | 'standard';
};

export const INFO_PARTNERS: InfoPartner[] = [
  {
    id: 'kppk-rzd',
    name: 'АО «Калининградская пригородная пассажирская компания»',
    shortName: 'КППК',
    description: 'Региональный железнодорожный перевозчик пригородных маршрутов Калининградской области.',
    compactLabel: 'Пригородные маршруты',
    href: 'https://www.kppk39.ru/',
    accent: '#d71920',
    logoText: 'КППК',
    logoUrl: '/assets/partners/kppk-rzd.svg',
    logoAlt: 'Логотип КППК / РЖД',
    logoShape: 'compact',
    logoSurface: 'dark',
  },
  {
    id: 'znanie-russia',
    name: 'Российское общество «Знание»',
    shortName: 'Знание',
    description: 'Просветительская организация и партнёр событий с образовательной программой.',
    compactLabel: 'Просветительские события',
    href: 'https://znanierussia.ru/',
    accent: '#2b2522',
    logoText: 'Знание',
    logoUrl: '/assets/partners/znanie-russia.svg',
    logoAlt: 'Логотип Российского общества «Знание»',
    logoShape: 'wide',
    logoSurface: 'light',
    priority: 'primary',
  },
  {
    id: 'kgd80',
    name: 'Фестиваль «80 историй о главном»',
    shortName: '80 историй',
    description: 'Просветительский фестиваль к 80-летию Калининградской области.',
    compactLabel: 'Фестиваль 80-летия',
    href: 'https://kgd80.ru/',
    accent: '#df452d',
    logoText: '80',
    logoUrl: '/assets/partners/kgd80.svg',
    logoAlt: 'Вертикальный логотип фестиваля «80 историй о главном»',
    logoShape: 'tall',
    logoSurface: 'light',
    priority: 'primary',
  },
  {
    id: 'kantata-education',
    name: 'Фестиваль «Кантата» · образовательная программа',
    shortName: 'Кантата',
    description: 'Бесплатные лекции и кинопоказы образовательной программы международного фестиваля классической музыки.',
    compactLabel: 'Лекции и музыка',
    href: 'https://kantatafest.ru/obrazovatelnaya-programma',
    accent: '#251a13',
    logoText: 'Кантата',
    logoUrl: '/assets/partners/kantata-education.webp',
    logoFallbackUrl: '/assets/partners/kantata-education.png',
    logoAlt: 'Логотип фестиваля «Кантата» в начертании слова «КАНТАТА»',
    logoShape: 'wide',
    logoSurface: 'light',
    priority: 'primary',
  },
  {
    id: 'act-opus',
    name: 'Театр «Акт Опус»',
    shortName: 'Акт Опус',
    description: 'Калининградский театр с собственной афишей спектаклей и премьер.',
    compactLabel: 'Театр и премьеры',
    href: 'https://actop.us/plays',
    accent: '#111111',
    logoText: 'Акт Опус',
    logoUrl: '/assets/partners/act-opus.webp',
    logoFallbackUrl: '/assets/partners/act-opus.png',
    logoAlt: 'Логотип театра «Акт Опус»',
    logoShape: 'wide',
    logoSurface: 'light',
    priority: 'primary',
  },
];
