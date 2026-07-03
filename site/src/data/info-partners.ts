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
  gridColumnStart?: number;
  gridColumnSpan?: number;
  gridRowStart?: number;
  gridRowSpan?: number;
  mobileColumnStart?: number;
  mobileColumnSpan?: number;
  mobileRowStart?: number;
  mobileRowSpan?: number;
};

export const INFO_PARTNERS: InfoPartner[] = [
  {
    id: 'kppk-rzd',
    name: 'Акционерное общество «Калининградская пригородная пассажирская компания»',
    shortName: 'АО «КППК»',
    description: 'Акционерное общество «Калининградская пригородная пассажирская компания».',
    compactLabel: 'АО «КППК»',
    href: 'https://www.kppk39.ru/',
    accent: '#d71920',
    logoText: 'АО «КППК»',
    logoUrl: '/assets/partners/kppk-rzd-red.svg',
    logoAlt: 'Логотип РЖД на сайте АО «КППК»',
    logoShape: 'compact',
    logoSurface: 'transparent',
    gridColumnStart: 7,
    gridColumnSpan: 2,
    gridRowStart: 1,
    mobileColumnStart: 3,
    mobileColumnSpan: 2,
    mobileRowStart: 3,
  },
  {
    id: 'znanie-russia',
    name: 'Российское общество «Знание»',
    shortName: 'Знание',
    description: 'Российское общество «Знание».',
    compactLabel: '',
    href: 'https://znanierussia.ru/',
    accent: '#2b2522',
    logoText: 'Знание',
    logoUrl: '/assets/partners/znanie-russia.svg',
    logoAlt: 'Логотип Российского общества «Знание»',
    logoShape: 'wide',
    logoSurface: 'light',
    priority: 'primary',
    gridColumnStart: 3,
    gridColumnSpan: 4,
    gridRowStart: 1,
    mobileColumnStart: 3,
    mobileColumnSpan: 2,
    mobileRowStart: 1,
  },
  {
    id: 'kgd80',
    name: 'Фестиваль «80 историй о главном»',
    shortName: '80 историй',
    description: 'Просветительский фестиваль к 80-летию Калининградской области.',
    compactLabel: 'Просветительский фестиваль к 80-летию Калининградской области',
    href: 'https://kgd80.ru/',
    accent: '#df452d',
    logoText: '80',
    logoUrl: '/assets/partners/kgd80.svg',
    logoAlt: 'Вертикальный логотип фестиваля «80 историй о главном»',
    logoShape: 'tall',
    logoSurface: 'light',
    priority: 'primary',
    gridColumnStart: 1,
    gridColumnSpan: 2,
    gridRowStart: 1,
    gridRowSpan: 2,
    mobileColumnStart: 1,
    mobileColumnSpan: 2,
    mobileRowStart: 1,
    mobileRowSpan: 2,
  },
  {
    id: 'kantata-education',
    name: 'Фестиваль «Кантата» · образовательная программа',
    shortName: 'Кантата',
    description: 'Образовательная программа фестиваля.',
    compactLabel: 'Образовательная программа фестиваля',
    href: 'https://kantatafest.ru/obrazovatelnaya-programma',
    accent: '#251a13',
    logoText: 'Кантата',
    logoUrl: '/assets/partners/kantata-education.webp',
    logoFallbackUrl: '/assets/partners/kantata-education.png',
    logoAlt: 'Логотип фестиваля «Кантата» в начертании слова «КАНТАТА»',
    logoShape: 'wide',
    logoSurface: 'light',
    priority: 'primary',
    gridColumnStart: 3,
    gridColumnSpan: 3,
    gridRowStart: 2,
    mobileColumnStart: 1,
    mobileColumnSpan: 2,
    mobileRowStart: 3,
  },
  {
    id: 'act-opus',
    name: 'Театр «Акт Опус»',
    shortName: 'Акт Опус',
    description: 'Театр «Акт Опус».',
    compactLabel: '',
    href: 'https://actop.us/plays',
    accent: '#111111',
    logoText: 'Акт Опус',
    logoUrl: '/assets/partners/act-opus.webp',
    logoFallbackUrl: '/assets/partners/act-opus.png',
    logoAlt: 'Логотип театра «Акт Опус»',
    logoShape: 'wide',
    logoSurface: 'light',
    priority: 'primary',
    gridColumnStart: 6,
    gridColumnSpan: 3,
    gridRowStart: 2,
    mobileColumnStart: 3,
    mobileColumnSpan: 2,
    mobileRowStart: 2,
  },
];
