export type InfoPartner = {
  id: string;
  name: string;
  shortName: string;
  description: string;
  href: string;
  accent: string;
  logoUrl: string;
  logoTone?: 'light' | 'brand';
  logoScale?: 'compact' | 'wide' | 'tall';
};

export const INFO_PARTNERS: InfoPartner[] = [
  {
    id: 'kppk-rzd',
    name: 'АО «Калининградская пригородная пассажирская компания»',
    shortName: 'КППК',
    description: 'Региональный железнодорожный перевозчик пригородных маршрутов Калининградской области.',
    href: 'https://www.kppk39.ru/',
    accent: '#d71920',
    logoUrl: '/assets/partners/kppk-rzd.svg',
    logoTone: 'brand',
    logoScale: 'compact',
  },
  {
    id: 'znanie-russia',
    name: 'Российское общество «Знание»',
    shortName: 'Знание',
    description: 'Просветительская организация и партнёр событий с образовательной программой.',
    href: 'https://znanierussia.ru/',
    accent: '#1769ff',
    logoUrl: '/assets/partners/znanie-russia.svg',
    logoScale: 'wide',
  },
  {
    id: 'kgd80',
    name: 'Фестиваль «80 историй о главном»',
    shortName: '80 историй',
    description: 'Просветительский фестиваль к 80-летию Калининградской области.',
    href: 'https://kgd80.ru/',
    accent: '#8d3b20',
    logoUrl: '/assets/partners/kgd80.svg',
    logoScale: 'wide',
  },
  {
    id: 'kantata-education',
    name: 'Фестиваль «Кантата» · образовательная программа',
    shortName: 'Кантата',
    description: 'Бесплатные лекции и кинопоказы образовательной программы международного фестиваля классической музыки.',
    href: 'https://kantatafest.ru/obrazovatelnaya-programma',
    accent: '#5236a7',
    logoUrl: '/assets/partners/kantata-education.png',
    logoScale: 'wide',
  },
];
