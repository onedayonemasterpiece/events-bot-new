export type InfoPartner = {
  id: string;
  name: string;
  shortName: string;
  description: string;
  href: string;
  accent: string;
  logoText: string;
};

export const INFO_PARTNERS: InfoPartner[] = [
  {
    id: 'kppk-rzd',
    name: 'АО «Калининградская пригородная пассажирская компания»',
    shortName: 'КППК',
    description: 'Региональный железнодорожный перевозчик пригородных маршрутов Калининградской области.',
    href: 'https://www.kppk39.ru/',
    accent: '#d71920',
    logoText: 'КППК',
  },
  {
    id: 'znanie-russia',
    name: 'Российское общество «Знание»',
    shortName: 'Знание',
    description: 'Просветительская организация и партнёр событий с образовательной программой.',
    href: 'https://znanierussia.ru/',
    accent: '#1769ff',
    logoText: 'Знание',
  },
  {
    id: 'kgd80',
    name: 'Фестиваль «80 историй о главном»',
    shortName: '80 историй',
    description: 'Просветительский фестиваль к 80-летию Калининградской области.',
    href: 'https://kgd80.ru/',
    accent: '#8d3b20',
    logoText: '80',
  },
  {
    id: 'kantata-education',
    name: 'Фестиваль «Кантата» · образовательная программа',
    shortName: 'Кантата',
    description: 'Бесплатные лекции и кинопоказы образовательной программы международного фестиваля классической музыки.',
    href: 'https://kantatafest.ru/obrazovatelnaya-programma',
    accent: '#5236a7',
    logoText: 'Кантата',
  },
];
