// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'MaxCompute SDK',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  url: 'https://aliyun.github.io',
  baseUrl: '/aliyun-odps-java-sdk/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'aliyun', // Usually your GitHub org/user name.
  projectName: 'aliyun-odps-java-sdk', // Usually your repo name.
  trailingSlash: true,

  onBrokenAnchors: 'ignore',
  onBrokenLinks: 'ignore',
  onBrokenMarkdownLinks: 'throw',

  markdown: {
      mermaid: true,
  },
  themes: [
      '@docusaurus/theme-mermaid',
      // ... Your other themes.
      [
        require.resolve("@easyops-cn/docusaurus-search-local"),
        /** @type {import("@easyops-cn/docusaurus-search-local").PluginOptions} */
        ({
          hashed: false,
          language: ["en"],
          docsRouteBasePath: "/",
          indexBlog: false,
          highlightSearchTermsOnTargetPage: true,
          searchResultLimits: 10,
          ignoreCssSelectors: ["pre"],
        }),
      ],
  ],

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'zh-Hans',
    locales: ['zh-Hans'],
  },

  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          routeBasePath: '/', // Serve the docs at the site's root
          sidebarPath: './sidebars.js',
          exclude: ['**/superpowers/**'],
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      },
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      docs: {
        sidebar: {
          hideable: true,
          autoCollapseCategories: true,
        },
      },
      image: 'img/logo.svg',
      navbar: {
        title: 'MaxCompute SDK',
        logo: {
          alt: 'MaxCompute Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docs',
            position: 'left',
            label: '文档',
          },
          {
            href: 'https://github.com/aliyun/aliyun-odps-jdbc',
            position: 'right',
            label: '使用 JDBC 链接 MaxCompute',
          },
//          {
//            type: 'docsVersionDropdown',
//            sidebarId: 'version',
//            position: 'left',
//            dropdownActiveClassDisabled: true,
//          },
          {
            href: 'https://github.com/aliyun/aliyun-odps-java-sdk',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['java', 'python', 'go'],
      },
      colorMode: {
        defaultMode: 'light',
        respectPrefersColorScheme: true,
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'SDK',
            items: [
              { label: 'Java SDK', href: 'https://github.com/aliyun/aliyun-odps-java-sdk' },
              { label: 'Python SDK', href: 'https://github.com/aliyun/aliyun-odps-python-sdk' },
              { label: 'Go SDK', href: 'https://github.com/aliyun/aliyun-odps-go-sdk' },
            ],
          },
          {
            title: 'MaxCompute',
            items: [
              { label: 'MaxCompute 官方文档', href: 'https://help.aliyun.com/zh/maxcompute/' },
              { label: 'JDBC Driver', href: 'https://github.com/aliyun/aliyun-odps-jdbc' },
            ],
          },
          {
            title: '更多',
            items: [
              { label: '更新日志', to: '/changelog' },
              { label: '常见问题', to: '/faq' },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Alibaba Cloud. Built with Docusaurus.`,
      },
    }),
};

export default config;

