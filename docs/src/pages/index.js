import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import styles from './index.module.css';

// 修改顶部Banner组件
function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      {/* 背景图容器 */}
      <div className={styles.backgroundWrapper}>
        <img
          src="/img/vcg_VCG211151450126_RF.jpg"
          alt="技术背景（已授权）"
          className={styles.backgroundImage}
          loading="lazy"
        />
        <div className={styles.backgroundOverlay} />
      </div>

      {/* 内容容器 */}
      <div className={clsx('container', styles.contentContainer)}>
        <Heading as="h1" className={styles.heroTitle}>
          MaxCompute Java SDK
        </Heading>
        <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
      </div>
    </header>
  );
}

function Features() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          <div className={clsx('col col--4', styles.feature)}>
            <h3>全功能覆盖</h3>
            <p>完整支持 ODPS 数据操作、资源管理、作业调度等核心 API</p>
          </div>
          <div className={clsx('col col--4', styles.feature)}>
            <h3>安全可靠</h3>
            <p>支持 AccessKey、STS 等多种阿里云认证方式</p>
          </div>
          <div className={clsx('col col--4', styles.feature)}>
            <h3>丰富示例</h3>
            <p>提供大量场景化代码示例，涵盖从SQL查询到数据处理</p>
          </div>
        </div>
      </div>
    </section>
  );
}

function QuickStart() {
  return (
    <section className={styles.quickStart}>
      <div className="container">
        <h2>快速开始</h2>
        <div className="row">
          <div className="col col--6">
            <h3>Maven 安装</h3>
            <pre>
              <code>
                {`<dependency>
  <groupId>com.aliyun.odps</groupId>
  <artifactId>odps-sdk-core</artifactId>
  <version>最新版本</version>
</dependency>`}
              </code>
            </pre>
          </div>
          <div className="col col--6">
            <h3>基础使用</h3>
            <pre>
              <code>
                {`// 初始化客户端
Account account = new AliyunAccount(accessId, accessKey);
Odps odps = new Odps(account);
odps.setEndpoint(SAMPLE_ENDPOINT);
odps.setDefaultProject(SAMPLE_PROJECT);`}
              </code>
            </pre>
          </div>
        </div>
        <div className={styles.buttons}>
          <Link
            className="button button--primary button--lg"
            to="/intro">
            查看完整文档 →
          </Link>
        </div>
      </div>
    </section>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="ODPS SDK for Java 文档"
      description="阿里云 MaxCompute ODPS Java SDK 的完整开发文档，包含安装指南、API 参考和最佳实践">
      <HomepageHeader />
      <main>
        <Features />
        <QuickStart />
      </main>
    </Layout>
  );
}