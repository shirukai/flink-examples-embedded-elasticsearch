package flink.embedded.elasticsearch.examples;

import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.File;
import java.io.IOException;

/**
 * 使用第三方依赖实现内置ES集群
 * <p>Following Github repository</p>
 * <a href="https://github.com/allegro/embedded-elasticsearch">
 * https://github.com/allegro/embedded-elasticsearch</a>
 *
 * @author shirukai
 */
public class EmbeddedEsQuickStartExample {
    public static void main(String[] args) throws IOException, InterruptedException {
        EmbeddedElastic esCluster = EmbeddedElastic.builder()
                .withElasticVersion("5.6.16")
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 19300)
                .withSetting(PopularProperties.CLUSTER_NAME, "test-es")
                .withSetting("http.cors.enabled", true)
                .withSetting("http.cors.allow-origin", "*")
                // 安装包下载路径
                .withDownloadDirectory(new File("data"))
                .build();
        esCluster.start();
        Thread.currentThread().join();
    }
}
