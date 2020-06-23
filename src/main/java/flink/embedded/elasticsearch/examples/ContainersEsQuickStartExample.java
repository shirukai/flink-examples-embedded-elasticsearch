package flink.embedded.elasticsearch.examples;

import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.Collections;

/**
 * TestContainer启动es快速入门示例
 * <a href="https://www.testcontainers.org/modules/elasticsearch/">
 * https://www.testcontainers.org/modules/elasticsearch/</a>
 *
 * @author shirukai
 */
public class ContainersEsQuickStartExample {
    public static void main(String[] args) throws InterruptedException {
        // Create the elasticsearch container.
        try (ElasticsearchContainer container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:5.6.16")) {
            // Disable x-pack
            container.setEnv(Collections.singletonList("xpack.security.enabled=false"));

            // Start the container. This step might take some time...
            container.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(container::close));

            // 让它阻塞一会吧
            Thread.currentThread().join();
        }

    }
}
