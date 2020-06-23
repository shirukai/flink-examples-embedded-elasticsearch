package flink.embedded.elasticsearch.examples;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.NodeValidationException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;


/**
 * 写es单元测试
 *
 * @author shirukai
 */
public class WriteElasticSearchJobTest {
    /**
     * 设置绑定端口
     */
    private static final Integer ES_BIND_PORT = 19300;
    /**
     * 设置集群名称
     */
    private static final String ES_CLUSTER_NAME = "test-es";
    /**
     * 内置es节点实例
     */
    private static ElasticSearchLocalNode esNode;

    /**
     * 创建Flink mini cluster，当涉及多个flink任务时，可以避免创建多次集群。
     * 同时可以通过mini cluster 自定义task数量以及slot的数量。
     */
    @ClassRule
    public static MiniClusterWithClientResource flinkMiniCluster =
            new MiniClusterWithClientResource(new MiniClusterResourceConfiguration
                    .Builder()
                    .setConfiguration(new Configuration())
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(1)
                    .build());


    @Test
    public void writeElasticSearchJobTest() throws InterruptedException {
        String[] args = new String[]{
                "--es.cluster.nodes",
                "127.0.0.1",
                "-es.cluster.port",
                ES_BIND_PORT.toString(),
                "-es.cluster.name",
                ES_CLUSTER_NAME
        };
        // 提交flink任务
        WriteElasticSearchJob.main(args);

        Client esClient = esNode.client();
        Thread.sleep(1000);
        SearchResponse response = esClient.prepareSearch("events").execute().actionGet();
        System.out.println(response.toString());
    }

    /**
     * 测试类执行前，创建es节点实例
     *
     * @throws NodeValidationException e
     */
    @BeforeClass
    public static void prepare() throws NodeValidationException {
        esNode = new ElasticSearchLocalNode.Builder()
                .setClusterName(ES_CLUSTER_NAME)
                .setTcpPort(ES_BIND_PORT)
                .setTempDir("data/es")
                .build();
        esNode.start();
    }

    /**
     * 测试类执行后，关闭es节点实例
     */
    @AfterClass
    public static void shutdown() {
        if (esNode != null) {
            esNode.stop(true);
        }
    }
}
