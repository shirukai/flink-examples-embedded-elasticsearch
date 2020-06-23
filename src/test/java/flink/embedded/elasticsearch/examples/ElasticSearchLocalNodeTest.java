package flink.embedded.elasticsearch.examples;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.node.NodeValidationException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * 对ElasticSearchLocalNode进行单元测试
 *
 * @author shirukai
 */
public class ElasticSearchLocalNodeTest {
    private static ElasticSearchLocalNode esNode;

    /**
     * 单元测试之前，创建es节点实例，绑定端口19300
     */
    @BeforeClass
    public static void prepare() throws NodeValidationException {
        esNode = new ElasticSearchLocalNode.Builder()
                .setClusterName("test-es")
                .setTcpPort(19300)
                .setTempDir("data/es")
                .build();
        esNode.start();

    }

    @Test
    public void addIndexTest() {
        People people = new People("xiaoming", 19, 15558800456L);
        esNode.client().prepareIndex()
                .setIndex("people")
                .setType("man")
                .setId("1")
                .setSource(people.toString())
                .get();
    }

    @Test
    public void getIndexTest() throws InterruptedException {
        Thread.sleep(1000);
        SearchResponse response = esNode.client().prepareSearch("people").execute().actionGet();
        System.out.println(response.toString());
    }

    /**
     * 单元测试之后，停止es节点
     */
    @AfterClass
    public static void shutdown() {
        if (null != esNode) {
            esNode.stop(true);
        }
    }

    public static class People {
        private String name;
        private int age;
        private long phone;

        public People(String name, int age, long phone) {
            this.name = name;
            this.age = age;
            this.phone = phone;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public long getPhone() {
            return phone;
        }

        public void setPhone(long phone) {
            this.phone = phone;
        }

        @Override
        public String toString() {
            return "{" +
                    "\"name\":\"" + name + "\"" +
                    ", \"age\":\"" + age + "\"" +
                    ", \"phone\":\"" + phone + "\"" +
                    "}";
        }
    }
}