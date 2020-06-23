package flink.embedded.elasticsearch.examples;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.transport.Netty4Plugin;

import java.util.Collections;

/**
 * Es本地节点快速入门示例
 *
 * @author shirukai
 */
public class LocalNodeQuickStartExample {
    private static class PluginNode extends Node {
        public PluginNode(Settings settings) {
            super(InternalSettingsPreparer.prepareEnvironment(settings, null), Collections.singletonList(Netty4Plugin.class));
        }
    }

    public static void main(String[] args) throws NodeValidationException, InterruptedException {
        String systemTempDir = System.getProperty("java.io.tmpdir");
        String esTempDir = systemTempDir + "/es";
        Settings settings = Settings.builder()
                .put("cluster.name", "test")
                .put("http.enabled", true)
                .put("path.home", systemTempDir)
                .put("path.data", esTempDir)
                .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                .build();
        PluginNode node = new PluginNode(settings);
        node.start();
        // 让它一直阻塞吧
        Thread.currentThread().join();
    }
}
