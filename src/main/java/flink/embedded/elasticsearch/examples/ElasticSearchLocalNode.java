package flink.embedded.elasticsearch.examples;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.transport.Netty4Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;

/**
 * 继承org.elasticsearch.node.Node实现一个本地节点
 *
 * @author shirukai
 */
public class ElasticSearchLocalNode extends Node {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchLocalNode.class);
    private final String tempDir;

    private ElasticSearchLocalNode(Settings preparedSettings, String tempDir) {
        super(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null), Collections.singletonList(Netty4Plugin.class));
        this.tempDir = tempDir;
    }

    public static class Builder {
        private final Settings.Builder builder;
        private String tempDir;

        public Builder() {
            builder = Settings.builder();
            builder.put("network.host", "0.0.0.0");
            builder.put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME);
            builder.put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME);
            builder.put("http.enabled", true);
        }


        public ElasticSearchLocalNode.Builder put(String key, String value) {
            this.builder.put(key, value);
            return this;
        }

        public ElasticSearchLocalNode.Builder setClusterName(String name) {
            this.builder.put("cluster.name", name);
            return this;
        }

        public ElasticSearchLocalNode.Builder setTcpPort(int port) {
            this.builder.put("transport.tcp.port", port);
            return this;
        }

        public ElasticSearchLocalNode.Builder setTempDir(String tempDir) {
            this.tempDir = tempDir;
            this.builder.put("path.home", tempDir + "/home");
            this.builder.put("path.data", tempDir + "/data");
            return this;
        }

        public ElasticSearchLocalNode.Builder enableHttpCors(boolean enable) {
            this.builder.put("http.cors.enabled", enable);
            if (enable) {
                this.builder.put("http.cors.allow-origin", "*");
            }
            return this;
        }

        public ElasticSearchLocalNode build() {
            return new ElasticSearchLocalNode(this.builder.build(), tempDir);
        }
    }

    public void stop() {
        this.stop(false);
    }

    public void stop(boolean cleanDataDir) {

        if (cleanDataDir && tempDir != null) {
            try {
                this.close();
                Files.walk(new File(tempDir).toPath())
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                LOG.error("Failed to stop elasticsearch local node.", e);
            }
        }


    }
}
