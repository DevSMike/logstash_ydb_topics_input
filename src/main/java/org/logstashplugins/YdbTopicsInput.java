package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import org.logstashplugins.util.MessageHandler;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * @author Mikhail Lukashev
 */
@LogstashPlugin(name = "ydb_topics_input")
public class YdbTopicsInput implements Input {

    public static final PluginConfigSpec<Long> EVENT_COUNT_CONFIG =
            PluginConfigSpec.numSetting("count", 3);

    public static final PluginConfigSpec<String> PREFIX_CONFIG =
            PluginConfigSpec.stringSetting("prefix", "message");

    private final String topicPath;
    private final String connectionString;

    private final String id;
    private final CountDownLatch done = new CountDownLatch(1);
    private final String consumerName;
    private TopicClient topicClient;
    private AsyncReader reader;
    private GrpcTransport transport;

    public YdbTopicsInput(String id, Configuration config, Context context) {
        this.id = id;
        topicPath = config.get(PluginConfigSpec.stringSetting("topic_path"));
        connectionString = config.get(PluginConfigSpec.stringSetting("connection_string"));
        consumerName = config.get(PluginConfigSpec.stringSetting("consumer_name"));
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        initialize();

        ReaderSettings settings = ReaderSettings.newBuilder()
                .setConsumerName(consumerName)
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(topicPath)
                        .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                        .setMaxLag(Duration.ofMinutes(30))
                        .build())
                .build();

        ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
                .setEventHandler(new MessageHandler(consumer))
                .build();

        reader = topicClient.createAsyncReader(settings, handlerSettings);
        reader.init().join();
    }

    private void initialize() {
        try {
            transport = GrpcTransport.forConnectionString(connectionString)
                    .build();
            topicClient = TopicClient.newClient(transport).build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize YDB TopicClient", e);
        }
    }

    @Override
    public void stop() {
        reader.shutdown();
        closeTransport();
        done.countDown();
    }

    private void closeTransport() {
        transport.close();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Arrays.asList(EVENT_COUNT_CONFIG, PREFIX_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }
}