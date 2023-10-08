package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;


@LogstashPlugin(name = "ydb_topics_input")
public class YdbTopicsInput implements Input {

    public static final PluginConfigSpec<Long> EVENT_COUNT_CONFIG =
            PluginConfigSpec.numSetting("count", 3);

    public static final PluginConfigSpec<String> PREFIX_CONFIG =
            PluginConfigSpec.stringSetting("prefix", "message");

    private final String id;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped;
    private TopicClient topicClient;

    private SyncReader reader;


    public void setTopicClient(TopicClient topicClient) {
        this.topicClient = topicClient;
    }

    public void setReader(SyncReader reader) {
        this.reader = reader;
    }

    public YdbTopicsInput(String id, Configuration config, Context context) {
        this.id = id;
        String topicPath = config.get(PluginConfigSpec.stringSetting("topic_path"));
        String connectionString = config.get(PluginConfigSpec.stringSetting("connection_string"));

        //инициализация TopicClient
        try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .build()) {
            topicClient = TopicClient.newClient(transport).build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize YDB TopicClient", e);
        }

        ReaderSettings settings = ReaderSettings.newBuilder()
                .setConsumerName("my-consumer")
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(topicPath)
                        .build())
                .build();

        reader = topicClient.createSyncReader(settings);
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        try {
            while (!stopped) {
                Message message = reader.receive(); // Чтение сообщения из топика
                if (message != null) {
                    Map<String, Object> logstashEvent = Collections.singletonMap("message", message.getData());
                    consumer.accept(logstashEvent);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Error while reading messages from YDB Topic", e);
        } finally {
            stopped = true;
            done.countDown();
        }
    }


    @Override
    public void stop() {
        stopped = true;
        reader.shutdown();
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
