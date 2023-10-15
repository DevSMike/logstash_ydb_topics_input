package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import org.logstashplugins.util.AsyncReaderCreator;
import org.logstashplugins.util.ConsumerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.AsyncReader;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

@LogstashPlugin(name = "ydb_topics_input")
public class YdbTopicsInput implements Input {

    public static final PluginConfigSpec<Long> EVENT_COUNT_CONFIG =
            PluginConfigSpec.numSetting("count", 3);

    public static final PluginConfigSpec<String> PREFIX_CONFIG =
            PluginConfigSpec.stringSetting("prefix", "message");

    private final Logger logger = LoggerFactory.getLogger(YdbTopicsInput.class);

    private final String topicPath;
    private final String connectionString;
    private final String id;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped;
    private TopicClient topicClient;
    private AsyncReader reader;
    private GrpcTransport transport; // Добавляем поле для транспорта
    private ConsumerData consumerData;

    public void setConsumer(ConsumerData consumer) {
        consumerData = consumer;
    }

    public void setTopicClient(TopicClient topicClient) {
        this.topicClient = topicClient;
    }

    public void setReader(AsyncReader reader) {
        this.reader = reader;
    }

    public YdbTopicsInput(String id, Configuration config, Context context) {
        this.id = id;
        topicPath = config.get(PluginConfigSpec.stringSetting("topic_path"));
        connectionString = config.get(PluginConfigSpec.stringSetting("connection_string"));
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        // Открываем транспорт в методе initialize
        initialize();

        reader = AsyncReaderCreator.createAsyncReader(consumer, topicPath, topicClient, consumerData);

        reader.init()
                .thenRun(() -> {
                    logger.info("Async reader initialization finished successfully");
                })
                .exceptionally(ex -> {
                    logger.error("Async reader initialization failed with exception: ", ex);
                    return null;
                });
    }

    // Метод для инициализации транспорта
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
        stopped = true;
        reader.shutdown();
        // Закрываем транспорт в методе stop
        closeTransport();
    }

    // Метод для закрытия транспорта
    private void closeTransport() {
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception e) {
                logger.error("Failed to close the GrpcTransport", e);
            }
        }
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