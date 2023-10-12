package org.logstashplugins.util;

import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;

public class AsyncReaderCreator {

    public static AsyncReader createAsyncReader(Consumer<Map<String, Object>> consumer, String topicPath, TopicClient topicClient) {

        ReaderSettings settings = ReaderSettings.newBuilder()
                .setConsumerName("my-consumer")
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(topicPath)
                        .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                        .setMaxLag(Duration.ofMinutes(30))
                        .build())
                .build();

        ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
                .setEventHandler(new MessageHandler(consumer))
                .build();

        return topicClient.createAsyncReader(settings, handlerSettings);
    }
}
