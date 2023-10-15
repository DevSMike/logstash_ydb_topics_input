package org.logstashplugins.input;

import co.elastic.logstash.api.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstashplugins.YdbTopicsInput;
import org.logstashplugins.util.AsyncReaderCreator;
import org.logstashplugins.util.ConsumerData;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.SupportedCodecs;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.*;
import tech.ydb.topic.write.AsyncWriter;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.QueueOverflowException;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.Mockito.*;

import java.util.concurrent.CompletableFuture;


public class YdbTopicsInputTest {


    private static final String CONNECTION_STRING = "grpc://localhost:2136?database=/local";

    private static final String TOPIC_PATH = "my-topic";

    private YdbTopicsInput input;
    private GrpcTransport transport;

    private TopicClient client;

    private AsyncReader reader;


    @After
    public void tearDown() {
        try {
            transport.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
//        input.stop();
        reader.shutdown();

    }

    @Test
    public void testStart() throws InterruptedException {

        try {
            transport = GrpcTransport.forConnectionString(CONNECTION_STRING)
                    .build();
            client = TopicClient.newClient(transport).build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize YDB TopicClient", e);
        }


        ConsumerData consumerData = new ConsumerData("topicConsumer", SupportedCodecs.newBuilder()
                .addCodec(Codec.RAW)
                .addCodec(Codec.GZIP)
                .build());

        client.createTopic(TOPIC_PATH, CreateTopicSettings.newBuilder()
                .addConsumer(consumerData.getConsumer())
                .setSupportedCodecs(SupportedCodecs.newBuilder()
                        .addCodec(Codec.RAW)
                        .addCodec(Codec.GZIP)
                        .build())
                .setPartitioningSettings(PartitioningSettings.newBuilder()
                        .setMinActivePartitions(3)
                        .build())
                .build());

        String producerAndGroupID = "group-id";
        WriterSettings settings = WriterSettings.newBuilder()
                .setTopicPath(TOPIC_PATH)
                .setProducerId(producerAndGroupID)
                .setMessageGroupId(producerAndGroupID)
                .build();

        AsyncWriter writer = client.createAsyncWriter(settings);
        writer.init()
                .thenRun(() -> System.out.println("Writer started good"))
                .exceptionally(ex -> {
                    System.out.println("Writer started error" + ex.getMessage());
                    return null;
                });

        try {
            writer.send(Message.of("message from writer".getBytes()));
        } catch (QueueOverflowException exception) {
            System.out.println(exception.getMessage());
        }


//        Map<String, Object> configValues = new HashMap<>();
//
//        configValues.put(YdbTopicsInput.PREFIX_CONFIG.name(), "message");
//        configValues.put(YdbTopicsInput.EVENT_COUNT_CONFIG.name(), 1L);
//        configValues.put("topic_path", TOPIC_PATH);
//        configValues.put("connection_string", CONNECTION_STRING);
//
//        Configuration config = new ConfigurationImpl(configValues);
//
//        input = new YdbTopicsInput("test-input", config, null);
//        input.setConsumer(consumerData);


        Map<String, Object> resultMap = new HashMap<>();
        Consumer<Map<String, Object>> consumer = stringObjectMap -> {
            for (String key : stringObjectMap.keySet()) {
                resultMap.put(key, stringObjectMap.get(key));
            }
        };

         reader = AsyncReaderCreator.createAsyncReader(consumer, TOPIC_PATH, client, consumerData);
    //    input.start(consumer);

        reader.init()
                .thenRun(() -> {
                    System.out.println("Async reader initialization finished successfully");
                })
                .exceptionally(ex -> {
                    System.out.println("Async reader initialization failed with exception: " + ex.getMessage());

                    return null;
                });



        System.out.println(resultMap);
    }
}
