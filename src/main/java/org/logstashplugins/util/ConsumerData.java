package org.logstashplugins.util;

import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.Consumer;
import tech.ydb.topic.description.SupportedCodecs;


public class ConsumerData {

    private final Consumer consumer;

    public ConsumerData(String name, SupportedCodecs codecs) {
        consumer = Consumer.newBuilder()
                .setName(name)
                .setSupportedCodecs(codecs)
                .build();
    }

    public static ConsumerData createDefaultConsumer() {
        return new ConsumerData("DefaultConsumer", SupportedCodecs.newBuilder()
                .addCodec(Codec.RAW)
                .addCodec(Codec.GZIP)
                .build());
    }
    public String getConsumerName() {
        return consumer.getName();
    }

    public Consumer getConsumer() {
        return consumer;
    }
}
