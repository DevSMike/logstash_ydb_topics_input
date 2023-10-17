package org.logstashplugins.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;

import java.util.Collections;
import java.util.function.Consumer;

import java.util.Map;

public class MessageHandler extends AbstractReadEventHandler {
    private final Logger logger = LoggerFactory.getLogger(MessageHandler.class);
    private final Consumer<Map<String, Object>> consumer;

    public MessageHandler(Consumer<Map<String, Object>> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onMessages(DataReceivedEvent event) {
        for (Message message : event.getMessages()) {
            logger.info("Message received. SeqNo={}, offset={}", message.getSeqNo(), message.getOffset());

            Map<String, Object> logstashEvent = Collections.singletonMap("message", new String(message.getData()));

            consumer.accept(logstashEvent);

            message.commit().join();
        }
    }
}
