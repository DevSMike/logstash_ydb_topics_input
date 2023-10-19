package org.logstashplugins.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.function.Consumer;

import java.util.Map;

public class MessageHandler extends AbstractReadEventHandler {
    private final Logger logger = LoggerFactory.getLogger(MessageHandler.class);
    private final Consumer<Map<String, Object>> consumer;
    private final String schema;

    public MessageHandler(Consumer<Map<String, Object>> consumer, String schema) {
        this.consumer = consumer;
        this.schema = schema;
    }

    @Override
    public void onMessages(DataReceivedEvent event) {
        MessageProcessor messageProcessor;

        if (schema.equals("JSON")) {
            messageProcessor = (message, consumer) -> {
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode jsonNode = objectMapper.readTree(new String(message.getData()));
                    Map<String, Object> logstashEvent = new HashMap<>();
                    jsonNode.fields().forEachRemaining(entry -> logstashEvent.put(entry.getKey(), entry.getValue().asText()));
                    consumer.accept(logstashEvent);
                    message.commit().join();
                } catch (IOException e) {
                    logger.error("Error parsing JSON: {}", e.getMessage());
                }
            };
        } else {
            messageProcessor = (message, consumer) -> {
                Map<String, Object> logstashEvent = Collections.singletonMap("message", new String(message.getData()));
                consumer.accept(logstashEvent);
                message.commit().join();
            };
        }

        for (Message message : event.getMessages()) {
            logger.info("Message received. SeqNo={}, offset={}", message.getSeqNo(), message.getOffset());
            messageProcessor.process(message, consumer);
        }
    }
}


@FunctionalInterface
interface MessageProcessor {
    void process(Message message, Consumer<Map<String, Object>> consumer);
}
