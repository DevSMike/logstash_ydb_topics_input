package org.logstashplugins.input;

import org.junit.Before;
import org.junit.Test;
import org.logstashplugins.input.util.CustomMessage;
import org.logstashplugins.util.MessageHandler;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.impl.events.DataReceivedEventImpl;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class MessageHandlerTest {

    @Mock
    private Consumer<Map<String, Object>> consumer;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    public Void printCommit() {
        Runnable runnable = () -> System.out.println("Hello!");
        runnable.run();
        return null;
    }

    @Test
    public void testOnMessagesBinary() {
        String json = "{\n" +
                "    \"name\": \"user\",\n" +
                "    \"email\": \"user@user.com\"\n" +
                "}";
        MessageHandler messageHandler = new MessageHandler(consumer, "BINARY");

        CustomMessage message = new CustomMessage(json.getBytes(), 0, 0);

        CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(printCommit());
        Supplier<CompletableFuture<Void>> commitCallback = () -> completedFuture;

        DataReceivedEvent dataReceivedEvent = new DataReceivedEventImpl(Collections.singletonList(message),
                null, commitCallback);

        messageHandler.onMessages(dataReceivedEvent);

        Mockito.verify(consumer, Mockito.times(1)).accept(Mockito.any());
    }

    @Test
    public void testOnMessageJson() {
        String json = " { \"name\": \"example\", \"meta\": { \"id\" : 1, \"level\" : 3 } }}";
        MessageHandler messageHandler = new MessageHandler(consumer, "JSON");

        CustomMessage message = new CustomMessage(json.getBytes(), 0, 0);

        CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(printCommit());
        Supplier<CompletableFuture<Void>> commitCallback = () -> completedFuture;

        DataReceivedEvent dataReceivedEvent = new DataReceivedEventImpl(Collections.singletonList(message),
                null, commitCallback);

        messageHandler.onMessages(dataReceivedEvent);

        Mockito.verify(consumer, Mockito.times(1)).accept(Mockito.any());
    }
}
