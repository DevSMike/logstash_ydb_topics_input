package org.logstashplugins.input;

import co.elastic.logstash.api.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstashplugins.YdbTopicsInput;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import tech.ydb.topic.read.SyncReader;

import java.util.*;
import java.util.function.Consumer;
import static org.mockito.Mockito.*;

public class YdbTopicsInputTest {

    @Mock
    private SyncReader reader;

    private YdbTopicsInput input;

    @Before
    public void setUp() {

        Map<String, Object> configValues = new HashMap<>();
        configValues.put(YdbTopicsInput.PREFIX_CONFIG.name(), "message");
        configValues.put(YdbTopicsInput.EVENT_COUNT_CONFIG.name(), 1L);
        configValues.put("topic_path", "fake_topic_path");
        configValues.put("connection_string", "grpc://localhost:2136?database=/local");

        Configuration config = new ConfigurationImpl(configValues);

        input = new YdbTopicsInput("test-input", config, null);
        MockitoAnnotations.openMocks(this);

    }

    @After
    public void tearDown() {
        input.stop();
    }

    @Test
    public void testStart() throws InterruptedException {
        Consumer<Map<String, Object>> consumer = mock(Consumer.class);
        CustomMessage testMessage = new CustomMessage("Test Message".getBytes(), 0, 0);
        // Мокируем метод receive для первого вызова возвращать testMessage, а затем вернуть null
        when(reader.receive())
                .thenReturn(testMessage)
                .thenReturn(null);

        input.setReader(reader);

        Thread thread = new Thread(() -> {
            input.start(consumer);
        });
        thread.start();
        // Ждем некоторое время, чтобы дать возможность потоку обработать сообщение
        Thread.sleep(100);

        verify(consumer, times(1)).accept(Collections.singletonMap("message", testMessage.getData())); // Проверяем, что consumer был вызван с ожидаемыми данными
        input.stop();
        input.awaitStop();
    }

}
