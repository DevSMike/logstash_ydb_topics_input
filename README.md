# Logstash YDB Topics Plugin

На вход плагина поступают данные из **YDB Topics**, а на выход передаются непосредтсвенно в **Logstash**.

## Тестовая Конфигурация Input Плагина

```
input {
  ydb_topics_input {
    count => 3  # Количество сообщений для чтения
    prefix => "message"  # Префикс для сообщений
    topic_path => "topic_path"  # Путь к топику в YDB Topics
    connection_string => "grpc://localhost:2136?database=/local"
  }
}

output {
  stdout { codec => rubydebug }  # Вывод в стандартный вывод с форматированием Ruby Debug
}
```
