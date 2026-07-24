package com.example.kafka.service.consumer;

import com.example.kafka.model.Product;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;

public abstract class BaseConsumer {

    protected Properties getCommonProperties() {
        Properties props = new Properties();
        // Десериализатор
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        // Десериализатор
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer.class.getName());
        // Адреса брокеров
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        // Адрес schema registry
        props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
        // Класс для десериализации
        props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, Product.class.getName());
        // Обрабатывать сообщения с начала, если нет offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}



