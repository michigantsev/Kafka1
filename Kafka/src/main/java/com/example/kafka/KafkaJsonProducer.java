package com.example.kafka;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;


public class KafkaJsonProducer {
    public static class Product {
        private Integer id;
        private String name;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }


    public static void main(String[] args) throws Exception {
        Thread singleConsumerThread = new Thread(KafkaJsonProducer::runSingleMessageConsumer);
        Thread batchConsumerThread = new Thread(KafkaJsonProducer::runBatchMessageConsumer);
        singleConsumerThread.start();
        batchConsumerThread.start();

        Thread.sleep(3000);

        runProducer();
    }

    private static void runProducer() {
        Properties props = new Properties();
        // Сериализатор
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Сериализатор
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        // Адреса брокеров
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        // Адрес schema registry
        props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
        // At least once. Ждет подтверждения от всех реплик
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Попытки отправки
        props.put(ProducerConfig.RETRIES_CONFIG, 5);

        Integer id = 1;
        while (true) {
                try (KafkaProducer<String, Product> producer = new KafkaProducer<>(props)) {
                    Product jsonMessage = new Product();
                    jsonMessage.setId(id++);
                    jsonMessage.setName("Product Name");

                    System.out.println("[Producer] ID: " + jsonMessage.getId() + ", Name: " + jsonMessage.getName());

                    ProducerRecord<String, Product> record = new ProducerRecord<>("my-topic",
                        UUID.randomUUID().toString(),
                        jsonMessage);

                    producer.send(record);
                    producer.flush();
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    System.out.println("[Producer] " + e.getMessage());
                }
        }
    }

    private static void runSingleMessageConsumer() {
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
        // id группы
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-message-group");
        // Обрабатывать сообщения с начала, если нет offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Автокоммит offset - включен по условию
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, Product> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("my-topic"));

            while (true) {
                try {
                    ConsumerRecords<String, Product> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, Product> record : records) {
                        Product product = record.value();
                        System.out.println("[SingleMessageConsumer] ID: " + product.getId() + ", Name: " + product.getName());
                    }
                } catch (org.apache.kafka.common.errors.SerializationException e) {
                    System.out.println("[SingleMessageConsumer] Ошибка десериализации");
                }
            }
        } catch (Exception e) {
            System.out.println("[SingleMessageConsumer] " + e.getMessage());
        }
    }

    private static void runBatchMessageConsumer() {
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
        // id группы
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-message-group");
        // Обрабатывать сообщения с начала, если нет offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Автокоммит offset - выключен по условию
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Минимальный объем сообщений - 5 Кб. Точно больше 10 сообщений.
        // Максимальное ожидание 5 секунд - Producer отправляет сообщения раз в полсекунды, так что должно накопится 10
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 5120);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 5000);

        try (KafkaConsumer<String, Product> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("my-topic"));

            while (true) {
                try {
                    ConsumerRecords<String, Product> records = consumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        for (ConsumerRecord<String, Product> record : records) {
                            Product product = record.value();
                            System.out.println("[BatchMessageConsumer] ID: " + product.getId() + ", Name: " + product.getName());
                        }
                        consumer.commitSync();
                    }
                } catch (org.apache.kafka.common.errors.SerializationException e) {
                    System.out.println("[BatchMessageConsumer] Ошибка десериализации");
                }
            }
        } catch (Exception e) {
            System.out.println("[BatchMessageConsumer] " + e.getMessage());
        }
    }
}
