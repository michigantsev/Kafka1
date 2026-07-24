package com.example.kafka.service.producer;

import com.example.kafka.model.Product;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import java.util.Properties;
import java.util.UUID;

@Service
public class ProducerService {

    private final KafkaProducer<String, Product> producer;
    private volatile boolean running = true;

    public ProducerService() {
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

        this.producer = new KafkaProducer<>(props);
    }

    @Async
    public void runProducer() {
        Integer id = 1;
        while (running) {
            try {
                Product jsonMessage = new Product();
                jsonMessage.setId(id++);
                jsonMessage.setName("Product Name");

                System.out.println("[Producer] ID: " + jsonMessage.getId() + ", Name: " + jsonMessage.getName());

                ProducerRecord<String, Product> record = new ProducerRecord<>("my-topic",
                        UUID.randomUUID().toString(),
                        jsonMessage);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("[Producer] Ошибка: " + exception.getMessage());
                    }
                });
                producer.flush();
                Thread.sleep(500);
            } catch (InterruptedException e) {
                System.out.println("[Producer] " + e.getMessage());
                break;
            } catch (Exception e) {
                System.out.println("[Producer] Ошибка: " + e.getMessage());
            }
        }
        if (producer != null) {
            producer.close();
        }
    }

    @PreDestroy
    public void stop() {
        this.running = false;
    }
}



