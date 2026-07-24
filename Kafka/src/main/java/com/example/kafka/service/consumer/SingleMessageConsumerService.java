package com.example.kafka.service.consumer;

import com.example.kafka.model.Product;
import com.example.kafka.service.consumer.BaseConsumer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Service
public class SingleMessageConsumerService extends BaseConsumer {

    private volatile boolean running = true;

    @Async
    public void runConsumer() {
        Properties props = getCommonProperties();
        // id группы
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-message-group");
        // Обрабатывать сообщения с начала, если нет offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Автокоммит offset - включен по условию
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, Product> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("my-topic"));

            while (running) {
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

    @PreDestroy
    public void stop() {
        this.running = false;
    }
}


