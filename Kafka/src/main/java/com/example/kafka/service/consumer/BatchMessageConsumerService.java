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
public class BatchMessageConsumerService extends BaseConsumer {

    private volatile boolean running = true;

    @Async
    public void runConsumer() {
        Properties props = getCommonProperties();
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

            while (running) {
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

    @PreDestroy
    public void stop() {
        this.running = false;
    }
}
