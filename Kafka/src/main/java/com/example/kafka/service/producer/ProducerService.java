package com.example.kafka.service.producer;

import com.example.kafka.model.Message;
import com.example.kafka.model.UserBlocking;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ProducerService {

    private final KafkaProducer<String, Object> producer;
    private volatile boolean running = true;

    public ProducerService() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        this.producer = new KafkaProducer<>(props);
    }

    @Async
    public void runProducer() {
        try {
            UserBlocking block = new UserBlocking();
            block.userId = "user_2";
            block.blockedUserId = "user_1";
            block.timestamp = System.currentTimeMillis();

            System.out.println("[Producer] Отправка блокировки: user_2 забанил user_1");
            producer.send(new ProducerRecord<>("blocked_users", "user_2", block));

            System.out.println("[Producer] Отправка запрещенного слова: яндекс");
            producer.send(new ProducerRecord<>("forbidden_words", "яндекс", "яндекс"));
            producer.flush();

            Thread.sleep(2000);

            int idCounter = 1;

            while (running) {
                Message blockedMsg = new Message();
                blockedMsg.userId = "user_1";
                blockedMsg.recipientId = "user_2";
                blockedMsg.message = "Сообщение №" + idCounter + " от забаненного";
                blockedMsg.timestamp = System.currentTimeMillis();

                producer.send(new ProducerRecord<>("messages", "user_1", blockedMsg), (metadata, exception) -> {
                    if (exception != null) System.out.println("[Producer] Ошибка: " + exception.getMessage());
                });

                Message censMsg = new Message();
                censMsg.userId = "user_2";
                censMsg.recipientId = "user_1";
                censMsg.message = "Сообщение №" + idCounter + " яндекс";
                blockedMsg.timestamp = System.currentTimeMillis();

                producer.send(new ProducerRecord<>("messages", "user_1", censMsg));

                System.out.println("[Producer] Отправлена пара тестовых сообщений №" + idCounter);
                producer.flush();

                idCounter++;
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("[Producer] Поток остановлен: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("[Producer] Критическая ошибка: " + e.getMessage());
        }
    }

    @PreDestroy
    public void stop() {
        this.running = false;
        if (producer != null) {
            producer.close();
        }
    }
}



