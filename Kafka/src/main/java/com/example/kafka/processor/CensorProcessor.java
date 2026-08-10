package com.example.kafka.processor;

import com.example.kafka.model.Message;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class CensorProcessor implements Processor<String, Message, String, Message> {
    private ProcessorContext<String, Message> context;
    private KeyValueStore<String, String> wordsStore;
    private final String storeName;

    public CensorProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<String, Message> context) {
        this.context = context;
        this.wordsStore = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<String, Message> record) {
        System.out.println("[CensorProcessor] Проверка цензуры");
        Message msg = record.value();

        if (msg.message == null || msg.message.isEmpty()) {
            context.forward(record);
            return;
        }

        String filteredText = msg.message;
        try (var iterator = wordsStore.all()) {
            while (iterator.hasNext()) {
                String word = iterator.next().key;

                if (filteredText.toLowerCase().contains(word.toLowerCase())) {
                    filteredText = filteredText.replaceAll("(?i)" + word, "***");
                }
            }
        }

        msg.message = filteredText;
        context.forward(record.withValue(msg));
    }
}
