package com.example.kafka.processor;

import com.example.kafka.model.Message;
import com.example.kafka.topology.MessageFilterTopology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class CensorProcessor implements Processor<String, Message, String, Message> {
    private ProcessorContext<String, Message> context;

    @Override
    public void init(ProcessorContext<String, Message> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Message> record) {
        System.out.println("[CensorProcessor] проверка ценузры");
        Message msg = record.value();

        if (msg.message == null || msg.message.isEmpty()) {
            context.forward(record);
            return;
        }

        String filteredText = msg.message;

        for (String word : MessageFilterTopology.FORBIDDEN_WORDS) {
            if (filteredText.toLowerCase().contains(word.toLowerCase())) {
                filteredText = filteredText.replaceAll("(?i)" + word, "***");
            }
        }

        msg.message = filteredText;
        context.forward(record.withValue(msg));
    }
}
