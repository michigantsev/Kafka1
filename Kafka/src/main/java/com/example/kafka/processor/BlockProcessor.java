package com.example.kafka.processor;

import com.example.kafka.model.Message;
import com.example.kafka.model.UserBlocking;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class BlockProcessor implements Processor<String, Message, String, Message> {
    private KeyValueStore<String, UserBlocking> blockStore;
    private ProcessorContext<String, Message> context;
    private final String storeName;

    public BlockProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<String, Message> context) {
        this.context = context;
        this.blockStore = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<String, Message> record) {
        System.out.println("[BlockProcessor] проверка блокировки");
        Message msg = record.value();
        UserBlocking blockEvent = blockStore.get(msg.recipientId);
        if (blockEvent != null && msg.userId.equals(blockEvent.blockedUserId)) {
            return;
        }
        context.forward(record);
    }
}
