//package io.pravega.example.common;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.pravega.client.stream.EventRead;
//import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
//import org.apache.flink.api.common.serialization.SerializationSchema;
//
//public class TransactionSerializationSchema extends PravegaDeserializationSchema<ChunkedVideoFrame> {
//    private final ObjectMapper mapper = new ObjectMapper();
//
//    public TransactionSerializationSchema() {
//        super(ChunkedVideoFrame.class, new TransactionSeralizer());
//    }
//
//    @Override
//    public ChunkedVideoFrame extractEvent(EventRead<ChunkedVideoFrame> eventRead) {
//        final ChunkedVideoFrame event = eventRead.getEvent();
//        if (event != null) {
//            event.eventReadMetadata = new EventReadMetadata(eventRead);
//        }
//        return event;
//    }
//
//    @Override
//    public byte[] serialize(Transaction element) {
//        try {
//            return mapper.writeValueAsBytes(element);
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to serialize", e);
//        }
//    }
//}
