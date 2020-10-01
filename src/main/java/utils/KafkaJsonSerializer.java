package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaJsonSerializer implements Serializer {

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] byteMessage = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            byteMessage = objectMapper.writeValueAsBytes(o);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return byteMessage;
    }

    @Override
    public void close() {

    }
}