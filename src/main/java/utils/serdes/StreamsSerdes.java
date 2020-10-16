package utils.serdes;

import model.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StreamsSerdes {
    public static Serde<Logs> LogsSerde() { return new LogsSerde(); }

    public static Serde<ClickLogs> ClickLogsSerde() { return new ClickLogsSerde(); }

    public static Serde<CartLogs> CartLogsSerde() { return new CartLogsSerde(); }

    public static Serde<SearchLogs> SearchLogsSerde() { return new SearchLogsSerde(); }

    public static Serde<OrderCompleteLogs> OrderCompleteLogsSerde() { return new OrderCompleteLogsSerde(); }

    public static Serde<ClickCounts> ClickCountsSerde() { return new ClickCountsSerde(); }

    public static Serde<ClickMatrix> ClickMatrixSerde() { return new ClickMatrixSerde(); }

    public static final class LogsSerde extends WrapperSerde<Logs> {
        public LogsSerde() {
            super(new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>(Logs.class));
        }
    }

    public static final class ClickLogsSerde extends WrapperSerde<ClickLogs> {
        public ClickLogsSerde() {
            super(new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>(ClickLogs.class));
        }
    }

    public static final class CartLogsSerde extends WrapperSerde<CartLogs> {
        public CartLogsSerde() {
            super(new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>(CartLogs.class));
        }
    }

    public static final class SearchLogsSerde extends WrapperSerde<SearchLogs> {
        public SearchLogsSerde() {
            super(new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>(SearchLogs.class));
        }
    }

    public static final class OrderCompleteLogsSerde extends WrapperSerde<OrderCompleteLogs> {
        public OrderCompleteLogsSerde() {
            super(new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>(OrderCompleteLogs.class));
        }
    }

    public static final class ClickCountsSerde extends WrapperSerde<ClickCounts> {
        public ClickCountsSerde() {
            super(new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>(ClickCounts.class));
        }
    }

    public static final class ClickMatrixSerde extends WrapperSerde<ClickMatrix> {
        public ClickMatrixSerde() {
            super(new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>(ClickMatrix.class));
        }
    }

    private static class WrapperSerde<T> implements Serde<T> {

        private KafkaJsonSerializer<T> serializer;
        private KafkaJsonDeserializer<T> deserializer;

        WrapperSerde(KafkaJsonSerializer serializer, KafkaJsonDeserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) { }

        @Override
        public void close() { }

        @Override
        public Serializer<T> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }
    }
}
