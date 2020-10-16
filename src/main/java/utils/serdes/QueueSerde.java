package utils.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

public class QueueSerde<T> implements Serde<Queue<T>> {

    private final Serde<Queue<T>> inner;

    public QueueSerde(final Serde<T> SerDe) {
        inner = Serdes.serdeFrom(new QueueSerializer<>(SerDe.serializer()),
                new QueueDeserializer<>(SerDe.deserializer()));
    }

    @Override
    public Serializer<Queue<T>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<Queue<T>> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }

}