import model.ClickLogs;
import model.ClickMatrix;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Topics;
import utils.extractor.WebLogTimeExtractor;
import utils.serdes.PriorityQueueSerde;
import utils.serdes.StreamsSerdes;

import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class ClickMatrixProcessor {

    private static Logger LOG = LoggerFactory.getLogger(ClickMatrixProcessor.class);

    public static void main(String[] args) {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<ClickLogs> clickLogsSerde = StreamsSerdes.ClickLogsSerde();
        final Serde<ClickMatrix> clickMatrixSerde = StreamsSerdes.ClickMatrixSerde();
        final Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

//        Duration rankingWindow = Duration.ofHours(3);
//        Duration hoppingWindow = Duration.ofMinutes(15);

        Duration rankingWindow = Duration.ofMinutes(2);
        Duration gracePeriod = Duration.ofMinutes(1);
        Duration hoppingWindow = Duration.ofMinutes(1);

        final Comparator<ClickMatrix> nullComparator = (o1, o2) -> 0;
        final Comparator<ClickMatrix> descendingComparator =
                (o1, o2) -> (int) (o2.getClickCount() - o1.getClickCount());

        final KStream<String, ClickLogs> clickLogsKStream = streamsBuilder.stream(Topics.CLICK_LOG.topicName(), Consumed.with(Serdes.String(), clickLogsSerde));


        final KStream<ClickMatrix, ClickMatrix> clickMatrixTransform = clickLogsKStream
                .filter((k, v) -> v.getItemId() != null, Named.as("RemoveNulls"))
                .map((ignoredKey, clickLogsValue) -> {
                    ClickMatrix cm = ClickMatrix.from(clickLogsValue);
                    return KeyValue.pair(cm, cm);
                }, Named.as("MappingKeysForAggregation"));

//        final KStream<Windowed<ClickMatrix>, Long> clickMatrix = clickMatrixTransform
//                .groupByKey(Grouped.with(clickMatrixSerde, clickMatrixSerde))
//                .windowedBy(TimeWindows.of(rankingWindow).advanceBy(hoppingWindow).grace(gracePeriod))
//                .count(Materialized.as("CountItemByPiwikIdItemId"))
//                .suppress(Suppressed.untilWindowCloses(unbounded()))
//                .toStream();
//
//        clickMatrix.print(Printed.<Windowed<ClickMatrix>, Long>toSysOut());
//
//        final KTable<Windowed<String>, PriorityQueue<ClickMatrix>> clickMatrixQueue = clickMatrix
//                .toTable(Named.as("QueueTable"))
//                .groupBy((windowedClickMatrix, count) -> {
//                            ClickMatrix clickMatrice = windowedClickMatrix.key();
//                            Windowed<String> piwikId = new Windowed<>(clickMatrice.getPiwikId(), windowedClickMatrix.window());
//                            clickMatrice.setClickCount(count);
//                            return KeyValue.pair(piwikId, clickMatrice);
//                        },
//                        Grouped.with("GroupingByPiwikIdItemId", windowedStringSerde, clickMatrixSerde))
//                .aggregate(
//                        () -> new PriorityQueue<>(nullComparator),
//                        (nullKey, record, queue) -> {
//                            queue.add(record);
//                            return queue;
//                        },
//                        (nullKey, record, queue) -> {
//                            queue.remove(record);
//                            return queue;
//                        },
//                        Materialized.<Windowed<String>,
//                                PriorityQueue<ClickMatrix>,
//                                KeyValueStore<Bytes, byte[]>>
//                                as("ConvertToQueue")
//                                .withKeySerde(windowedStringSerde)
//                                .withValueSerde(new PriorityQueueSerde<>(nullComparator, clickMatrixSerde)));
//
//
//        final KTable<Windowed<String>, String> aggClickMatrix = clickMatrixQueue.
//                mapValues(queue -> {
//                    final StringBuilder sb = new StringBuilder();
//                    final ClickMatrix firstRecord = queue.poll();
//                    final int queueSize = queue.size();
//                    if (firstRecord != null) {
//                        sb.append(String.format("\"%s\": ", firstRecord.getPiwikId()));
//                        sb.append(String.format("{\"%s\": %s", firstRecord.getItemId(), firstRecord.getClickCount()));
//                        for (int i = 1; i < queueSize; i++) {
//                            final ClickMatrix record = queue.poll();
//                            if (record == null) {
//                                break;
//                            }
//                            sb.append(", ");
//                            sb.append(String.format("\"%s\": %s", record.getItemId(), record.getClickCount()));
//                        }
//                        sb.append("}");
//                    }
//                    return sb.toString();
//                }).
//                groupBy((piwikId, itemClickCount) -> {
//                    Windowed<String> piwikIdWindowed = new Windowed<>("ALL", piwikId.window());
//                    return KeyValue.pair(piwikIdWindowed, itemClickCount);
//                }, Grouped.with("ConvertToOneKey", windowedStringSerde, stringSerde)).
//                reduce(new Reducer<String>() {
//                    @Override
//                    public String apply(String value1, String value2) {
//
//                        return value1.concat(", ").concat(value2);
//                    }
//                }, new Reducer<String>() {
//                    @Override
//                    public String apply(String value1, String value2) {
//                        return value1.replace(", ".concat(value2), "");
//                    }
//                }, Materialized.<Windowed<String>,
//                        String,
//                        KeyValueStore<Bytes, byte[]>>
//                        as("ConvertToMatrix")
//                        .withKeySerde(windowedStringSerde)
//                        .withValueSerde(stringSerde)).
//                mapValues(message -> "{".concat(message).concat("}"));
//        KStream<Windowed<String>, String> clickMatrixStream = aggClickMatrix.toStream();
//        clickMatrixStream.print(Printed.<Windowed<String>, String>toSysOut());


        final KTable<Windowed<String>, String> clickMatrix = clickMatrixTransform
                .groupByKey(Grouped.with(clickMatrixSerde, clickMatrixSerde))
                .windowedBy(TimeWindows.of(rankingWindow).advanceBy(hoppingWindow).grace(gracePeriod))
                .count(Materialized.as("CountItemByPiwikIdItemId"))
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .groupBy((windowedClickMatrix, count) -> {
                            ClickMatrix clickMatrice = windowedClickMatrix.key();
                            Windowed<String> piwikId = new Windowed<>(clickMatrice.getPiwikId(), windowedClickMatrix.window());
                            clickMatrice.setClickCount(count);
                            return KeyValue.pair(piwikId, clickMatrice);
                        },
                        Grouped.with("GroupingByPiwikIdItemId", windowedStringSerde, clickMatrixSerde))
                .aggregate(
                        () -> new PriorityQueue<>(nullComparator),
                        (nullKey, record, queue) -> {
                            queue.add(record);
                            return queue;
                        },
                        (nullKey, record, queue) -> {
                            queue.remove(record);
                            return queue;
                        },
                        Materialized.<Windowed<String>,
                                PriorityQueue<ClickMatrix>,
                                KeyValueStore<Bytes, byte[]>>
                                as("ConvertToQueue")
                                .withKeySerde(windowedStringSerde)
                                .withValueSerde(new PriorityQueueSerde<>(nullComparator, clickMatrixSerde))).
                mapValues(queue -> {
                    final StringBuilder sb = new StringBuilder();
                    final ClickMatrix firstRecord = queue.poll();
                    final int queueSize = queue.size();
                    if (firstRecord != null) {
                        sb.append(String.format("\"%s\": ", firstRecord.getPiwikId()));
                        sb.append(String.format("{\"%s\": %s", firstRecord.getItemId(), firstRecord.getClickCount()));
                        for (int i = 1; i < queueSize; i++) {
                            final ClickMatrix record = queue.poll();
                            if (record == null) {
                                break;
                            }
                            sb.append(", ");
                            sb.append(String.format("\"%s\": %s", record.getItemId(), record.getClickCount()));
                        }
                        sb.append("}");
                    }
                    return sb.toString();
                }).
                groupBy((piwikId, itemClickCount) -> {
                    Windowed<String> piwikIdWindowed = new Windowed<>("ALL", piwikId.window());
                    return KeyValue.pair(piwikIdWindowed, itemClickCount);
                }, Grouped.with("ConvertToOneKey", windowedStringSerde, stringSerde)).
                reduce(new Reducer<String>() {
                    @Override
                    public String apply(String value1, String value2) {

                        return value1.concat(", ").concat(value2);
                    }
                }, new Reducer<String>() {
                    @Override
                    public String apply(String value1, String value2) {
                        return value1.replace(", ".concat(value2), "");
                    }
                }, Materialized.<Windowed<String>,
                        String,
                        KeyValueStore<Bytes, byte[]>>
                        as("ConvertToMatrix")
                        .withKeySerde(windowedStringSerde)
                        .withValueSerde(stringSerde)).
                mapValues(message -> "{".concat(message).concat("}"));

        KStream<Windowed<String>, String> clickMatrixStream = clickMatrix.toStream();
        clickMatrixStream.print(Printed.<Windowed<String>, String>toSysOut());

        clickMatrixStream.to(Topics.CLICK_MATRIX.topicName(), Produced.with(windowedStringSerde, stringSerde));
        final Topology topology = streamsBuilder.build();
        LOG.debug(String.valueOf(topology.describe()));
        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getProperties());
        kafkaStreams.start();

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Jolp-ClickMatrix-Processor");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Jolp-ClickMatrix-Processor");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Jolp-ClickMatrix-Processor");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "jolp-kafka-001:9092,jolp-kafka-002:9092,jolp-kafka-003:9092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "-");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WebLogTimeExtractor.class);
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        return props;
    }
}
