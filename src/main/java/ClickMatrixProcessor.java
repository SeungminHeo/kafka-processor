import model.ClickLogs;
import model.ClickMatrix;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
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

public class ClickMatrixProcessor {

    private static Logger LOG = LoggerFactory.getLogger(ClickMatrixProcessor.class);

    public static void main(String[] args) {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<ClickLogs> clickLogsSerde = StreamsSerdes.ClickLogsSerde();
        final Serde<ClickMatrix> clickMatrixSerde = StreamsSerdes.ClickMatrixSerde();
        final Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        Duration rankingWindow = Duration.ofHours(1);

        final KStream<String, ClickLogs> clickLogsKStream = streamsBuilder.stream(Topics.CLICK_LOG.topicName(), Consumed.with(Serdes.String(), clickLogsSerde));


        final KStream<ClickMatrix, ClickMatrix> clickMatrixTransform = clickLogsKStream
                .filter((k, v) -> v.getItemId() != null, Named.as("RemoveNulls"))
                .map((ignoredKey, clickLogsValue) -> {
                    ClickMatrix cm = ClickMatrix.from(clickLogsValue);
                    return KeyValue.pair(cm, cm);
                }, Named.as("MappingKeysForAggregation"));

        final KTable<Windowed<ClickMatrix>, Long> clickMatrix = clickMatrixTransform
                .groupByKey(Grouped.with(clickMatrixSerde, clickMatrixSerde))
                .windowedBy(TimeWindows.of(rankingWindow))
                .count(Materialized.as("CountItemByPiwikIdItemId"));

        final Comparator<ClickMatrix> nullComparator = (o1, o2) -> 0;

        final KTable<Windowed<String>, PriorityQueue<ClickMatrix>> allClickMatrixQueue = clickMatrix
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
                                .withValueSerde(new PriorityQueueSerde<>(nullComparator, clickMatrixSerde)));

        final KTable<Windowed<String>, String> clickMatrixByPiwikId = allClickMatrixQueue.
                mapValues(queue -> {
                    final StringBuilder sb = new StringBuilder();
                    final ClickMatrix firstRecord = queue.poll();
                    if (firstRecord != null) {
                        sb.append(String.format("\"%s\": ", firstRecord.getPiwikId()));
                        sb.append(String.format("{\"%s\": %s", firstRecord.getItemId(), firstRecord.getClickCount()));
                        for (int i = 1; i < queue.size(); i++) {
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
                });

        final KTable<Windowed<String>, String> aggClickMatrix = clickMatrixByPiwikId.
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
        KStream<Windowed<String>, String> clickMatrixStream = aggClickMatrix.toStream();

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
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "jolp-kafka-001:9092,jolp-kafka-002:9092,jolp-kafka-003:9092");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "115.85.181.243:9092,115.85.180.11:9092,115.85.180.110:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WebLogTimeExtractor.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        return props;
    }
}
