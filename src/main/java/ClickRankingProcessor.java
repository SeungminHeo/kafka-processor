import model.ClickCounts;
import model.ClickLogs;
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

public class ClickRankingProcessor {

    private static Logger LOG = LoggerFactory.getLogger(ClickRankingProcessor.class);

    public static void main(String[] args) {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<ClickLogs> clickLogsSerde = StreamsSerdes.ClickLogsSerde();
        final Serde<ClickCounts> clickCountsSerde = StreamsSerdes.ClickCountsSerde();
        final Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        Duration rankingWindow = Duration.ofHours(1);

        final KStream<String, ClickLogs> clickLogsKStream = streamsBuilder.stream(Topics.CLICK_LOG.topicName(), Consumed.with(Serdes.String(), clickLogsSerde));

        final KStream<ClickCounts, ClickCounts> clickCountTransform = clickLogsKStream
                .filter((k, v) -> v.getItemId() != null, Named.as("RemoveNulls"))
                .map((ignoredKey, clickLogsValue) -> {
                    ClickCounts cc = ClickCounts.from(clickLogsValue);
                    return KeyValue.pair(cc, cc);
                }, Named.as("MappingKeysForAggregation"));

        final KTable<Windowed<ClickCounts>, Long> clickCount = clickCountTransform
                .groupByKey(Grouped.with("AssignKeys", clickCountsSerde, clickCountsSerde))
                .windowedBy(TimeWindows.of(rankingWindow))
                .count(Materialized.as("CountItem"));

        // 내림차순 정렬
        final Comparator<ClickCounts> descendingComparator =
                (o1, o2) -> (int) (o2.getClickCount() - o1.getClickCount());

        final KTable<Windowed<String>, PriorityQueue<ClickCounts>> allClickCountsPriorityQueue = clickCount
                .groupBy((windowedClickCounts, count) -> {
                            ClickCounts clickCounts = windowedClickCounts.key();
                            Windowed<String> windowedItemId = new Windowed<>("ALL", windowedClickCounts.window());
                            clickCounts.setClickCount(count);
                            return KeyValue.pair(windowedItemId, clickCounts);
                        },
                        Grouped.with("GroupingAsOneKey", windowedStringSerde, clickCountsSerde))
                .aggregate(
                        () -> new PriorityQueue<>(descendingComparator),
                        (nullKey, record, queue) -> {
                            queue.add(record);
                            return queue;
                        },
                        (nullKey, record, queue) -> {
                            queue.remove(record);
                            return queue;
                        },
                        Materialized.<Windowed<String>, PriorityQueue<ClickCounts>, KeyValueStore<Bytes, byte[]>>
                                as("ConvertToPriorityQueue")
                                .withKeySerde(windowedStringSerde)
                                .withValueSerde(new PriorityQueueSerde<>(descendingComparator, clickCountsSerde)));

        final int topN = 100;
        final KTable<Windowed<String>, String> topClickCounts = allClickCountsPriorityQueue.
                mapValues(queue -> {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("{");
                    for (int i = 0; i < topN; i++) {
                        if (i!=0) {
                            sb.append(", ");
                        }
                        final ClickCounts record = queue.poll();
                        if (record == null) {
                            break;
                        }

                        sb.append(String.format("\"%s\": %s", record.getItemId(), record.getClickCount()));

                    }
                    sb.append("}");
                    return sb.toString();
                }, Named.as("ComputeRanking"));

        topClickCounts.toStream().to(Topics.CLICK_RANKING.topicName(), Produced.with(windowedStringSerde, stringSerde));

        final Topology topology = streamsBuilder.build();
        LOG.debug(String.valueOf(topology.describe()));
        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getProperties());
        kafkaStreams.start();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Jolp-ClickRanking-Processor");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Jolp-ClickRanking-Processor");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Jolp-ClickRanking-Processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "jolp-kafka-001:9092,jolp-kafka-002:9092,jolp-kafka-003:9092");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "-");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WebLogTimeExtractor.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        return props;
    }
}
