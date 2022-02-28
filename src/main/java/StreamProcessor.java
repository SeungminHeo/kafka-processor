import model.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Topics;
import utils.serdes.StreamsSerdes;

import java.util.Properties;

public class StreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamProcessor.class);

    public static void main(String[] args) throws Exception {
        boolean isServer = Boolean.parseBoolean(args[0]);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Set up Serdes
        final Serde<Logs> logsSerde = StreamsSerdes.LogsSerde();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<ClickLogs> clickLogsSerde = StreamsSerdes.ClickLogsSerde();
        final Serde<SearchLogs> searchLogsSerde = StreamsSerdes.SearchLogsSerde();
        final Serde<CartLogs> cartLogsSerde = StreamsSerdes.CartLogsSerde();
        final Serde<OrderCompleteLogs> orderCompleteLogsSerde = StreamsSerdes.OrderCompleteLogsSerde();

        // Set up predicates for branch
        final Predicate<String, Logs> clickLogs = (key, logs) -> logs.getUrl().contains("m/product.html");
        final Predicate<String, Logs> orderAttemptLogs = (key, logs) -> logs.getUrl().contains("m/order.html");
        final Predicate<String, Logs> orderCompleteLogs = (key, logs) -> logs.getUrl().contains("m/order_complete.html");
        final Predicate<String, Logs> cartLogs = (key, logs) -> logs.getUrl().contains("m/basket.html");
        final Predicate<String, Logs> searchLogs = (key, logs) -> logs.getUrl().contains("m/search.html");

        // branch inde5x
        int CLICK_LOG = 0;
        int ORDER_LOG = 1;
        int ORDER_COMPLETE_LOG = 2;
        int CART_LOG = 3;
        int SEARCH_LOG = 4;

        final KStream<String, Logs> logsStream = streamsBuilder.stream(Topics.LOG_DATA_RAW.topicName(), Consumed.with(Serdes.String(), logsSerde));
        final KStream<String, Logs>[] webLogBranchStream = logsStream.selectKey((k, v) -> v.getUrl()).branch(clickLogs, orderAttemptLogs, orderCompleteLogs, cartLogs, searchLogs);

        final KStream<String, Logs> clickLogsBranch = webLogBranchStream[CLICK_LOG];
        final KStream<String, Logs> orderAttemptLogsBranch = webLogBranchStream[ORDER_LOG];
        final KStream<String, Logs> orderCompleteLogsBranch = webLogBranchStream[ORDER_COMPLETE_LOG];
        final KStream<String, Logs> cartLogsBranch = webLogBranchStream[CART_LOG];
        final KStream<String, Logs> searchLogsBranch = webLogBranchStream[SEARCH_LOG];

        // click log processing
        final KStream<String, ClickLogs> clickLogsKStream = clickLogsBranch
                .mapValues(logs -> ClickLogs.builder(logs).build())
                .filter((k, v) -> v.getItemId() != null, Named.as("RemoveNulls"));
        clickLogsKStream.to(Topics.CLICK_LOG.topicName(), Produced.with(stringSerde, clickLogsSerde));

        // search log processing
        final KStream<String, SearchLogs> searchLogsKStream = searchLogsBranch.mapValues(logs -> SearchLogs.builder(logs).build());
        searchLogsKStream.to(Topics.SEARCH_LOG.topicName(), Produced.with(stringSerde, searchLogsSerde));

        // cart log processing
        final KStream<String, CartLogs> cartLogsKStream = cartLogsBranch.mapValues(logs -> CartLogs.builder(logs).build());
        cartLogsKStream.to(Topics.CART_LOG.topicName(), Produced.with(stringSerde, cartLogsSerde));

        // order complete log processing
        final KStream<String, OrderCompleteLogs> orderCompleteLogsKStream = orderCompleteLogsBranch.mapValues(logs -> OrderCompleteLogs.builder(logs).build());
        orderCompleteLogsKStream.to(Topics.ORDER_COMPLETE_LOG.topicName(), Produced.with(stringSerde, orderCompleteLogsSerde));


        final Topology topology = streamsBuilder.build();
        LOG.debug(String.valueOf(topology.describe()));
        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getProperties(isServer));
        kafkaStreams.start();
    }


    private static Properties getProperties(boolean isServer) {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Jolp-Kafka-Streams-Client");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Jolp-Kafka-Processor");
        if(isServer) {
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "-");
        } else {
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "-");

        }

//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WebLogTimeExtractor.class);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        return props;
    }
}


