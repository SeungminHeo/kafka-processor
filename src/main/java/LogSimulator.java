import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import utils.KafkaJsonSerializer;

import java.sql.*;
import java.util.Properties;

public class LogSimulator {

    ResultSet logData;
    Properties props;

    LogSimulator(String brokerServers) {
        try {
            props = configureProducer(brokerServers);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    LogSimulator() {
        try {
            props = configureProducer("jolp-kafka-001:9092,jolp-kafka-002:9092,jolp-kafka-003:9092");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Properties configureProducer(String kafkaBrokerServers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaBrokerServers);
        properties.put("acks", "all");
        properties.put("compression.type", "snappy");
        return properties;
    }

}