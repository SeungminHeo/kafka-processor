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

    public void simulate(String topicName) {
        try {
            Producer<String, String> kafkaProducer = new KafkaProducer<String, String>(props, new StringSerializer(), new KafkaJsonSerializer());
            for(int i=0; i<90; i++) {
                System.out.println(String.format("Day %s Simulating..", i));
                DataBaseConnection dbc = new DataBaseConnection();
                logData = dbc.loadLogData(
                        String.format("SELECT * FROM logdata WHERE time < '2020-09-21' - INTERVAL %s day AND time > '2020-09-21' - INTERVAL %s day", i - 1, i));
                while(logData.next()) {
                    String jsonMessage = convertToJsonObj(logData);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, jsonMessage);
                    logSender(kafkaProducer, record);
                }
                System.out.println("Log Simulator OK.");
            }

        } catch (SQLException e) {
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

    public void logSender(Producer kafkaProducer, ProducerRecord record) {
        try {
            kafkaProducer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String convertToJsonObj(ResultSet resultSet) throws SQLException {

        int total_columns = resultSet.getMetaData().getColumnCount();
        JSONObject obj = new JSONObject();
        for (int i = 0; i < total_columns; i++) {
            obj.put(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1));
        }
        return obj.toString();
    }
}
