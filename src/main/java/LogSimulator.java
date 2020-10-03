import model.Logs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.database.DataBaseConnection;
import utils.serdes.KafkaJsonSerializer;
import utils.Topics;

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
            Producer<String, Logs> kafkaProducer = new KafkaProducer<String, Logs>(props, new StringSerializer(), new KafkaJsonSerializer());

            for(int i=0; i<90; i++) {
                System.out.println(String.format("Day %s Simulating..", i));
                DataBaseConnection dbc = new DataBaseConnection();
                logData = dbc.loadLogData(
                        String.format("SELECT * FROM logdata WHERE time < '2020-09-21' - INTERVAL %s day AND time > '2020-09-21' - INTERVAL %s day", i - 1, i));
                while(logData.next()) {
                    int random = (int)(300 * Math.random());
                    Thread.sleep(random);
                    Logs jsonMessage = convertToJsonObj(logData);
                    ProducerRecord<String, Logs> record = new ProducerRecord<>(topicName, jsonMessage);
                    logSender(kafkaProducer, record);
                }
                System.out.println("Log Simulator OK.");
            }
            kafkaProducer.close();

        } catch (SQLException | InterruptedException e) {
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

    private static Logs convertToJsonObj(ResultSet resultSet) throws SQLException {
        Logs logs = Logs
                .builder()
                .piwikId(resultSet.getString("piwik_id"))
                .time(resultSet.getTime("time"))
                .visitCount(resultSet.getInt("visit_cnt"))
                .isApp(resultSet.getString("is_app"))
                .isMobile(resultSet.getString("is_mobile"))
                .title(resultSet.getString("title"))
                .url(resultSet.getString("url"))
                .urlref(resultSet.getString("urlref"))
                .dateId(resultSet.getDate("date_id"))
                .build();

        return logs;
    }

    public static void main(String[] args){
        LogSimulator logSimulator = new LogSimulator();

        logSimulator.simulate(Topics.LOG_DATA_RAW.topicName());
    }
}

