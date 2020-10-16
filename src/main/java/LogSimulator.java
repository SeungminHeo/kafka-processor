import model.Logs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.database.DataBaseConnection;
import utils.serdes.KafkaJsonSerializer;
import utils.Topics;

import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
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

    public void simulate(String topicName, int accelerateRate, int dayFromLastDay) {
        try {
            Producer<String, Logs> kafkaProducer = new KafkaProducer<String, Logs>(props, new StringSerializer(), new KafkaJsonSerializer());

            for(int i=dayFromLastDay; i>=0; i--) {
                System.out.println(String.format("Day %s Simulating..", i));
                DataBaseConnection dbc = new DataBaseConnection();
                logData = dbc.loadLogData(
                        String.format("SELECT * FROM logdata WHERE time < '2020-09-21' - INTERVAL %s day AND time > '2020-09-21' - INTERVAL %s day", i - 1, i));
                Long waitTime = null;
                LocalDateTime beforeTime = null;
                LocalDateTime nextTime = null;
                while(logData.next()) {
                    if (beforeTime == null) {
                        waitTime = Long.valueOf(0);
                    } else {
                        nextTime = logData.getTimestamp("time").toLocalDateTime();
                        waitTime = Duration.between(beforeTime, nextTime).toMillis() / accelerateRate;
                    }
                    Thread.sleep(waitTime);
                    Logs jsonMessage = convertToJsonObj(logData);
                    ProducerRecord<String, Logs> record = new ProducerRecord<>(topicName, jsonMessage);
                    logSender(kafkaProducer, record);
                    beforeTime = logData.getTimestamp("time").toLocalDateTime();
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
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
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
                .time(resultSet.getTimestamp("time"))
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
//        LogSimulator logSimulator = new LogSimulator("115.85.181.243:9092,115.85.180.11:9092,115.85.180.110:9092");
        int accelerateRate = Integer.parseInt(args[0]);
        int daysFromLastDay = Integer.parseInt(args[1]);

        System.out.println(String.format("Log Simulating for %s days with x%s speed.", daysFromLastDay, accelerateRate));

        logSimulator.simulate(Topics.LOG_DATA_RAW.topicName(), accelerateRate, daysFromLastDay);
    }
}

