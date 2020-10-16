package utils.extractor;

import model.Logs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.sql.Timestamp;

public class WebLogTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {

        if(! (consumerRecord.value() instanceof Logs)) {
            return System.currentTimeMillis();
        }

        Logs webLogs = (Logs) consumerRecord.value();
        Timestamp eventTime = webLogs.getTime();
        return (eventTime != null) ? eventTime.getTime() : consumerRecord.timestamp();
    }
}
