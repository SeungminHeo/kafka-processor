import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import utils.Topics;

import java.io.File;
import java.io.FileReader;
import java.util.*;

public class TopicManager {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "jolp-kafka-001:9092,jolp-kafka-002:9092,jolp-kafka-003:9092");

        AdminClient adminClient = AdminClient.create(props);

        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        Set<String> topicSets = adminClient.listTopics(listTopicsOptions).names().get();
        topicSets.remove("__consumer_offsets");
        System.out.println(topicSets);
        adminClient.deleteTopics(topicSets);

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        for (Topics topic : Topics.values()) {
            Thread.sleep(500);
            newTopics.add(new NewTopic(topic.topicName() , 60, (short) 3));
        }
        adminClient.createTopics(newTopics);

        adminClient.close();
    }
}
