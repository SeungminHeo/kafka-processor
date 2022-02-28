import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import utils.Topics;

import java.io.File;
import java.io.FileReader;
import java.util.*;

public class TopicManager {

    public static void main(String[] args) throws Exception {
        boolean isServer = Boolean.parseBoolean(args[0]);

        Properties props = new Properties();
        if(isServer) {
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "-");
        } else {
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "-");
        }

        AdminClient adminClient = AdminClient.create(props);

        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        Set<String> topicSets = adminClient.listTopics(listTopicsOptions).names().get();
        topicSets.remove("__consumer_offsets");
        System.out.println(topicSets);
        adminClient.deleteTopics(topicSets);

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        for (Topics topic : Topics.values()) {
            Thread.sleep(1000);
            System.out.println(topic.topicName() + " is created.");
            newTopics.add(new NewTopic(topic.topicName() , 60, (short) 3));
        }
        adminClient.createTopics(newTopics);

        adminClient.close();
    }
}
