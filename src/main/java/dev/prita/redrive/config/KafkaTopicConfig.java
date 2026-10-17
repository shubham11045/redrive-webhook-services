package dev.prita.redrive.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic redriveEventsTopic(RedriveProperties props) {
        return TopicBuilder.name(props.topic())
                .partitions(props.topicPartitions())
                .replicas(1) // single-broker dev/demo setup; documented limitation
                .build();
    }
}
