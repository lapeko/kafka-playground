package com.example.__spring_webflux.configs;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Configuration
public class KafkaConfig {
    private static final String RETENTION = "3600000";
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    @Value("${topics.people-basic.name}")
    String topicName;

    @Value("${topics.people-basic.partitions}")
    int topicPartitions;

    @Value("${topics.people-basic.replicas}")
    int topicReplicas;

    @Bean
    public NewTopic peopleBasicTopic() {
        return TopicBuilder
                .name(topicName)
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .build();
    }

    @Bean
    public NewTopic peopleBasicShortTopic() {
        return TopicBuilder
                .name(topicName + "-short")
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, RETENTION)
                .build();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void updateTopicConfig() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        var adminClient = AdminClient.create(props);
        var retention = adminClient
                .describeConfigs(Set.of(configResource))
                .all()
                .get()
                .get(configResource)
                .get(TopicConfig.RETENTION_MS_CONFIG)
                .value();
        if ("3600000".equals(retention)) {
            return;
        }

        var configEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, RETENTION);
        var operation = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Map.of(configResource, Set.of(operation));

        adminClient.incrementalAlterConfigs(alterConfigs).all().get();
        log.info("Topic retention updated");
    }
}
