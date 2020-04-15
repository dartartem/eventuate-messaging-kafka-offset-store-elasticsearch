package io.eventuate.tram.consumer.kafka.elasticsearch;

import javax.inject.Singleton;
import io.micronaut.context.annotation.Factory;

@Factory
public class EventuateKafkaConsumerElasticsearchSpringConfigurationPropertiesConfiguration {
    @Singleton
    ElasticsearchOffsetStorageConfigurationProperties eventuateKafkaConsumerElasticsearchSpringConfigurationProperties(
        EventuateKafkaConsumerElasticsearchMicronautConfigurationProperties eventuateKafkaConsumerElasticsearchMicronautConfigurationProperties) {
        return new ElasticsearchOffsetStorageConfigurationProperties(eventuateKafkaConsumerElasticsearchMicronautConfigurationProperties.getProperties());
    }
}
