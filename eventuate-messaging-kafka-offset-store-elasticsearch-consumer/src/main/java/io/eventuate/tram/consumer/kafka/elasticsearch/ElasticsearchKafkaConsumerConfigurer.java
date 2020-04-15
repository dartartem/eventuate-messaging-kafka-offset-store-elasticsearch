package io.eventuate.tram.consumer.kafka.elasticsearch;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.client.RestHighLevelClient;
import io.eventuate.messaging.kafka.basic.consumer.DefaultKafkaConsumerOffsetStorage;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerConfigurer;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerOffsetStorageStrategy;
import io.eventuate.messaging.kafka.basic.consumer.SaveOffsetOnRebalanceListener;

public class ElasticsearchKafkaConsumerConfigurer implements KafkaConsumerConfigurer {

    private final RestHighLevelClient client;
    private final ElasticsearchOffsetStorageConfigurationProperties properties;

    public ElasticsearchKafkaConsumerConfigurer(RestHighLevelClient client, ElasticsearchOffsetStorageConfigurationProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    @Override
    public KafkaConsumer<String, byte[]> makeConsumer(Properties consumerProperties) {
        return new KafkaConsumer<>(consumerProperties);
    }

    @Override
    public void subscribe(KafkaConsumer<String, byte[]> kafkaConsumer, KafkaConsumerOffsetStorageStrategy offsetStorage, String subscriberId, List<String> topics) {
        kafkaConsumer.subscribe(
            topics,
            new SaveOffsetOnRebalanceListener(
                offsetStorage,
                kafkaConsumer)
        );
    }

    @Override
    public KafkaConsumerOffsetStorageStrategy makeOffsetStorage(
        KafkaConsumer<String, byte[]> consumer, String subscriberId)
    {
        return new ElasticsearchOffsetStorage(
            new DefaultKafkaConsumerOffsetStorage(consumer),
            client,
            properties,
            subscriberId
        );
    }
}
