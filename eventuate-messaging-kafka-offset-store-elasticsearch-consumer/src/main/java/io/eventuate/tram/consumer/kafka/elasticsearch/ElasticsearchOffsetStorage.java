package io.eventuate.tram.consumer.kafka.elasticsearch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerOffsetStorageStrategy;
import io.eventuate.messaging.kafka.basic.consumer.OffsetStorageException;

public class ElasticsearchOffsetStorage implements KafkaConsumerOffsetStorageStrategy {

    public static final int SIZE = 10000;

    private final KafkaConsumerOffsetStorageStrategy defaultStorage;
    private final RestHighLevelClient client;
    private final ElasticsearchOffsetStorageConfigurationProperties properties;
    private String subscriptionId;

    private final static ObjectMapper MAPPER =
        new ObjectMapper().registerModule(
            new SimpleModule()
                .addSerializer(OffsetStorageDocument.class, new OffsetStorageDocumentSerializer())
                .addDeserializer(OffsetStorageDocument.class, new OffsetStorageDocumentDeserializer())
        );

    public ElasticsearchOffsetStorage(KafkaConsumerOffsetStorageStrategy defaultStorage, RestHighLevelClient client, ElasticsearchOffsetStorageConfigurationProperties properties, String subscriptionId) {
        this.defaultStorage = defaultStorage;
        this.client = client;
        this.properties = properties;
        this.subscriptionId = subscriptionId;
    }

    @Override
    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            if (offsets.isEmpty()) {
                return;
            }
            defaultStorage.commit(offsets);
            BulkRequest request = new BulkRequest();
            offsets.forEach((partition, offsetAndMetadata) -> {
                try {
                    OffsetStorageDocument document = new OffsetStorageDocument(partition, offsetAndMetadata, subscriptionId);
                    request.add(new IndexRequest()
                                    .index(properties.getOffsetStorageIndexName())
                                    .type(properties.getOffsetStorageTypeName())
                                    .id(document.id())
                                    .source(MAPPER.writeValueAsString(document), XContentType.JSON));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                throw new OffsetStorageException(response.buildFailureMessage());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error storing the offsets in elasticsearch", e);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> read() {
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            SearchResponse response = client.search(
                new SearchRequest()
                    .source(new SearchSourceBuilder()
                                .size(SIZE)
                                .query(QueryBuilders.matchQuery(OffsetStorageDocument.SUBSCRIPTION_ID, subscriptionId)))
                    .indices(properties.getOffsetStorageIndexName()),
                RequestOptions.DEFAULT);
            for (SearchHit hit : response.getHits().getHits()) {
                OffsetStorageDocument offsetDocument = MAPPER.readValue(hit.getSourceAsString(), OffsetStorageDocument.class);
                offsets.put(offsetDocument.partition, offsetDocument.offsetAndMetadata);
            }
            return offsets;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final static class OffsetStorageDocument {

        static final String SUBSCRIPTION_ID = "subscriptionId";
        static final String TOPIC = "topic";
        static final String PARTITION = "partition";
        static final String OFFSET = "offset";
        static final String METADATA = "metadata";

        TopicPartition partition;
        OffsetAndMetadata offsetAndMetadata;

        String subscriptionId;

        public OffsetStorageDocument(TopicPartition partition, OffsetAndMetadata offsetAndMetadata, String subscriptionId) {
            this.partition = partition;
            this.offsetAndMetadata = offsetAndMetadata;
            this.subscriptionId = subscriptionId;
        }

        public String id() {
            return String.format("%s-%s-%s", subscriptionId, partition.topic(), partition.partition());
        }
    }

    private final static class OffsetStorageDocumentSerializer extends StdSerializer<OffsetStorageDocument> {

        public OffsetStorageDocumentSerializer() {
            this(null);
        }

        protected OffsetStorageDocumentSerializer(Class<OffsetStorageDocument> t) {
            super(t);
        }

        @Override
        public void serialize(OffsetStorageDocument document, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonGenerationException {
            jgen.writeStartObject();
            jgen.writeStringField(OffsetStorageDocument.SUBSCRIPTION_ID, document.subscriptionId);
            jgen.writeStringField(OffsetStorageDocument.TOPIC, document.partition.topic());
            jgen.writeNumberField(OffsetStorageDocument.PARTITION, document.partition.partition());
            jgen.writeNumberField(OffsetStorageDocument.OFFSET, document.offsetAndMetadata.offset());
            jgen.writeStringField(OffsetStorageDocument.METADATA, document.offsetAndMetadata.metadata());
            jgen.writeEndObject();
        }
    }

    static final class OffsetStorageDocumentDeserializer extends StdDeserializer<OffsetStorageDocument> {

        public OffsetStorageDocumentDeserializer() {
            this(null);
        }

        public OffsetStorageDocumentDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public OffsetStorageDocument deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            JsonNode node = jp.getCodec().readTree(jp);
            String topic = node.get(OffsetStorageDocument.TOPIC).asText();
            int partition = node.get(OffsetStorageDocument.PARTITION).numberValue().intValue();
            long offset = node.get(OffsetStorageDocument.OFFSET).numberValue().longValue();
            String metadata = node.get(OffsetStorageDocument.METADATA).asText();
            String subscriptionId = node.get(OffsetStorageDocument.SUBSCRIPTION_ID).asText();
            return new OffsetStorageDocument(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, metadata), subscriptionId);
        }
    }
}
