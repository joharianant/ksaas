package com.confluent.publish.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.api-key}")
    private String kafkaApiKey;

    @Value("${app.kafka.api-secret}")
    private String kafkaApiSecret;

    @Value("${app.schema-registry.url}")
    private String schemaRegistryUrl;

    @Value("${app.schema-registry.api-key}")
    private String schemaRegistryApiKey;

    @Value("${app.schema-registry.api-secret}")
    private String schemaRegistryApiSecret;

    @Bean
    public ProducerFactory<String, GenericRecord> producerFactory() {

        Map<String, Object> props = new HashMap<>();

        // Kafka broker
        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        // Serializers
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class
        );

        // Confluent Cloud authentication
        props.put(
                "security.protocol",
                "SASL_SSL"
        );
        props.put(
                SaslConfigs.SASL_MECHANISM,
                "PLAIN"
        );
        props.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"" + kafkaApiKey + "\" "
                        + "password=\"" + kafkaApiSecret + "\";"
        );

        // TLS
        props.put(
                SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                "https"
        );

        // Confluent Schema Registry
        props.put(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl
        );
        props.put(
                AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
                "USER_INFO"
        );
        props.put(
                AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,
                schemaRegistryApiKey + ":" + schemaRegistryApiSecret
        );

        // Use the existing registered schema
        props.put(
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                false
        );
        props.put(
                AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
                "io.confluent.kafka.serializers.subject.TopicNameStrategy"
        );

        // Producer reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, GenericRecord> kafkaTemplate(
            ProducerFactory<String, GenericRecord> producerFactory) {

        return new KafkaTemplate<>(producerFactory);
    }
}

