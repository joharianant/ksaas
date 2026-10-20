package com.confluent.publish.service;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class EmployeeProducerService {

    private final KafkaTemplate<String, GenericRecord> kafkaTemplate;
    private final Schema employeeSchema;
    private final String topic;

    public EmployeeProducerService(
            KafkaTemplate<String, GenericRecord> kafkaTemplate,
            @Value("${app.kafka.topic}") String topic) {

        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.employeeSchema = loadSchema();
    }

    private Schema loadSchema() {
        try (InputStream input = getClass()
                .getResourceAsStream("/avro/employee.avsc")) {

            if (input == null) {
                throw new IllegalStateException(
                        "employee.avsc not found in /avro");
            }

            return new Schema.Parser().parse(input);

        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to load Employee Avro schema", e);
        }
    }

    public CompletableFuture<SendResult<String, GenericRecord>>
    publishEmployee(String name, long id) {

        GenericRecord employee =
                new GenericData.Record(employeeSchema);

        employee.put("name", name);
        employee.put("id", id);

        return kafkaTemplate.send(
                topic,
                String.valueOf(id),
                employee
        );
    }
}
