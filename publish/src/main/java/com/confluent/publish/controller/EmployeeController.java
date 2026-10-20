package com.confluent.publish.controller;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.confluent.publish.service.EmployeeProducerService;
import org.apache.avro.generic.GenericRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.kafka.support.SendResult;

@RestController
@RequestMapping("/employees")
public class EmployeeController {

    private final EmployeeProducerService producerService;

    public EmployeeController(
            EmployeeProducerService producerService) {
        this.producerService = producerService;
    }

    public record EmployeeRequest(String name, Long id) {}

    public record PublishResponse(
            String status,
            String message,
            String topic,
            String employeeName,
            Long employeeId,
            Integer partition,
            Long offset
    ) {}

    @PostMapping
    public CompletableFuture<ResponseEntity<PublishResponse>>
    publishEmployee(@RequestBody EmployeeRequest request) {

        if (request == null
                || request.name() == null
                || request.name().isBlank()
                || request.id() == null
                || request.id() < 0) {

            return CompletableFuture.completedFuture(
                    ResponseEntity.badRequest().body(
                            new PublishResponse(
                                    "ERROR",
                                    "Valid name and non-negative id are required",
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                            )
                    )
            );
        }

        return producerService
                .publishEmployee(request.name(), request.id())
                .thenApply(result -> {
                    PublishResponse response = new PublishResponse(
                            "SUCCESS",
                            "Employee published successfully",
                            result.getRecordMetadata().topic(),
                            request.name(),
                            request.id(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset()
                    );

                    return ResponseEntity
                            .status(HttpStatus.CREATED)
                            .body(response);
                })
                .exceptionally(ex -> {
                    throw new CompletionException(ex);
                });
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Employee producer is running");
    }
}

