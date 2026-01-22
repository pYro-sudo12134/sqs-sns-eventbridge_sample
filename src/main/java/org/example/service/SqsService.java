package org.example.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.example.config.LocalStackConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Singleton
public class SqsService {
    private static final Logger logger = LoggerFactory.getLogger(SqsService.class);
    private final SqsAsyncClient sqsAsyncClient;

    @Inject
    public SqsService(LocalStackConfig config) {
        this.sqsAsyncClient = config.getSqsAsyncClient();
    }

    public CompletableFuture<String> createQueue(String queueName) {
        return createQueue(queueName, null);
    }

    public CompletableFuture<String> createQueue(String queueName, Map<QueueAttributeName, String> attributes) {
        CreateQueueRequest.Builder builder = CreateQueueRequest.builder()
                .queueName(queueName);

        if (attributes != null && !attributes.isEmpty()) {
            builder.attributes(attributes);
        }

        return sqsAsyncClient.createQueue(builder.build())
                .thenApply(response -> {
                    logger.info("Queue created: {}", queueName);
                    return response.queueUrl();
                });
    }

    public CompletableFuture<String> getQueueArn(String queueUrl) {
        GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .build();

        return sqsAsyncClient.getQueueAttributes(request)
                .thenApply(response -> response.attributes().get(QueueAttributeName.QUEUE_ARN));
    }

    public CompletableFuture<String> sendMessage(String queueUrl, String messageBody) {
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        return sqsAsyncClient.sendMessage(request)
                .thenApply(response -> {
                    logger.info("Message sent to SQS: {}", queueUrl);
                    return response.messageId();
                });
    }

    public CompletableFuture<List<Message>> receiveMessages(String queueUrl) {
        return receiveMessages(queueUrl, 10, 20);
    }

    public CompletableFuture<List<Message>> receiveMessages(String queueUrl, int maxMessages, int waitTimeSeconds) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxMessages)
                .waitTimeSeconds(waitTimeSeconds)
                .build();

        return sqsAsyncClient.receiveMessage(request)
                .thenApply(ReceiveMessageResponse::messages);
    }

    public CompletableFuture<Void> deleteMessage(String queueUrl, String receiptHandle) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();

        return sqsAsyncClient.deleteMessage(request)
                .thenAccept(response -> {
                    logger.info("Message deleted from queue");
                });
    }

    public CompletableFuture<Void> processMessages(String queueUrl) {
        return receiveMessages(queueUrl)
                .thenCompose(messages -> {
                    if (messages.isEmpty()) {
                        logger.info("No messages in queue: {}", queueUrl);
                        return CompletableFuture.completedFuture(null);
                    }

                    logger.info("Processing {} messages from queue: {}", messages.size(), queueUrl);

                    return CompletableFuture.allOf(
                            messages.stream()
                                    .map(message -> {
                                        logger.info("Received message: {}", message.body());
                                        logger.info("Message ID: {}", message.messageId());
                                        return deleteMessage(queueUrl, message.receiptHandle())
                                                .thenAccept(response ->
                                                        logger.info("SQS processing is finished")
                                                );
                                    })
                                    .toArray(CompletableFuture[]::new));
                });
    }

    public CompletableFuture<Void> purgeQueue(String queueUrl) {
        PurgeQueueRequest request = PurgeQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();

        return sqsAsyncClient.purgeQueue(request)
                .thenAccept(response -> {
                    logger.info("Queue purged: {}", queueUrl);
                })
                .exceptionally(throwable -> {
                    logger.warn("Could not purge queue: {}", throwable.getMessage());
                    return null;
                });
    }

    public CompletableFuture<Void> listQueues() {
        return sqsAsyncClient.listQueues()
                .thenAccept(response -> {
                    logger.info("Available SQS queues:");
                    if (response.queueUrls().isEmpty()) {
                        logger.info("  No queues found");
                    } else {
                        response.queueUrls().forEach(url -> {
                            logger.info("  - {}", url);
                        });
                    }
                });
    }
}