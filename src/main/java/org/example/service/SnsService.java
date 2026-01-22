package org.example.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.example.config.LocalStackConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.ListSubscriptionsByTopicRequest;
import software.amazon.awssdk.services.sns.model.ListTopicsRequest;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;

import java.util.concurrent.CompletableFuture;

@Singleton
public class SnsService {
    private static final Logger logger = LoggerFactory.getLogger(SnsService.class);
    private final SnsAsyncClient snsAsyncClient;

    @Inject
    public SnsService(LocalStackConfig config) {
        this.snsAsyncClient = config.getSnsAsyncClient();
    }

    public CompletableFuture<String> createTopic(String topicName) {
        CreateTopicRequest request = CreateTopicRequest.builder()
                .name(topicName)
                .build();

        return snsAsyncClient.createTopic(request)
                .thenApply(response -> {
                    logger.info("Topic created: {} (ARN: {})", topicName, response.topicArn());
                    return response.topicArn();
                });
    }

    public CompletableFuture<String> publishMessage(String topicArn, String message) {
        return publishMessage(topicArn, null, message);
    }

    public CompletableFuture<String> publishMessage(String topicArn, String subject, String message) {
        PublishRequest.Builder requestBuilder = PublishRequest.builder()
                .topicArn(topicArn)
                .message(message);

        if (subject != null && !subject.isEmpty()) {
            requestBuilder.subject(subject);
        }

        return snsAsyncClient.publish(requestBuilder.build())
                .thenApply(response -> {
                    logger.info("Message published to {}: {}", topicArn, response.messageId());
                    return response.messageId();
                });
    }

    public CompletableFuture<String> subscribeSqsToTopic(String topicArn, String queueArn) {
        SubscribeRequest request = SubscribeRequest.builder()
                .topicArn(topicArn)
                .protocol("sqs")
                .endpoint(queueArn)
                .build();

        return snsAsyncClient.subscribe(request)
                .thenApply(response -> {
                    logger.info("SQS subscribed to topic. SubscriptionArn: {}", response.subscriptionArn());
                    return response.subscriptionArn();
                });
    }

    public CompletableFuture<Void> listTopics() {
        ListTopicsRequest request = ListTopicsRequest.builder().build();

        return snsAsyncClient.listTopics(request)
                .thenAccept(response -> {
                    logger.info("Available SNS topics:");
                    response.topics().forEach(topic -> logger.info("  - {}", topic.topicArn()));
                });
    }

    public CompletableFuture<Void> listSubscriptions(String topicArn) {
        ListSubscriptionsByTopicRequest request = ListSubscriptionsByTopicRequest.builder()
                .topicArn(topicArn)
                .build();

        return snsAsyncClient.listSubscriptionsByTopic(request)
                .thenAccept(response -> {
                    logger.info("Subscriptions for topic {}:", topicArn);
                    response.subscriptions().forEach(subscription ->
                            logger.info("  - {}: {}", subscription.protocol(), subscription.endpoint()));
                });
    }
}