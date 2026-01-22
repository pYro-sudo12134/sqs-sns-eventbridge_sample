package org.example.service;

import org.example.config.LocalStackConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.*;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SnsServiceTest {

    @Mock
    private LocalStackConfig config;

    @Mock
    private SnsAsyncClient snsAsyncClient;

    private SnsService snsService;

    @BeforeEach
    void setUp() {
        when(config.getSnsAsyncClient()).thenReturn(snsAsyncClient);
        snsService = new SnsService(config);
    }

    @Test
    void shouldCreateTopicSuccessfully() {
        String topicName = "test-topic";
        String topicArn = "arn:aws:sns:test";
        CreateTopicResponse response = CreateTopicResponse.builder()
                .topicArn(topicArn)
                .build();

        when(snsAsyncClient.createTopic(any(CreateTopicRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<String> result = snsService.createTopic(topicName);

        assertNotNull(result);
        assertEquals(topicArn, result.join());
        verify(snsAsyncClient).createTopic(any(CreateTopicRequest.class));
    }

    @Test
    void shouldPublishMessageWithoutSubject() {
        String topicArn = "arn:aws:sns:test";
        String message = "Test message";
        String messageId = "msg-123";

        PublishResponse response = PublishResponse.builder()
                .messageId(messageId)
                .build();

        when(snsAsyncClient.publish(any(PublishRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<String> result = snsService.publishMessage(topicArn, message);

        assertNotNull(result);
        assertEquals(messageId, result.join());
        verify(snsAsyncClient).publish(any(PublishRequest.class));
    }

    @Test
    void shouldPublishMessageWithSubject() {
        String topicArn = "arn:aws:sns:test";
        String subject = "Test Subject";
        String message = "Test message";
        String messageId = "msg-123";

        PublishResponse response = PublishResponse.builder()
                .messageId(messageId)
                .build();

        when(snsAsyncClient.publish(any(PublishRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<String> result = snsService.publishMessage(topicArn, subject, message);

        assertNotNull(result);
        assertEquals(messageId, result.join());
        verify(snsAsyncClient).publish(any(PublishRequest.class));
    }

    @Test
    void shouldSubscribeSqsToTopic() {
        String topicArn = "arn:aws:sns:test";
        String queueArn = "arn:aws:sqs:test";
        String subscriptionArn = "arn:aws:sns:test:subscription";

        SubscribeResponse response = SubscribeResponse.builder()
                .subscriptionArn(subscriptionArn)
                .build();

        when(snsAsyncClient.subscribe(any(SubscribeRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<String> result = snsService.subscribeSqsToTopic(topicArn, queueArn);

        assertNotNull(result);
        assertEquals(subscriptionArn, result.join());
        verify(snsAsyncClient).subscribe(any(SubscribeRequest.class));
    }

    @Test
    void shouldListTopics() {
        ListTopicsResponse response = ListTopicsResponse.builder()
                .topics(Topic.builder().topicArn("arn:aws:sns:test").build())
                .build();

        when(snsAsyncClient.listTopics(any(ListTopicsRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Void> result = snsService.listTopics();

        assertNotNull(result);
        assertDoesNotThrow(result::join);
        verify(snsAsyncClient).listTopics(any(ListTopicsRequest.class));
    }

    @Test
    void shouldListSubscriptions() {
        String topicArn = "arn:aws:sns:test";
        ListSubscriptionsByTopicResponse response = ListSubscriptionsByTopicResponse.builder()
                .subscriptions(Subscription.builder()
                        .protocol("sqs")
                        .endpoint("arn:aws:sqs:test")
                        .build())
                .build();

        when(snsAsyncClient.listSubscriptionsByTopic(any(ListSubscriptionsByTopicRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Void> result = snsService.listSubscriptions(topicArn);

        assertNotNull(result);
        assertDoesNotThrow(result::join);
        verify(snsAsyncClient).listSubscriptionsByTopic(any(ListSubscriptionsByTopicRequest.class));
    }
}