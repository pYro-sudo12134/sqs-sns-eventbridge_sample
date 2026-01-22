package org.example.service;

import org.example.config.LocalStackConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SqsServiceTest {

    @Mock
    private LocalStackConfig config;

    @Mock
    private SqsAsyncClient sqsAsyncClient;

    private SqsService sqsService;

    @BeforeEach
    void setUp() {
        when(config.getSqsAsyncClient()).thenReturn(sqsAsyncClient);
        sqsService = new SqsService(config);
    }

    @Test
    void shouldCreateQueueWithoutAttributes() {
        String queueName = "test-queue";
        String queueUrl = "http://localhost:4566/000000000000/test-queue";
        CreateQueueResponse response = CreateQueueResponse.builder()
                .queueUrl(queueUrl)
                .build();

        when(sqsAsyncClient.createQueue(any(CreateQueueRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<String> result = sqsService.createQueue(queueName);

        assertNotNull(result);
        assertEquals(queueUrl, result.join());
        verify(sqsAsyncClient).createQueue(any(CreateQueueRequest.class));
    }

    @Test
    void shouldCreateQueueWithAttributes() {
        String queueName = "test-queue";
        String queueUrl = "http://localhost:4566/000000000000/test-queue";
        Map<QueueAttributeName, String> attributes = Map.of(
                QueueAttributeName.VISIBILITY_TIMEOUT, "30"
        );

        CreateQueueResponse response = CreateQueueResponse.builder()
                .queueUrl(queueUrl)
                .build();

        when(sqsAsyncClient.createQueue(any(CreateQueueRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<String> result = sqsService.createQueue(queueName, attributes);

        assertNotNull(result);
        assertEquals(queueUrl, result.join());
        verify(sqsAsyncClient).createQueue(any(CreateQueueRequest.class));
    }

    @Test
    void shouldGetQueueArn() {
        String queueUrl = "http://localhost:4566/000000000000/test-queue";
        String queueArn = "arn:aws:sqs:us-east-1:000000000000:test-queue";

        GetQueueAttributesResponse response = GetQueueAttributesResponse.builder()
                .attributes(Map.of(QueueAttributeName.QUEUE_ARN, queueArn))
                .build();

        when(sqsAsyncClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<String> result = sqsService.getQueueArn(queueUrl);

        assertNotNull(result);
        assertEquals(queueArn, result.join());
        verify(sqsAsyncClient).getQueueAttributes(any(GetQueueAttributesRequest.class));
    }

    @Test
    void shouldSendMessage() {
        String queueUrl = "http://localhost:4566/000000000000/test-queue";
        String messageBody = "Test message";
        String messageId = "msg-123";

        SendMessageResponse response = SendMessageResponse.builder()
                .messageId(messageId)
                .build();

        when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<String> result = sqsService.sendMessage(queueUrl, messageBody);

        assertNotNull(result);
        assertEquals(messageId, result.join());
        verify(sqsAsyncClient).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void shouldReceiveMessages() {
        String queueUrl = "http://localhost:4566/000000000000/test-queue";
        Message message = Message.builder()
                .messageId("msg-123")
                .body("Test message")
                .receiptHandle("receipt-handle")
                .build();

        ReceiveMessageResponse response = ReceiveMessageResponse.builder()
                .messages(message)
                .build();

        when(sqsAsyncClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<List<Message>> result = sqsService.receiveMessages(queueUrl);

        assertNotNull(result);
        List<Message> messages = result.join();
        assertEquals(1, messages.size());
        assertEquals("msg-123", messages.get(0).messageId());
        verify(sqsAsyncClient).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    void shouldDeleteMessage() {
        String queueUrl = "http://localhost:4566/000000000000/test-queue";
        String receiptHandle = "receipt-handle";

        DeleteMessageResponse response = DeleteMessageResponse.builder().build();
        when(sqsAsyncClient.deleteMessage(any(DeleteMessageRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Void> result = sqsService.deleteMessage(queueUrl, receiptHandle);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(sqsAsyncClient).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void shouldProcessMessages() {
        String queueUrl = "http://localhost:4566/000000000000/test-queue";
        Message message = Message.builder()
                .messageId("msg-123")
                .body("Test message")
                .receiptHandle("receipt-handle")
                .build();

        ReceiveMessageResponse receiveResponse = ReceiveMessageResponse.builder()
                .messages(message)
                .build();

        DeleteMessageResponse deleteResponse = DeleteMessageResponse.builder().build();

        when(sqsAsyncClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(receiveResponse));
        when(sqsAsyncClient.deleteMessage(any(DeleteMessageRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(deleteResponse));

        CompletableFuture<Void> result = sqsService.processMessages(queueUrl);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(sqsAsyncClient).receiveMessage(any(ReceiveMessageRequest.class));
        verify(sqsAsyncClient).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void shouldHandleEmptyQueueWhenProcessing() {
        String queueUrl = "http://localhost:4566/000000000000/test-queue";

        ReceiveMessageResponse receiveResponse = ReceiveMessageResponse.builder()
                .messages(List.of())
                .build();

        when(sqsAsyncClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(receiveResponse));

        CompletableFuture<Void> result = sqsService.processMessages(queueUrl);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(sqsAsyncClient).receiveMessage(any(ReceiveMessageRequest.class));
        verify(sqsAsyncClient, never()).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void shouldPurgeQueue() {
        String queueUrl = "http://localhost:4566/000000000000/test-queue";
        PurgeQueueResponse response = PurgeQueueResponse.builder().build();

        when(sqsAsyncClient.purgeQueue(any(PurgeQueueRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Void> result = sqsService.purgeQueue(queueUrl);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(sqsAsyncClient).purgeQueue(any(PurgeQueueRequest.class));
    }

    @Test
    void shouldListQueues() {
        ListQueuesResponse response = ListQueuesResponse.builder()
                .queueUrls("http://localhost:4566/000000000000/test-queue")
                .build();

        when(sqsAsyncClient.listQueues())
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Void> result = sqsService.listQueues();

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(sqsAsyncClient).listQueues();
    }
}