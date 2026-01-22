package org.example.service;

import org.example.config.LocalStackConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.eventbridge.model.CreateEventBusRequest;
import software.amazon.awssdk.services.eventbridge.model.CreateEventBusResponse;
import software.amazon.awssdk.services.eventbridge.model.ListRulesRequest;
import software.amazon.awssdk.services.eventbridge.model.ListRulesResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;
import software.amazon.awssdk.services.eventbridge.model.PutRuleRequest;
import software.amazon.awssdk.services.eventbridge.model.PutRuleResponse;
import software.amazon.awssdk.services.eventbridge.model.PutTargetsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutTargetsResponse;
import software.amazon.awssdk.services.eventbridge.model.Rule;
import software.amazon.awssdk.services.eventbridge.model.RuleState;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventBridgeServiceTest {

    @Mock
    private LocalStackConfig config;

    @Mock
    private EventBridgeAsyncClient eventBridgeAsyncClient;

    private EventBridgeService eventBridgeService;

    @BeforeEach
    void setUp() {
        when(config.getEventBridgeAsyncClient()).thenReturn(eventBridgeAsyncClient);
        eventBridgeService = new EventBridgeService(config);
    }

    @Test
    void shouldCreateEventBusSuccessfully() {
        String eventBusName = "test-bus";
        CreateEventBusResponse response = CreateEventBusResponse.builder().build();
        when(eventBridgeAsyncClient.createEventBus(any(CreateEventBusRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Void> result = eventBridgeService.createEventBus(eventBusName);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(eventBridgeAsyncClient).createEventBus(any(CreateEventBusRequest.class));
    }

    @Test
    void shouldHandleEventBusCreationException() {
        String eventBusName = "test-bus";
        RuntimeException exception = new RuntimeException("Event bus already exists");
        when(eventBridgeAsyncClient.createEventBus(any(CreateEventBusRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(exception));

        CompletableFuture<Void> result = eventBridgeService.createEventBus(eventBusName);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(eventBridgeAsyncClient).createEventBus(any(CreateEventBusRequest.class));
    }

    @Test
    void shouldPutEventWithSnsTarget() {
        String eventBusName = "test-bus";
        String ruleName = "test-rule";
        String snsTargetArn = "arn:aws:sns:test";
        String source = "test-source";
        String detailType = "test-detail-type";

        PutRuleResponse putRuleResponse = PutRuleResponse.builder().ruleArn("arn:aws:events:test").build();
        PutTargetsResponse putTargetsResponse = PutTargetsResponse.builder().build();

        when(eventBridgeAsyncClient.putRule(any(PutRuleRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(putRuleResponse));
        when(eventBridgeAsyncClient.putTargets(any(PutTargetsRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(putTargetsResponse));

        CompletableFuture<Void> result = eventBridgeService.putEventWithSnsTarget(
                eventBusName, ruleName, snsTargetArn, source, detailType);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(eventBridgeAsyncClient).putRule(any(PutRuleRequest.class));
        verify(eventBridgeAsyncClient).putTargets(any(PutTargetsRequest.class));
    }

    @Test
    void shouldPutEventWithSqsTarget() {
        String eventBusName = "test-bus";
        String ruleName = "test-rule";
        String sqsTargetArn = "arn:aws:sqs:test";

        PutRuleResponse putRuleResponse = PutRuleResponse.builder().ruleArn("arn:aws:events:test").build();
        PutTargetsResponse putTargetsResponse = PutTargetsResponse.builder().build();

        when(eventBridgeAsyncClient.putRule(any(PutRuleRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(putRuleResponse));
        when(eventBridgeAsyncClient.putTargets(any(PutTargetsRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(putTargetsResponse));

        CompletableFuture<Void> result = eventBridgeService.putEventWithSqsTarget(
                eventBusName, ruleName, sqsTargetArn);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(eventBridgeAsyncClient).putRule(any(PutRuleRequest.class));
        verify(eventBridgeAsyncClient).putTargets(any(PutTargetsRequest.class));
    }

    @Test
    void shouldSendEventToEventBridge() {
        String eventBusName = "test-bus";
        String source = "test-source";
        String detailType = "test-detail-type";
        String detail = "{\"key\": \"value\"}";

        PutEventsResponse response = PutEventsResponse.builder()
                .failedEntryCount(0)
                .build();

        when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Void> result = eventBridgeService.sendEventToEventBridge(
                eventBusName, source, detailType, detail);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(eventBridgeAsyncClient).putEvents(any(PutEventsRequest.class));
    }

    @Test
    void shouldHandleFailedEventSending() {
        String eventBusName = "test-bus";
        String source = "test-source";
        String detailType = "test-detail-type";
        String detail = "{\"key\": \"value\"}";

        PutEventsResponse response = PutEventsResponse.builder()
                .failedEntryCount(1)
                .entries(PutEventsResultEntry.builder()
                        .errorCode("TestError")
                        .errorMessage("Test error message")
                        .build())
                .build();

        when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Void> result = eventBridgeService.sendEventToEventBridge(
                eventBusName, source, detailType, detail);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(eventBridgeAsyncClient).putEvents(any(PutEventsRequest.class));
    }

    @Test
    void shouldListRules() {
        String eventBusName = "test-bus";
        ListRulesResponse response = ListRulesResponse.builder()
                .rules(Rule.builder()
                        .name("test-rule")
                        .state(RuleState.ENABLED)
                        .build())
                .build();

        when(eventBridgeAsyncClient.listRules(any(ListRulesRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Void> result = eventBridgeService.listRules(eventBusName);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.join());
        verify(eventBridgeAsyncClient).listRules(any(ListRulesRequest.class));
    }
}