package org.example.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.example.config.LocalStackConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.eventbridge.model.CreateEventBusRequest;
import software.amazon.awssdk.services.eventbridge.model.ListRulesRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutRuleRequest;
import software.amazon.awssdk.services.eventbridge.model.PutRuleResponse;
import software.amazon.awssdk.services.eventbridge.model.PutTargetsRequest;
import software.amazon.awssdk.services.eventbridge.model.RuleState;
import software.amazon.awssdk.services.eventbridge.model.Target;

import java.util.concurrent.CompletableFuture;

@Singleton
public class EventBridgeService {
    private static final Logger logger = LoggerFactory.getLogger(EventBridgeService.class);
    private final EventBridgeAsyncClient eventBridgeAsyncClient;

    @Inject
    public EventBridgeService(LocalStackConfig config) {
        this.eventBridgeAsyncClient = config.getEventBridgeAsyncClient();
    }

    public CompletableFuture<Void> createEventBus(String eventBusName) {
        CreateEventBusRequest request = CreateEventBusRequest.builder()
                .name(eventBusName)
                .build();

        return eventBridgeAsyncClient.createEventBus(request)
                .thenAccept(response -> logger.info("EventBus created: {}", eventBusName))
                .exceptionally(throwable -> {
                    logger.warn("EventBus might already exist: {}", throwable.getMessage());
                    return null;
                });
    }

    public CompletableFuture<Void> putEventWithSnsTarget(String eventBusName, String ruleName,
                                                         String snsTargetArn, String source, String detailType) {
        String pattern = String.format(
                "{\"source\":[\"%s\"],\"detail-type\":[\"%s\"]}",
                source, detailType
        );

        PutRuleRequest ruleRequest = PutRuleRequest.builder()
                .eventBusName(eventBusName)
                .name(ruleName)
                .eventPattern(pattern)
                .state(RuleState.ENABLED)
                .build();

        CompletableFuture<PutRuleResponse> ruleFuture = eventBridgeAsyncClient.putRule(ruleRequest);

        return ruleFuture.thenCompose(ruleResponse -> {
            logger.info("Rule created: {}", ruleName);

            Target snsTarget = Target.builder()
                    .id("SnsTarget-" + ruleName)
                    .arn(snsTargetArn)
                    .build();

            PutTargetsRequest targetsRequest = PutTargetsRequest.builder()
                    .eventBusName(eventBusName)
                    .rule(ruleName)
                    .targets(snsTarget)
                    .build();

            return eventBridgeAsyncClient.putTargets(targetsRequest)
                    .thenAccept(targetsResponse ->
                            logger.info("SNS target added to rule: {}", ruleName));
        });
    }

    public CompletableFuture<Void> putEventWithSqsTarget(String eventBusName, String ruleName,
                                                         String sqsTargetArn) {
        String pattern = String.format(
                "{\"source\":[\"com.example.app\"],\"detail-type\":[\"%s\"]}",
                ruleName.replace("-", " ")
        );

        PutRuleRequest ruleRequest = PutRuleRequest.builder()
                .eventBusName(eventBusName)
                .name(ruleName)
                .eventPattern(pattern)
                .state(RuleState.ENABLED)
                .build();

        CompletableFuture<PutRuleResponse> ruleFuture = eventBridgeAsyncClient.putRule(ruleRequest);

        return ruleFuture.thenCompose(ruleResponse -> {
            logger.info("Rule created: {}", ruleName);

            Target sqsTarget = Target.builder()
                    .id("SqsTarget-" + ruleName)
                    .arn(sqsTargetArn)
                    .inputPath("$.detail")
                    .build();

            PutTargetsRequest targetsRequest = PutTargetsRequest.builder()
                    .eventBusName(eventBusName)
                    .rule(ruleName)
                    .targets(sqsTarget)
                    .build();

            return eventBridgeAsyncClient.putTargets(targetsRequest)
                    .thenAccept(targetsResponse -> logger.info("SQS target added to rule: {}", ruleName));
        });
    }

    public CompletableFuture<Void> sendEventToEventBridge(String eventBusName, String source,
                                                          String detailType, String detail) {
        PutEventsRequestEntry entry = PutEventsRequestEntry.builder()
                .eventBusName(eventBusName)
                .source(source)
                .detailType(detailType)
                .detail(detail)
                .build();

        PutEventsRequest request = PutEventsRequest.builder()
                .entries(entry)
                .build();

        return eventBridgeAsyncClient.putEvents(request)
                .thenAccept(response -> {
                    if (response.failedEntryCount() > 0) {
                        logger.error("Failed to send event to EventBridge");
                        response.entries().forEach(entryResult -> {
                            if (entryResult.errorCode() != null) {
                                logger.error("Error: {} - {}",
                                        entryResult.errorCode(),
                                        entryResult.errorMessage()
                                );
                            }
                        });
                    } else {
                        logger.info("Event sent to EventBridge: {} - {}",
                                detailType, detail);
                    }
                });
    }

    public CompletableFuture<Void> listRules(String eventBusName) {
        ListRulesRequest request = ListRulesRequest.builder()
                .eventBusName(eventBusName)
                .build();

        return eventBridgeAsyncClient.listRules(request)
                .thenAccept(response -> {
                    logger.info("Rules for event bus '{}':", eventBusName);
                    response.rules().forEach(rule ->
                            logger.info("  - {} (State: {})", rule.name(), rule.state())
                    );
                });
    }
}