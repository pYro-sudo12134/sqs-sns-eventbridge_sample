package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.example.composition.root.CompositionRoot;
import org.example.config.LocalStackConfig;
import org.example.dto.IntegrationInfo;
import org.example.dto.QueueInfo;
import org.example.service.EventBridgeService;
import org.example.service.SnsService;
import org.example.service.SqsService;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Main {
    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new CompositionRoot());
        LocalStackConfig config = injector.getInstance(LocalStackConfig.class);
        EventBridgeService eventBridgeService = injector.getInstance(EventBridgeService.class);
        SnsService snsService = injector.getInstance(SnsService.class);
        SqsService sqsService = injector.getInstance(SqsService.class);

        ObjectMapper mapper = new ObjectMapper();

        try {
            demonstrateAllServices(eventBridgeService, snsService, sqsService, mapper)
                    .thenRun(() -> {
                        System.out.println("\nAll services demonstration completed");
                        config.shutdown();
                    })
                    .exceptionally(throwable -> {
                        System.err.println("Error in demonstration: " + throwable.getMessage());
                        config.shutdown();
                        return null;
                    })
                    .get();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            config.shutdown();
        }
    }

    private static CompletableFuture<Void> demonstrateAllServices(
            EventBridgeService ebs,
            SnsService sns,
            SqsService sqs,
            ObjectMapper mapper) {

        CompletableFuture<String> queueFuture = sqs.createQueue("demo-queue", Map.of())
                .thenApply(queueUrl -> {
                    System.out.println("1. Queue created: " + queueUrl);
                    return queueUrl;
                });

        CompletableFuture<QueueInfo> queueArnFuture = queueFuture.thenCompose(queueUrl ->
                sqs.getQueueArn(queueUrl)
                        .thenApply(queueArn -> {
                            System.out.println("   Queue ARN: " + queueArn);
                            return new QueueInfo(queueUrl, queueArn);
                        })
        );

        CompletableFuture<String> topicFuture = sns.createTopic("demo-topic")
                .thenApply(topicArn -> {
                    System.out.println("\n2. Topic created: " + topicArn);
                    return topicArn;
                });

        CompletableFuture<IntegrationInfo> subscriptionFuture = queueArnFuture.thenCombine(topicFuture, (queueInfo, topicArn) -> {
            System.out.println("\n3. Subscribing SQS to SNS");
            return sns.subscribeSqsToTopic(topicArn, queueInfo.arn())
                    .thenAccept(subArn -> System.out.println("   Subscription created: " + subArn))
                    .thenApply(v -> new IntegrationInfo(queueInfo, topicArn));
        }).thenCompose(future -> future);

        CompletableFuture<Void> eventBusFuture = ebs.createEventBus("demo-bus")
                .thenRun(() -> System.out.println("\n4. Event Bus created"));

        CompletableFuture<Void> ruleSnsFuture = subscriptionFuture.thenCombine(eventBusFuture, (integrationInfo, v) -> {
            System.out.println("\n5. Creating EventBridge rule with SNS target");
            return ebs.putEventWithSnsTarget(
                    "demo-bus",
                    "sns-target-rule",
                    integrationInfo.topicArn(),
                    "com.example.app",
                    "sns event"
            ).thenRun(() -> System.out.println("   Rule with SNS target created"));
        }).thenCompose(future -> future);

        CompletableFuture<Void> ruleSqsFuture = queueArnFuture.thenCompose(queueInfo ->
                ruleSnsFuture.thenCompose(v -> {
                    System.out.println("\n6. Creating EventBridge rule with SQS target");
                    return ebs.putEventWithSqsTarget(
                            "demo-bus",
                            "sqs-target-rule",
                            queueInfo.arn()
                    ).thenRun(() -> System.out.println("   Rule with SQS target created"));
                })
        );

        CompletableFuture<Void> sendEventToSnsFuture = ruleSqsFuture.thenCompose(v -> {
            System.out.println("\n7. Sending event to EventBridge (for SNS rule)");

            ObjectNode eventDetail = mapper.createObjectNode();
            eventDetail.put("orderId", "ORDER-001");
            eventDetail.put("customer", "Test User");
            eventDetail.put("amount", 100.0);
            eventDetail.put("currency", "USD");

            return ebs.sendEventToEventBridge(
                    "demo-bus",
                    "com.example.app",
                    "sns event",
                    eventDetail.toString()
            ).thenRun(() -> System.out.println("   Event sent (for SNS)"));
        });

        CompletableFuture<Void> sendEventToSqsFuture = sendEventToSnsFuture.thenCompose(v -> {
            System.out.println("\n8. Sending event to EventBridge (for SQS rule)");

            ObjectNode eventDetail = mapper.createObjectNode();
            eventDetail.put("type", "sqs-message");
            eventDetail.put("content", "Direct to SQS");

            return ebs.sendEventToEventBridge(
                    "demo-bus",
                    "com.example.app",
                    "sqs-target-rule",
                    eventDetail.toString()
            ).thenRun(() -> System.out.println("   Event sent (for SQS)"));
        });

        CompletableFuture<Void> checkMessagesFuture = queueArnFuture.thenCompose(queueInfo ->
                sendEventToSqsFuture.thenCompose(v -> {
                    System.out.println("\n9. Waiting for messages");
                    return CompletableFuture.runAsync(() -> {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }).thenCompose(ignored -> {
                        System.out.println("   Checking SQS for messages");
                        return sqs.processMessages(queueInfo.url());
                    });
                })
        );

        CompletableFuture<Void> directSnsWithoutSubjectFuture = topicFuture.thenCompose(topicArn ->
                checkMessagesFuture.thenCompose(v -> {
                    System.out.println("\n10. Sending direct message to SNS (without subject)");

                    ObjectNode snsMessage = mapper.createObjectNode();
                    snsMessage.put("type", "direct");
                    snsMessage.put("message", "Hello from SNS without subject!");

                    return sns.publishMessage(topicArn, snsMessage.toString())
                            .thenAccept(messageId -> System.out.println("   Direct message sent, ID: " + messageId));
                })
        );

        CompletableFuture<Void> directSnsWithSubjectFuture = topicFuture.thenCompose(topicArn ->
                directSnsWithoutSubjectFuture.thenCompose(v -> {
                    System.out.println("\n11. Sending direct message to SNS (with subject)");

                    ObjectNode snsMessage = mapper.createObjectNode();
                    snsMessage.put("type", "direct-with-subject");
                    snsMessage.put("message", "Hello from SNS with subject!");

                    return sns.publishMessage(topicArn, "Test Subject", snsMessage.toString())
                            .thenAccept(messageId -> System.out.println("   Direct message sent, ID: " + messageId));
                })
        );

        CompletableFuture<Void> directSqsFuture = queueArnFuture.thenCompose(queueInfo ->
                directSnsWithSubjectFuture.thenCompose(v -> {
                    System.out.println("\n12. Sending direct message to SQS");

                    ObjectNode sqsMessage = mapper.createObjectNode();
                    sqsMessage.put("type", "direct-sqs");
                    sqsMessage.put("content", "Hello directly to SQS!");

                    return sqs.sendMessage(queueInfo.url(), sqsMessage.toString())
                            .thenAccept(messageId -> System.out.println("   Direct SQS message sent, ID: " + messageId));
                })
        );

        CompletableFuture<Void> receiveWithSettingsFuture = queueArnFuture.thenCompose(queueInfo ->
                directSqsFuture.thenCompose(v -> {
                    System.out.println("\n13. Receiving messages with custom settings");
                    return sqs.receiveMessages(queueInfo.url(), 5, 10)
                            .thenAccept(messages -> {
                                System.out.println("   Received " + messages.size() + " messages");
                                messages.forEach(message -> {
                                    System.out.println("     Message ID: " + message.messageId());
                                    System.out.println("     Body: " + message.body());
                                });
                            });
                })
        );

        CompletableFuture<Void> deleteIndividualMessagesFuture = queueArnFuture.thenCompose(queueInfo ->
                receiveWithSettingsFuture.thenCompose(v -> {
                    System.out.println("\n14. Deleting individual messages");
                    return sqs.receiveMessages(queueInfo.url(), 2, 5)
                            .thenCompose(messages -> {
                                if (messages.isEmpty()) {
                                    System.out.println("   No messages to delete");
                                    return CompletableFuture.completedFuture(null);
                                }

                                return CompletableFuture.allOf(
                                        messages.stream()
                                                .map(message -> {
                                                    System.out.println("   Deleting message: " + message.messageId());
                                                    return sqs.deleteMessage(queueInfo.url(), message.receiptHandle());
                                                })
                                                .toArray(CompletableFuture[]::new)
                                );
                            });
                })
        );

        CompletableFuture<Void> listSnsSubscriptionsFuture = topicFuture.thenCompose(topicArn ->
                deleteIndividualMessagesFuture.thenCompose(v -> {
                    System.out.println("\n15. Listing SNS subscriptions");
                    return sns.listSubscriptions(topicArn);
                })
        );

        CompletableFuture<Void> listResourcesFuture = listSnsSubscriptionsFuture.thenCompose(v -> {
            System.out.println("\n16. Listing all resources");

            CompletableFuture<Void> listQueues = sqs.listQueues();
            CompletableFuture<Void> listTopics = sns.listTopics();
            CompletableFuture<Void> listRules = ebs.listRules("demo-bus");

            return CompletableFuture.allOf(listQueues, listTopics, listRules);
        });

        return listResourcesFuture.thenCompose(v -> queueArnFuture.thenCompose(queueInfo -> {
            System.out.println("\n17. Cleaning up");
            return sqs.purgeQueue(queueInfo.url())
                    .thenRun(() -> System.out.println("   Queue purged"));
        }));
    }
}