package org.example.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.example.composition.root.CompositionRoot;
import org.example.service.EventBridgeService;
import org.example.service.SnsService;
import org.example.service.SqsService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EventBridgeIntegrationTest extends LocalStackTestBase {

    private static final String eventBusName = "test-event-bus";
    private static String queueUrl;
    private static String queueArn;
    private static String topicArn;

    @Test
    @Order(1)
    void shouldSetupEventBridgeFlow() {
        Injector injector = Guice.createInjector(new CompositionRoot());
        EventBridgeService ebService = injector.getInstance(EventBridgeService.class);
        SqsService sqsService = injector.getInstance(SqsService.class);
        SnsService snsService = injector.getInstance(SnsService.class);

        ebService.createEventBus(eventBusName)
                .orTimeout(10, TimeUnit.SECONDS)
                .join();

        queueUrl = sqsService.createQueue("eventbridge-test-queue")
                .orTimeout(10, TimeUnit.SECONDS)
                .join();
        queueArn = sqsService.getQueueArn(queueUrl)
                .orTimeout(10, TimeUnit.SECONDS)
                .join();

        topicArn = snsService.createTopic("eventbridge-test-topic")
                .orTimeout(10, TimeUnit.SECONDS)
                .join();

        ebService.putEventWithSnsTarget(
                eventBusName,
                "test-sns-rule",
                topicArn,
                "com.example.test",
                "test.event"
        ).orTimeout(10, TimeUnit.SECONDS).join();

        snsService.subscribeSqsToTopic(topicArn, queueArn)
                .orTimeout(10, TimeUnit.SECONDS)
                .join();
    }

    @Test
    @Order(2)
    void shouldRouteEventThroughEventBridgeToSqs() {
        Injector injector = Guice.createInjector(new CompositionRoot());
        EventBridgeService ebService = injector.getInstance(EventBridgeService.class);
        SqsService sqsService = injector.getInstance(SqsService.class);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode eventDetail = mapper.createObjectNode();
        eventDetail.put("testId", "EB-INTEGRATION-001");
        eventDetail.put("message", "EventBridge to SQS via SNS");

        ebService.sendEventToEventBridge(
                eventBusName,
                "com.example.test",
                "test.event",
                eventDetail.toString()
        ).orTimeout(10, TimeUnit.SECONDS).join();

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Message> messages = sqsService.receiveMessages(queueUrl, 1, 10)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .join();

            Assertions.assertFalse(messages.isEmpty(), "Событие не доставлено в SQS");
            String body = messages.get(0).body();

            Assertions.assertTrue(body.contains("EventBridge to SQS via SNS"));

            sqsService.deleteMessage(queueUrl, messages.get(0).receiptHandle())
                    .orTimeout(5, TimeUnit.SECONDS)
                    .join();
        });
    }
}