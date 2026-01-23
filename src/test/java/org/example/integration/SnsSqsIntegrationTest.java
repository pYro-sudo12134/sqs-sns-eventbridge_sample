package org.example.integration;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.example.composition.root.CompositionRoot;
import org.example.service.SnsService;
import org.example.service.SqsService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SnsSqsIntegrationTest extends LocalStackTestBase {

    private static String queueUrl;
    private static String queueArn;
    private static String topicArn;
    private static String subscriptionArn;
    private static Injector injector;
    private static SqsService sqsService;
    private static SnsService snsService;

    @BeforeAll
    static void setup() {
        injector = Guice.createInjector(new CompositionRoot());
        sqsService = injector.getInstance(SqsService.class);
        snsService = injector.getInstance(SnsService.class);
    }

    @Test
    @Order(1)
    void shouldCreateResources() {
        queueUrl = sqsService.createQueue("integration-test-queue")
                .orTimeout(10, TimeUnit.SECONDS)
                .join();
        Assertions.assertNotNull(queueUrl);
        Assertions.assertTrue(queueUrl.contains("integration-test-queue"));

        queueArn = sqsService.getQueueArn(queueUrl)
                .orTimeout(10, TimeUnit.SECONDS)
                .join();
        Assertions.assertNotNull(queueArn);

        topicArn = snsService.createTopic("integration-test-topic")
                .orTimeout(10, TimeUnit.SECONDS)
                .join();
        Assertions.assertNotNull(topicArn);

        subscriptionArn = snsService.subscribeSqsToTopic(topicArn, queueArn)
                .orTimeout(10, TimeUnit.SECONDS)
                .join();
        Assertions.assertNotNull(subscriptionArn);

        Assertions.assertTrue(topicArn.startsWith("arn:aws:sns:"), "Topic ARN follows the pattern of 'arn:aws:sns:'");
        System.out.println("Created Topic ARN: " + topicArn);
    }

    @Test
    @Order(2)
    void shouldDeliverMessageFromSnsToSqs() {
        Assertions.assertNotNull(topicArn, "Topic ARN не должен быть null");
        Assertions.assertTrue(topicArn.startsWith("arn:aws:sns:"),
                "Topic ARN should start with 'arn:aws:sns:'. Actual: " + topicArn);

        String messageBody = "{\"test\": \"SNS to SQS integration\"}";

        String messageId = snsService.publishMessage(topicArn, messageBody)
                .orTimeout(15, TimeUnit.SECONDS)
                .join();
        Assertions.assertNotNull(messageId);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Message> messages = sqsService.receiveMessages(queueUrl, 1, 5)
                    .orTimeout(5, TimeUnit.SECONDS)
                    .join();

            Assertions.assertFalse(messages.isEmpty(), "message is not delivered to SQS");

            Message message = messages.get(0);
            Assertions.assertTrue(message.body().contains("SNS to SQS integration"));

            sqsService.deleteMessage(queueUrl, message.receiptHandle())
                    .orTimeout(5, TimeUnit.SECONDS)
                    .join();
        });
    }

    @Test
    @Order(3)
    void shouldListCreatedResources() {
        Assertions.assertDoesNotThrow(() -> {
            sqsService.listQueues()
                    .orTimeout(5, TimeUnit.SECONDS)
                    .join();
        });

        Assertions.assertDoesNotThrow(() -> {
            snsService.listSubscriptions(topicArn)
                    .orTimeout(5, TimeUnit.SECONDS)
                    .join();
        });
    }

    @AfterAll
    static void cleanup() {
        if (queueUrl != null) {
            try {
                sqsService.purgeQueue(queueUrl)
                        .orTimeout(5, TimeUnit.SECONDS)
                        .join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}