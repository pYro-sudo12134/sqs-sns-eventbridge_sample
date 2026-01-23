package org.example.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class LocalStackConfigTest {

    private LocalStackConfig config;

    @BeforeEach
    void setUp() {
        config = new LocalStackConfig(
                "http://localhost:4566",
                "us-east-1",
                "test",
                "test"
        );
    }

    @Test
    void shouldCreateEventBridgeAsyncClient() {
        EventBridgeAsyncClient client = config.getEventBridgeAsyncClient();
        assertNotNull(client);
    }

    @Test
    void shouldCreateSnsAsyncClient() {
        SnsAsyncClient client = config.getSnsAsyncClient();
        assertNotNull(client);
    }

    @Test
    void shouldCreateSqsAsyncClient() {
        SqsAsyncClient client = config.getSqsAsyncClient();
        assertNotNull(client);
    }

    @Test
    void shouldReturnSameInstanceForSameClient() {
        EventBridgeAsyncClient client1 = config.getEventBridgeAsyncClient();
        EventBridgeAsyncClient client2 = config.getEventBridgeAsyncClient();
        assertEquals(client1, client2);
    }

    @Test
    void shouldShutdownClientsWithoutException() {
        assertDoesNotThrow(() -> config.shutdown());
    }
}