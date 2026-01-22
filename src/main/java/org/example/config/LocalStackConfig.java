package org.example.config;

import com.google.inject.Singleton;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.net.URI;

@Singleton
public class LocalStackConfig {
    private static final String LOCALSTACK_ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";
    private static final String ACCESS_KEY = "test";
    private static final String SECRET_KEY = "test";

    private final EventBridgeAsyncClient eventBridgeAsyncClient;
    private final SnsAsyncClient snsAsyncClient;
    private final SqsAsyncClient sqsAsyncClient;

    public LocalStackConfig() {
        this.eventBridgeAsyncClient = createEventBridgeAsyncClient();
        this.snsAsyncClient = createSnsAsyncClient();
        this.sqsAsyncClient = createSqsAsyncClient();
    }

    public EventBridgeAsyncClient getEventBridgeAsyncClient() {
        return eventBridgeAsyncClient;
    }

    public SnsAsyncClient getSnsAsyncClient() {
        return snsAsyncClient;
    }

    public SqsAsyncClient getSqsAsyncClient() {
        return sqsAsyncClient;
    }

    private EventBridgeAsyncClient createEventBridgeAsyncClient() {
        return EventBridgeAsyncClient.builder()
                .endpointOverride(URI.create(LOCALSTACK_ENDPOINT))
                .region(Region.of(REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
                ))
                .build();
    }

    private SnsAsyncClient createSnsAsyncClient() {
        return SnsAsyncClient.builder()
                .endpointOverride(URI.create(LOCALSTACK_ENDPOINT))
                .region(Region.of(REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
                ))
                .build();
    }

    private SqsAsyncClient createSqsAsyncClient() {
        return SqsAsyncClient.builder()
                .endpointOverride(URI.create(LOCALSTACK_ENDPOINT))
                .region(Region.of(REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
                ))
                .build();
    }

    public void shutdown() {
        if (eventBridgeAsyncClient != null) {
            eventBridgeAsyncClient.close();
        }
        if (snsAsyncClient != null) {
            snsAsyncClient.close();
        }
        if (sqsAsyncClient != null) {
            sqsAsyncClient.close();
        }
    }
}