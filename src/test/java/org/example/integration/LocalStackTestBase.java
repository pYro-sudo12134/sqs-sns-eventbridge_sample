package org.example.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public abstract class LocalStackTestBase {
    private static final Logger logger = LoggerFactory.getLogger(LocalStackTestBase.class);
    protected static GenericContainer<?> localStack;
    protected static String endpointOverride;

    @BeforeAll
    static void setupLocalStack() {
        localStack = new GenericContainer<>(
                DockerImageName.parse("localstack/localstack:latest")
        )
                .withEnv("SERVICES", "sqs,sns,events")
                .withEnv("AWS_DEFAULT_REGION", "us-east-1")
                .withEnv("EDGE_PORT", "4566")
                .withExposedPorts(4566)
                .waitingFor(Wait.forHttp("/_localstack/health")
                        .forStatusCode(200)
                        .forPath("/_localstack/health"));

        localStack.start();

        endpointOverride = String.format(
                "http://%s:%d",
                localStack.getHost(),
                localStack.getMappedPort(4566)
        );

        System.setProperty("LOCALSTACK_ENDPOINT", endpointOverride);
        System.setProperty("AWS_REGION", "us-east-1");
        System.setProperty("AWS_ACCESS_KEY", "test");
        System.setProperty("AWS_SECRET_KEY", "test");

        logger.info("Test endpoint set to {}", endpointOverride);
    }

    @AfterAll
    static void tearDown() {
        if (localStack != null) {
            localStack.stop();
        }
    }
}