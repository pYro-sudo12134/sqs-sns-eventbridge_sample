package org.example.composition.root;

import com.google.inject.AbstractModule;
import org.example.config.LocalStackConfig;

public class CompositionRoot extends AbstractModule {
    @Override
    public void configure() {
        bind(LocalStackConfig.class).toProvider(() -> {
            String endpoint = System.getProperty("LOCALSTACK_ENDPOINT",
                    "http://localhost:4566");
            String region = System.getProperty("AWS_REGION", "us-east-1");
            String accessKey = System.getProperty("AWS_ACCESS_KEY", "test");
            String secretKey = System.getProperty("AWS_SECRET_KEY", "test");

            return new LocalStackConfig(endpoint, region, accessKey, secretKey);
        });
    }
}