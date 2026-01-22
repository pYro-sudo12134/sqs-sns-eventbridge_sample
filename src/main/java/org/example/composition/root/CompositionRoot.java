package org.example.composition.root;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.example.config.LocalStackConfig;
import org.example.service.EventBridgeService;
import org.example.service.SnsService;
import org.example.service.SqsService;

public class CompositionRoot extends AbstractModule {
    @Override
    public void configure() {
        bind(LocalStackConfig.class).in(Singleton.class);
        bind(EventBridgeService.class).in(Singleton.class);
        bind(SnsService.class).in(Singleton.class);
        bind(SqsService.class).in(Singleton.class);
    }
}