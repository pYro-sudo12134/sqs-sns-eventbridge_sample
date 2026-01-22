package org.example.composition.root;

import com.google.inject.AbstractModule;
import org.example.config.LocalStackConfig;

public class CompositionRoot extends AbstractModule {
    @Override
    public void configure() {
        bind(LocalStackConfig.class).toProvider(LocalStackConfig::new);
    }
}