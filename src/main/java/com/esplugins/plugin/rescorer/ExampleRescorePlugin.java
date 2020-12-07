package com.esplugins.plugin.rescorer;


import java.util.ArrayList;
import java.util.Collection;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;

import static java.util.Collections.singletonList;

public class ExampleRescorePlugin extends Plugin implements SearchPlugin {

  @Override
  public List<RescorerSpec<?>> getRescorers() {
    return singletonList(
        new RescorerSpec<>(ExampleRescoreBuilder.NAME, ExampleRescoreBuilder::new, ExampleRescoreBuilder::fromXContent));
  }

  @Override
  public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
    Collection<Class<? extends LifecycleComponent>> collections = new ArrayList<>();
    collections.add(DiscoveryClient.class);
    collections.add(ExampleRescoreBuilder.ExampleRescorer.class);
    return collections;
  }
}