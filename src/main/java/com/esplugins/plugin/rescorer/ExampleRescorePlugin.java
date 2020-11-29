package com.esplugins.plugin.rescorer;


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
}