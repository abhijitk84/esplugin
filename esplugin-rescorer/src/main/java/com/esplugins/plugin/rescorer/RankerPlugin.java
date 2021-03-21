package com.esplugins.plugin.rescorer;


import com.esplugins.plugin.metrics.RiemannMetricCollector;
import com.esplugins.plugin.settings.MetricConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;

import static java.util.Collections.singletonList;

public class RankerPlugin extends Plugin implements SearchPlugin {

  @Override
  public List<RescorerSpec<?>> getRescorers() {
    return singletonList(
        new RescorerSpec<>(RankerBuilder.NAME, RankerBuilder::new, RankerBuilder::fromXContent));
  }

  @Override
  public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
    Collection<Class<? extends LifecycleComponent>> collections = new ArrayList<>();
    collections.add(RiemannMetricCollector.class);
    collections.add(RankerRescorer.class);
    return collections;
  }

  @Override
  public List<Setting<?>> getSettings() {
    return Arrays.asList(MetricConfig.RIEMANN_HOST_SETTING,
        MetricConfig.RIEMANN_ATTRIBUTE_SETTING,
        MetricConfig.RIEMANN_PREFIX_SETTING,
        MetricConfig.RIEMANN_TAGS_SETTING,
        MetricConfig.RIEMANN_PORT_SETTING);
  }
}