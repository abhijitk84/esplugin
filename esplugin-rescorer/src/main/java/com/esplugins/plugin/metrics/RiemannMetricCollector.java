package com.esplugins.plugin.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.riemann.DropWizardRiemannReporter;
import com.codahale.metrics.riemann.Riemann;
import com.esplugins.plugin.rescorer.utils.SecurityUtils;
import com.esplugins.plugin.settings.MetricConfig;
import io.appform.functionmetrics.FunctionMetricsManager;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RiemannMetricCollector extends AbstractLifecycleComponent {

  private DropWizardRiemannReporter reporter;
  private Riemann riemann;
  private static final Logger log = LoggerFactory.getLogger(RiemannMetricCollector.class);

  @Inject
  public RiemannMetricCollector(Settings settings) throws Exception {
    String host = System.getenv("HOST");
    if (host == null) {
      host = InetAddress.getLocalHost().getHostName();
    }
    riemann = new Riemann(settings.get(MetricConfig.RIEMANN_HOST),
        settings.getAsInt(MetricConfig.RAEANN_PORT,5555));
    MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate("metrics-registry");
    FunctionMetricsManager.initialize("metrics", metricRegistry);
    reporter = DropWizardRiemannReporter
        .forRegistry(metricRegistry)
        .prefixedWith(settings.get(MetricConfig.RIEMANN_PREFIX,
            "phonepe.stage.discovery.elastic-search"))
        .useSeparator(".")
        .localHost(host)
        .tags(settings.getAsList(MetricConfig.RIEMANN_TAGS))
        .attributes(MetricConfig.RIEMANN_ATTRIBUTE_SETTING.getAsMap(settings))
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build(riemann);
  }

  @Override
  protected void doStart() {
    log.info("riemann start begin ....");
    startRiemannWithPrivileged();
    log.info("riemann start end ....");
  }

  private void startRiemannWithPrivileged() {
      SecurityUtils.doPrivilegedException(() -> {
        reporter.start(30, TimeUnit.SECONDS);
        return 1;
      });
  }

  @Override
  protected void doStop() {
    try {
      if (reporter != null) {
        reporter.stop();
      }
      if (riemann != null) {
        riemann.close();
      }
    } catch (Exception e) {
      log.error("Riemann stop failed with exception ", e);
    }

  }

  @Override
  protected void doClose() {
  }
}
