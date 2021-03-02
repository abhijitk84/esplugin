package com.esplugins.plugin.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.riemann.DropWizardRiemannReporter;
import com.codahale.metrics.riemann.Riemann;
import com.esplugins.plugin.rescorer.utils.SecurityUtils;
import com.google.common.collect.Maps;
import io.riemann.riemann.client.IRiemannClient;
import io.riemann.riemann.client.RiemannBatchClient;
import io.riemann.riemann.client.RiemannClient;
import io.riemann.riemann.client.UnsupportedJVMException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RiemannMetricCollector extends AbstractLifecycleComponent {

  private DropWizardRiemannReporter reporter;
  private ConsoleReporter consoleReporter;
  private CustomRiemannReported customRiemannReporter;
  private Riemann riemann;
  private static final Logger log = LoggerFactory.getLogger(RiemannMetricCollector.class);


  @Inject
  public RiemannMetricCollector(Settings settings) throws Exception {
    String host = System.getenv("HOST");
    if (host == null) {
      host = InetAddress.getLocalHost().getHostName();
    }
    log.error("Testinng slf4j log");
    IRiemannClient client = getClient("stg-riemannapp001.phonepe.nb6", 5555,10);
    riemann = new Riemann(client);
    MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate("metrics-registry");
    reporter = DropWizardRiemannReporter
        .forRegistry(metricRegistry)
        .prefixedWith("elastic-search")
        .useSeparator(".")
        .localHost(host)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build(riemann);
    customRiemannReporter = new CustomRiemannReported(
        metricRegistry,
        riemann,
        Clock.defaultClock(),
        TimeUnit.SECONDS,
        TimeUnit.MILLISECONDS,
        null,
       "phonepe.stage.elastic-search",
        ".",
          host,
        new ArrayList<>(),
        Maps.newHashMap(),
        MetricFilter.ALL,
        client);

    consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
        .convertDurationsTo(TimeUnit.MICROSECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build();
  }

  private static IRiemannClient getClient(String host, Integer port, int batchSize) throws IOException {
    RiemannClient c = RiemannClient.tcp(host, port);

    try {
      return new RiemannBatchClient(c, batchSize);
    } catch (UnsupportedJVMException var5) {
      return c;
    }
  }

  @Override
  protected void doStart() {
    reporter.start(30, TimeUnit.SECONDS);
    consoleReporter.start(30,TimeUnit.SECONDS);
    getClient();
    System.out.println("In riemannn Start ");
  }

  private void getClient(){
    try {
      SecurityUtils.doPrivilegedException(()->{
        customRiemannReporter.start(30,TimeUnit.SECONDS);
        return 1;
      });
    }catch (Exception e){
      System.out.println("customerpsjkdjsdj");
      e.printStackTrace();
    }

  }

  @Override
  protected void doStop() {
    System.out.println("In riemannn Stop");
    try {
      if (reporter != null) {
        reporter.stop();
      }
      if (riemann != null) {
        riemann.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Override
  protected void doClose() {
    System.out.println("In close");
  }
}