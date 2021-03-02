package com.esplugins.plugin.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.riemann.DropWizardRiemannReporter;
import com.codahale.metrics.riemann.Riemann;
import com.codahale.metrics.riemann.RiemannReporter;
import io.riemann.riemann.client.EventDSL;
import io.riemann.riemann.client.IRiemannClient;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomRiemannReported extends ScheduledReporter {
  private static final Logger log = LoggerFactory.getLogger(RiemannReporter.class);
  private final Riemann riemann;
  private final Clock clock;
  private final String prefix;
  private final String separator;
  private final String localHost;
  private final List<String> tags;
  private final Float ttl;
  private Map<String, String> attributes;
  private String appName;
  private final IRiemannClient client;

  public CustomRiemannReported(MetricRegistry registry, Riemann riemann, Clock clock, TimeUnit rateUnit, TimeUnit durationUnit, Float ttl, String prefix, String separator, String localHost, List<String> tags, Map<String, String> attributes, MetricFilter filter,
     IRiemannClient client ) {
    super(registry, "riemann-reporter", filter, rateUnit, durationUnit);
    this.riemann = riemann;
    this.clock = clock;
    this.prefix = prefix;
    this.separator = separator;
    this.localHost = localHost;
    this.tags = tags;
    this.ttl = ttl;
    this.appName = StringUtils.substringAfterLast(prefix, ".");
    this.attributes = attributes;
    this.client = client;
  }

  public void report(
      SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    long timestamp = this.clock.getTime() / 1000L;
    System.out.println("in report");
    log.debug("Reporting metrics: for {} at {}", timestamp, System.currentTimeMillis());

    try {
      this.riemann.connect();
      Iterator var8 = gauges.entrySet().iterator();

      Entry entry;
//      while(var8.hasNext()) {
//        entry = (Entry)var8.next();
//        this.reportGauge((String)entry.getKey(), (Gauge)entry.getValue(), timestamp);
//      }
//
//      var8 = counters.entrySet().iterator();
//
//      while(var8.hasNext()) {
//        entry = (Entry)var8.next();
//        this.reportCounter((String)entry.getKey(), (Counter)entry.getValue(), timestamp);
//      }
//
//      var8 = histograms.entrySet().iterator();
//
//      while(var8.hasNext()) {
//        entry = (Entry)var8.next();
//        this.reportHistogram((String)entry.getKey(), (Histogram)entry.getValue(), timestamp);
//      }

      var8 = meters.entrySet().iterator();

      while(var8.hasNext()) {
        entry = (Entry)var8.next();
        this.reportMetered((String)entry.getKey(), (Metered)entry.getValue(), timestamp);
      }

      var8 = timers.entrySet().iterator();

      while(var8.hasNext()) {
        entry = (Entry)var8.next();
        this.reportTimer((String)entry.getKey(), (Timer)entry.getValue(), timestamp);
      }

      log.trace("Flushing events to riemann");
      System.out.println("flushig it");
      client.flush();
      log.debug("Completed at {}", System.currentTimeMillis());
    } catch (Exception var10) {
      var10.printStackTrace();
      System.out.println("exceptiom in connectiom");
      log.warn("Unable to report to Riemann", this.riemann, var10);
    }

  }

  public void stop() {
    try {
      super.stop();
    } finally {
      try {
        this.riemann.close();
      } catch (IOException var7) {
        log.debug("Error disconnecting from Riemann", this.riemann, var7);
      }

    }

  }
  private interface EventClosure {
    EventDSL name(String... var1);
  }

  private void reportTimer(String name, Timer timer, long timestamp) {
    Snapshot snapshot = timer.getSnapshot();
    CustomRiemannReported.EventClosure reporter = this.newEvent(name, timestamp, timer.getClass().getSimpleName());
    reporter.name("max").metric(this.convertDuration((double)snapshot.getMax())).send();
    reporter.name("mean").metric(this.convertDuration(snapshot.getMean())).send();
    reporter.name("min").metric(this.convertDuration((double)snapshot.getMin())).send();
    reporter.name("stddev").metric(this.convertDuration(snapshot.getStdDev())).send();
    reporter.name("p50").metric(this.convertDuration(snapshot.getMedian())).send();
    reporter.name("p75").metric(this.convertDuration(snapshot.get75thPercentile())).send();
    reporter.name("p95").metric(this.convertDuration(snapshot.get95thPercentile())).send();
    reporter.name("p98").metric(this.convertDuration(snapshot.get98thPercentile())).send();
    reporter.name("p99").metric(this.convertDuration(snapshot.get99thPercentile())).send();
    reporter.name("p999").metric(this.convertDuration(snapshot.get999thPercentile())).send();
    this.reportMetered(name, timer, timestamp);
  }

  private CustomRiemannReported.EventClosure newEvent(final String metricName, final long timestamp, final String metricType) {
    final String prefix = this.prefix;
    final String separator = this.separator;
    return new CustomRiemannReported.EventClosure() {
      public EventDSL name(String... components) {
        EventDSL event = client.event();
        if (CustomRiemannReported.this.localHost != null) {
          event.host(CustomRiemannReported.this.localHost);
        }

        if (CustomRiemannReported.this.ttl != null) {
          event.ttl(CustomRiemannReported.this.ttl);
        }

        if (!CustomRiemannReported.this.tags.isEmpty()) {
          event.tags(CustomRiemannReported.this.tags);
        }

        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
          sb.append(prefix);
          sb.append(separator);
        }

        sb.append(metricName);
        String[] var4 = components;
        int var5 = components.length;

        for(int var6 = 0; var6 < var5; ++var6) {
          String part = var4[var6];
          sb.append(separator);
          sb.append(part);
        }

        event.service(sb.toString());
        event.time(timestamp);
        event.attribute("metric-type", metricType);
        if (!CustomRiemannReported.this.tags.isEmpty()) {
          event.attribute("app", (String)CustomRiemannReported.this.tags.get(0));
        } else {
          event.attribute("app", CustomRiemannReported.this.appName);
        }

        if (CustomRiemannReported.this.attributes != null && !CustomRiemannReported.this.attributes.isEmpty()) {
          event.attributes(CustomRiemannReported.this.attributes);
        }

        return event;
      }
    };
  }

  private void reportMetered(String name, Metered meter, long timestamp) {
    CustomRiemannReported.EventClosure reporter = this.newEvent(name, timestamp, meter.getClass().getSimpleName());
    System.out.println(reporter.name("count").metric(meter.getCount()));
    System.out.println(meter);
    try {
      reporter.name("count").metric(meter.getCount()).send();
      reporter.name("m1_rate").metric(this.convertRate(meter.getOneMinuteRate())).send();
      reporter.name("m5_rate").metric(this.convertRate(meter.getFiveMinuteRate())).send();
      reporter.name("m15_rate").metric(this.convertRate(meter.getFifteenMinuteRate())).send();
      reporter.name("mean_rate").metric(this.convertRate(meter.getMeanRate())).send();
    }catch (Exception e){
      e.printStackTrace();
    }
  }



}
