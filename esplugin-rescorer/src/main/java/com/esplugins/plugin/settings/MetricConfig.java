package com.esplugins.plugin.settings;


import java.util.Collections;
import java.util.List;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

public class MetricConfig {

  public static final String RIEMANN_HOST = "riemann.host";
  public static final String RAEANN_PORT = "riemann.port";
  public static final String RIEMANN_PREFIX = "riemann.prefix";
  public static final String RIEMANN_TAGS = "riemann.tags";
  private static final String RIEMANN_ATTRIBUTE = "riemann.attributes";

  public static final Setting<String> RIEMANN_HOST_SETTING =
      Setting.simpleString(RIEMANN_HOST, Property.NodeScope);

  public static final Setting<Integer> RIEMANN_PORT_SETTING =
      Setting.intSetting(RAEANN_PORT, 5555, Property.NodeScope);

  public static final Setting<String> RIEMANN_PREFIX_SETTING =
      Setting.simpleString(RIEMANN_PREFIX, "phonepe.stage.discovery.elastic-search",
          Property.NodeScope);

  public static final Setting<List<String>> RIEMANN_TAGS_SETTING = Setting
          .listSetting(RIEMANN_TAGS, Collections.emptyList(), String::valueOf, Property.NodeScope);

  public static final Setting.AffixSetting<String> RIEMANN_ATTRIBUTE_SETTING = Setting
      .prefixKeySetting(RIEMANN_ATTRIBUTE + ".",
          key -> Setting.simpleString(key,Property.NodeScope));

}
