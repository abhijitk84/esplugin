package com.esplugins.plugin.models;

import com.esplugins.plugin.rescorer.RankerRescorer;
import java.util.Map;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.rescore.RescoreContext;

public class RankerContext  extends RescoreContext {

  private final boolean isRankingEnable;
  private final int windowSize;
  private final Fields fields;
  private final Map<String,Object> data;
  private final float boostingScore;
  private QueryShardContext queryShardContext;

  public RankerContext( boolean isRankingEnable,
      int windowSize,
      @Nullable  Fields fields,
      @Nullable  Map<String,Object> data,
      @Nullable  Float boostingScore,
      QueryShardContext queryShardContext) {
    super(windowSize, RankerRescorer.getInStance());
    this.isRankingEnable = isRankingEnable;
    this.windowSize = windowSize;
    this.fields = fields;
    this.data = data;
    this.boostingScore = boostingScore == null || boostingScore <=0 ? 0f : boostingScore;
    this.queryShardContext = queryShardContext;
  }

  public boolean isRankingEnable() {
    return isRankingEnable;
  }

  @Override
  public int getWindowSize() {
    return windowSize;
  }

  public Map<String, Object> getData() {
    return data;
  }

  public Fields getFields() {
    return fields;
  }

  public QueryShardContext getQueryShardContext() {
    return queryShardContext;
  }

  public float getBoostingScore() {
    return boostingScore;
  }
}
