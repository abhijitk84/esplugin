package com.esplugins.plugin.rescorer;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

import com.esplugins.plugin.models.Fields;
import com.esplugins.plugin.models.RankerContext;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.RescorerBuilder;

public class RankerBuilder extends RescorerBuilder<RankerBuilder> {

  public static final String NAME = "rank";
  private static final Logger logger = LogManager.getLogger(RankerBuilder.class.getName());
  private static final ParseField IS_RANKING_ENABLE = new ParseField("is_ranking_enable");
  private static final ParseField FIELDS = new ParseField("fields");
  private static final ParseField DATA = new ParseField("data");
  private static final ConstructingObjectParser<RankerBuilder, Void> PARSER = new ConstructingObjectParser<>(
      NAME,
      args -> new RankerBuilder((boolean) args[0], (Fields) args[1], (Map<String, Object>) args[2]));

  static {
    PARSER.declareBoolean(constructorArg(), IS_RANKING_ENABLE);
    PARSER.declareField(optionalConstructorArg(), (parser, context) -> Fields.fromXContent(parser),
        FIELDS, ObjectParser.ValueType.OBJECT);
    PARSER.declareField(optionalConstructorArg(), XContentParser::map, DATA,
        ObjectParser.ValueType.OBJECT);
  }

  private final boolean isRankingEnable;
  private final Fields fields;
  private final Map<String, Object> data;

  public RankerBuilder(boolean isRankingEnable,
      @Nullable Fields fields,
      @Nullable Map<String, Object> data) {
    this.isRankingEnable = isRankingEnable;
    this.fields = fields;
    this.data = data;
  }

  public RankerBuilder(StreamInput in) throws IOException {
    super(in);
    this.isRankingEnable = in.readBoolean();
    this.fields = (Fields) in.readGenericValue();
    this.data = in.readMap();
  }

  public static RankerBuilder fromXContent(XContentParser parser) {
    return PARSER.apply(parser, null);
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeBoolean(isRankingEnable);
    out.writeGenericValue(fields);
    out.writeMap(data);
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }

  @Override
  public RescorerBuilder<RankerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
    return this;
  }

  @Override
  protected void doXContent(XContentBuilder builder, Params params) throws IOException {
    builder.field(IS_RANKING_ENABLE.getPreferredName(), isRankingEnable);
    if (FIELDS != null) {
      builder.field(FIELDS.getPreferredName(), fields);
    }
    if (DATA != null) {
      builder.field(DATA.getPreferredName(), data);
    }
  }

  @Override
  public RescoreContext innerBuildContext(int windowSize, QueryShardContext context) throws IOException {
    return new RankerContext(isRankingEnable, windowSize, fields, data, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    RankerBuilder other = (RankerBuilder) obj;
    return isRankingEnable == other.isRankingEnable
        && Objects.equals(windowSize, other.windowSize)
        && Objects.equals(data, other.data)
        && Objects.equals(fields, other.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), isRankingEnable, windowSize);
  }

}