package com.esplugins.plugin.models;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

import java.util.List;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

public class Fields {

  private static final ConstructingObjectParser<Fields, Void> PARSER = new ConstructingObjectParser<>(
      "fields",
      args -> new Fields((List<FieldInfo>) args[0]));

  static {
    PARSER.declareObjectArray(constructorArg(), (parser, context) -> FieldInfo.fromXContent(parser),
        new ParseField("field_infos"));

  }

  private List<FieldInfo> fieldInfos;

  public Fields(List<FieldInfo> fieldInfos) {
    this.fieldInfos = fieldInfos;
  }

  public static Fields fromXContent(XContentParser parser) {
    return PARSER.apply(parser, null);
  }

  public List<FieldInfo> getFieldInfos() {
    return fieldInfos;
  }

}
