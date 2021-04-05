package com.esplugins.plugin.models;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

import java.io.IOException;
import java.util.List;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;


public class Fields implements Writeable {

  private static final ConstructingObjectParser<Fields, Void> PARSER = new ConstructingObjectParser<>(
      "fields",
      args -> new Fields((List<FieldInfo>) args[0],(int)args[1]));

  static {
    PARSER.declareObjectArray(constructorArg(), (parser, context) -> FieldInfo.fromXContent(parser),
        new ParseField("field_infos"));
    PARSER.declareInt(optionalConstructorArg(), new ParseField("request_time_out"));

  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
     out.writeList(fieldInfos);
     out.writeOptionalInt(requestTimeout);
  }

  public Fields(StreamInput in) throws IOException{
    this.fieldInfos = in.readList(FieldInfo::new);
    this.requestTimeout = in.readOptionalInt();
  }

  public static Fields readFrom(StreamInput in) throws IOException {
    return new Fields(in.readList(FieldInfo::new),in.readOptionalInt());
  }

  private List<FieldInfo> fieldInfos;

  private int requestTimeout; // In MilliSec

  public Fields(List<FieldInfo> fieldInfos,int requestTimeout) {
    this.fieldInfos = fieldInfos;
    this.requestTimeout = requestTimeout <= 0 ? 300 : requestTimeout;
  }

  public static Fields fromXContent(XContentParser parser) {
    return PARSER.apply(parser, null);
  }

  public List<FieldInfo> getFieldInfos() {
    return fieldInfos;
  }

  public int getRequestTimeout() {
    return requestTimeout;
  }

}
