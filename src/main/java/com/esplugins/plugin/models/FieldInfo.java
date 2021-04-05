package com.esplugins.plugin.models;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

public class FieldInfo implements Writeable {

  private static final ConstructingObjectParser<FieldInfo, Void> PARSER = new ConstructingObjectParser<>(
      "fields",
      args -> new FieldInfo((String) args[0], (String) args[1], (String) args[2],
          (List<String>) args[3], (float) args[4], (float) args[5]));

  static {
    PARSER.declareString(constructorArg(), new ParseField("name"));
    PARSER.declareString(optionalConstructorArg(), new ParseField("source"));
    PARSER.declareString(optionalConstructorArg(), new ParseField("index_name"));
    PARSER.declareStringArray(optionalConstructorArg(), new ParseField("id_identifiers"));
    PARSER.declareFloat(constructorArg(), new ParseField("default_value"));
    PARSER.declareFloat(constructorArg(), new ParseField("weight"));

  }

  private String name;
  private Source source;
  private String indexName;
  private List<String> idIdentifiers;
  private float defaultValue;
  private float weight;

  public FieldInfo(String name,
      String source,
      String indexName,
      List<String> idIdentifiers,
      float defaultValue,
      float weight) {
    this.defaultValue = defaultValue;
    this.source = Source.valueOf(source);
    this.indexName = indexName;
    this.idIdentifiers = idIdentifiers == null ? Lists.newArrayList() : idIdentifiers;
    this.name = name;
    this.weight = weight;
  }

  public FieldInfo(StreamInput in) throws IOException {
    this.name = in.readString();
    this.source = in.readEnum(Source.class);
    this.indexName = in.readOptionalString();
    this.idIdentifiers = in.readStringList();
    this.defaultValue = in.readFloat();
    this.weight = in.readFloat();
    System.out.println(this.toString());
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(name);
    out.writeEnum(source);
    out.writeOptionalString(indexName);
    out.writeStringCollection(idIdentifiers);
    out.writeFloat(defaultValue);
    out.writeFloat(weight);
  }



  public static FieldInfo fromXContent(XContentParser parser) {
    return PARSER.apply(parser, null);
  }

  public float getDefaultValue() {
    return defaultValue;
  }

  public List<String> getIdIdentifiers() {
    return idIdentifiers;
  }

  public float getWeight() {
    return weight;
  }

  public Source getSource() {
    return source;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "indxname=" +indexName + "weight="+weight + "source="+source + "name="+name
        +"default="+defaultValue + "idIdentifiers="+idIdentifiers;
  }
}
