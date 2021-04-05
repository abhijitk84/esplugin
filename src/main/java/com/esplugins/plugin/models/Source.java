package com.esplugins.plugin.models;

import java.io.IOException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public enum Source implements Writeable {
  PRIMARY_INDEX,SECONDARY_INDEX,REQUEST;

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeVInt(this.ordinal());
  }
}
