package com.esplugins.plugin.rescorer.utils;

import com.esplugins.plugin.models.FieldInfo;
import com.esplugins.plugin.models.Source;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FieldUtils {

  public static List<FieldInfo> filterFieldInfoOnSource(Source source, List<FieldInfo> fieldInfos) {
    if (fieldInfos == null || fieldInfos.isEmpty()) {
      return Collections.emptyList();
    }
    return fieldInfos.stream()
        .filter(fieldInfo -> source == fieldInfo.getSource())
        .collect(Collectors.toList());
  }

}
