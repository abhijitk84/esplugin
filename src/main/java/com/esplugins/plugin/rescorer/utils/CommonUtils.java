package com.esplugins.plugin.rescorer.utils;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class CommonUtils {

  private static final String DELIMITER = "_";

  public static String concat(String first,String second){
    return first + DELIMITER + second;
  }

  public static String concat(List<String> first,String second){
    if(isEmpty(first)) return second;
    return second + DELIMITER + StringUtils.join(first,DELIMITER);
  }


  public static boolean isEmpty(List<?> objects ){
    return objects == null || objects.isEmpty();
  }

  public static boolean isEmpty(Map<?,Object> map){
    return map == null || map.isEmpty();
  }

  public static Float covertToFloat(Object value){
    if(value == null) return null;
    if(value instanceof Double){
      return ((Double) value).floatValue();
    }
    if(value instanceof Integer){
      return ((Integer) value).floatValue();
    }
    if(value instanceof Long){
      return ((Long) value).floatValue();
    }
    return new Float(value.toString());
  }

}
