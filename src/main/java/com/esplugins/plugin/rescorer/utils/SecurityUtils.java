package com.esplugins.plugin.rescorer.utils;

import com.esplugins.plugin.metrics.RiemannMetricCollector;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import org.elasticsearch.SpecialPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityUtils {

  private static final Logger log = LoggerFactory.getLogger(RiemannMetricCollector.class);

  public static <T> T doPrivilegedException(PrivilegedExceptionAction<T> operation) {
    SpecialPermission.check();
    try {
      return AccessController.doPrivileged(operation);
    } catch (PrivilegedActionException e) {
      log.error("Privileged error ", e);
      throw new RuntimeException(e.getCause());
    }
  }

}
