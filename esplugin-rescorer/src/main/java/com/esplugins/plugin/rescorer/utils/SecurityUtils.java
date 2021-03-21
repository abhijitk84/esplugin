package com.esplugins.plugin.rescorer.utils;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;

public class SecurityUtils {

  private static final Logger log = LogManager.getLogger(SecurityUtils.class.getName());

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
