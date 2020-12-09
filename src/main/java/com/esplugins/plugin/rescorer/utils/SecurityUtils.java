package com.esplugins.plugin.rescorer.utils;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import org.elasticsearch.SpecialPermission;

public class SecurityUtils {

    public static <T> T doPrivilegedException(PrivilegedExceptionAction<T> operation) throws Exception {
      SpecialPermission.check();
      try {
        return AccessController.doPrivileged(operation);
      } catch (PrivilegedActionException e) {
        throw (Exception) e.getCause();
      }
    }

}
