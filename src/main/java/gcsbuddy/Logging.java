package gcsbuddy;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple wrappers for GCSBuddy's logging calls
 */
final class Logging {
  private static Logger LOGGER = Logger.getLogger(GCSBuddy.class.getName());

  public static void info(String msg, Object...args) {
    LOGGER.log(Level.INFO, msg, args);
  }

  public static void warn(String msg, Object...args) {
    LOGGER.log(Level.WARNING, msg, args);
  }

  public static void warn(Throwable throwable) {
    LOGGER.log(Level.WARNING, "", throwable);
  }
}
