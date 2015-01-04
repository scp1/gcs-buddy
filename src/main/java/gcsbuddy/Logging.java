package gcsbuddy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



/**
 * Simple wrappers for GCSBuddy's logging calls
 */
final class Logging {
  protected static Logger LOGGER = LogManager.getLogger(GCSBuddy.class.getName());

  public static void info(String msg, Object...args) {
	LOGGER.info(msg, args);
  }

  public static void warn(String msg, Object...args) {
    LOGGER.warn(msg, args);
  }

  public static void warn(Throwable throwable) {
    LOGGER.warn("", throwable);
  }
}
