package gcsbuddy;

import java.io.Serializable;

public class RetriesExhaustedException extends RuntimeException implements Serializable {

  private static final long serialVersionUID = -2594059122419022555L;

  public RetriesExhaustedException(String message) {
    super(message);
  }

  public RetriesExhaustedException(Throwable e) {
    super(e);
  }

  public RetriesExhaustedException(String message, Throwable cause) {
    super(message, cause);
  }

}
