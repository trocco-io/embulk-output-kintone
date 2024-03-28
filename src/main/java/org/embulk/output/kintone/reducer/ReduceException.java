package org.embulk.output.kintone.reducer;

public class ReduceException extends RuntimeException {
  public ReduceException(String message) {
    super(message);
  }

  public ReduceException(Throwable cause) {
    super(cause);
  }
}
