package org.embulk.output.kintone.util;

public abstract class Lazy<T extends AutoCloseable> implements AutoCloseable {
  private T value;

  public T get() {
    if (value == null) {
      value = initialValue();
    }
    return value;
  }

  public void close() throws Exception {
    if (value != null) {
      value.close();
    }
  }

  protected abstract T initialValue();
}
