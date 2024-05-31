package org.embulk.output.kintone.record;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum Skip {
  AUTO,
  NEVER,
  ALWAYS;

  @JsonCreator
  public static Skip of(String name) {
    return valueOf(name.toUpperCase());
  }
}
