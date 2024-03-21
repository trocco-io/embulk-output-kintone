package org.embulk.output.kintone;

import org.embulk.config.ConfigException;

public enum KintoneMode {
  INSERT("insert"),
  UPDATE("update"),
  UPSERT("upsert");
  private final String value;

  KintoneMode(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }

  public static KintoneMode getKintoneModeByValue(String value) {
    for (KintoneMode mode : values()) {
      if (mode.toString().equals(value)) {
        return mode;
      }
    }
    throw new ConfigException(String.format("Unknown mode '%s'", value));
  }
}
