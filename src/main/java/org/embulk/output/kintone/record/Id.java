package org.embulk.output.kintone.record;

public class Id {
  public static final String FIELD = "$id";
  private Long value;

  public void setValue(Long value) {
    this.value = value;
  }

  public Long getValue() {
    return value;
  }

  public boolean isPresent() {
    return value != null;
  }
}
