package org.embulk.output.kintone;

import java.util.Collections;
import java.util.List;
import org.embulk.config.TaskSource;

public class KintoneColumnOptionBuilder {
  private String type;
  private String fieldCode;
  private String timezone;
  private String valueSeparator;

  public KintoneColumnOptionBuilder setType(String type) {
    this.type = type;
    return this;
  }

  public KintoneColumnOptionBuilder setFieldCode(String fieldCode) {
    this.fieldCode = fieldCode;
    return this;
  }

  public KintoneColumnOptionBuilder setTimezone(String timezone) {
    this.timezone = timezone;
    return this;
  }

  public KintoneColumnOptionBuilder setValueSeparator(String valueSeparator) {
    this.valueSeparator = valueSeparator;
    return this;
  }

  public KintoneColumnOption build() {
    return new KintoneColumnOption() {
      @Override
      public String getType() {
        return type;
      }

      @Override
      public String getFieldCode() {
        return fieldCode;
      }

      @Override
      public String getTimezone() {
        return timezone;
      }

      @Override
      public String getValueSeparator() {
        return valueSeparator;
      }

      @Override
      public List<KintoneSortColumn> getSortColumns() {
        return Collections.emptyList();
      }

      @Override
      public void validate() {}

      @Override
      public TaskSource dump() {
        return null;
      }
    };
  }
}
