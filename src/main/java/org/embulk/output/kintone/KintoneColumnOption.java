package org.embulk.output.kintone;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface KintoneColumnOption extends Task {
  @Config("type")
  String getType();

  @Config("field_code")
  String getFieldCode();

  @Config("timezone")
  @ConfigDefault("\"UTC\"")
  String getTimezone();

  @Config("val_sep")
  @ConfigDefault("\",\"")
  String getValueSeparator();
}
