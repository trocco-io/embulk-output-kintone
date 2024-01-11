package org.embulk.output.kintone;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface KintoneRetryOption extends Task {
  @Config("limit")
  @ConfigDefault("10")
  Integer getLimit();

  @Config("initial_wait_millis")
  @ConfigDefault("1000")
  Integer getInitialWaitMillis();

  @Config("max_wait_millis")
  @ConfigDefault("60000")
  Integer getMaxWaitMillis();
}
