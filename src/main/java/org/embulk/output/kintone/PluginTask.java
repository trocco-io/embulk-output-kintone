package org.embulk.output.kintone;

import java.util.Map;
import java.util.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface PluginTask extends Task {
  @Config("domain")
  String getDomain();

  @Config("app_id")
  int getAppId();

  @Config("guest_space_id")
  @ConfigDefault("null")
  Optional<Integer> getGuestSpaceId();

  @Config("token")
  @ConfigDefault("null")
  Optional<String> getToken();

  @Config("username")
  @ConfigDefault("null")
  Optional<String> getUsername();

  @Config("password")
  @ConfigDefault("null")
  Optional<String> getPassword();

  @Config("basic_auth_username")
  @ConfigDefault("null")
  Optional<String> getBasicAuthUsername();

  @Config("basic_auth_password")
  @ConfigDefault("null")
  Optional<String> getBasicAuthPassword();

  @Config("column_options")
  @ConfigDefault("{}")
  Map<String, KintoneColumnOption> getColumnOptions();

  @Config("mode")
  @ConfigDefault("\"insert\"")
  String getMode();

  @Config("update_key")
  @ConfigDefault("null")
  Optional<String> getUpdateKeyName();

  @Config("retry_options")
  @ConfigDefault("{}")
  KintoneRetryOption getRetryOptions();
}
