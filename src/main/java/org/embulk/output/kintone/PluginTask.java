package org.embulk.output.kintone;

import java.util.List;
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

  @Config("prefer_nulls")
  @ConfigDefault("\"false\"")
  boolean getPreferNulls();

  @Config("ignore_nulls")
  @ConfigDefault("\"false\"")
  boolean getIgnoreNulls();

  @Config("mode")
  @ConfigDefault("\"insert\"")
  String getMode();

  @Config("update_key")
  @ConfigDefault("null")
  Optional<String> getUpdateKeyName();

  @Config("reduce_key")
  @ConfigDefault("null")
  Optional<String> getReduceKeyName();

  @Config("sort_columns")
  @ConfigDefault("[]")
  List<KintoneSortColumn> getSortColumns();

  @Config("max_sort_tmp_files")
  @ConfigDefault("null")
  Optional<Integer> getMaxSortTmpFiles();

  @Config("max_sort_memory")
  @ConfigDefault("null")
  Optional<Long> getMaxSortMemory();

  @Config("chunk_size")
  @ConfigDefault("100")
  Integer getChunkSize();

  @Config("retry_options")
  @ConfigDefault("{}")
  KintoneRetryOption getRetryOptions();
}
