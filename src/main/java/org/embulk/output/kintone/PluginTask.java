package org.embulk.output.kintone;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

import java.util.Map;

public interface PluginTask
        extends Task
{
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
    public Map<String, KintoneColumnOption> getColumnOptions();

    @Config("mode")
    @ConfigDefault("insert")
    public String getMode();

    @Config("update_key")
    @ConfigDefault("null")
    public String getUpdateKeyName();
}
