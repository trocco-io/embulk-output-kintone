package org.embulk.output.kintone;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

import java.util.Optional;

public interface KintoneColumnOption
        extends Task
{
    @Config("type")
    public String getType();

    @Config("field_code")
    public String getFieldCode();

    @Config("timezone")
    @ConfigDefault("\"UTC\"")
    public Optional<String> getTimezone();

    @Config("update_key")
    @ConfigDefault("false")
    public boolean getUpdateKey();

    @Config("val_sep")
    @ConfigDefault("\",\"")
    public String getValueSeparator();
}
