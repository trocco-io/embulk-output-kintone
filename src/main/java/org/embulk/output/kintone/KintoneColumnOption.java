package org.embulk.output.kintone;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface KintoneColumnOption
        extends Task
{
    @Config("type")
    public String getType();

    @Config("field_code")
    public String getFieldCode();

    @Config("update_key")
    @ConfigDefault("false")
    public boolean getUpdateKey();
}
