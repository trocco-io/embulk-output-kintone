package org.embulk.output.kintone;

import org.embulk.config.ConfigException;
import org.embulk.spi.Page;

public enum KintoneMode {
  INSERT("insert") {
    @Override
    public void validate(PluginTask task, KintoneClient client) {
      if (task.getUpdateKeyName().isPresent()) {
        throw new ConfigException("When mode is insert, require no update_key.");
      }
    }

    @Override
    public void add(Page page, KintonePageOutput output) {
      output.insertPage(page);
    }
  },
  UPDATE("update") {
    @Override
    public void validate(PluginTask task, KintoneClient client) {
      if (!task.getUpdateKeyName().isPresent()) {
        throw new ConfigException("When mode is update, require update_key.");
      }
      client.validateUpdateKey(task.getUpdateKeyName().orElse(null));
    }

    @Override
    public void add(Page page, KintonePageOutput output) {
      output.updatePage(page);
    }
  },
  UPSERT("upsert") {
    @Override
    public void validate(PluginTask task, KintoneClient client) {
      if (!task.getUpdateKeyName().isPresent()) {
        throw new ConfigException("When mode is upsert, require update_key.");
      }
      client.validateUpdateKey(task.getUpdateKeyName().orElse(null));
    }

    @Override
    public void add(Page page, KintonePageOutput output) {
      output.upsertPage(page);
    }
  };
  private final String value;

  KintoneMode(String value) {
    this.value = value;
  }

  public abstract void validate(PluginTask task, KintoneClient client);

  public abstract void add(Page page, KintonePageOutput output);

  @Override
  public String toString() {
    return value;
  }

  public static KintoneMode of(PluginTask task) {
    String value = task.getMode();
    for (KintoneMode mode : values()) {
      if (mode.toString().equals(value)) {
        return mode;
      }
    }
    throw new ConfigException(String.format("Unknown mode '%s'", value));
  }
}
