package org.embulk.output.kintone;

import java.util.function.Consumer;
import org.embulk.config.ConfigException;
import org.embulk.output.kintone.record.Id;
import org.embulk.output.kintone.record.Skip;
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
    public void add(Page page, Skip skip, KintonePageOutput output) {
      output.insertPage(page);
    }
  },
  UPDATE("update") {
    @Override
    public void validate(PluginTask task, KintoneClient client) {
      if (!task.getUpdateKeyName().isPresent() && client.getColumn(Id.FIELD) == null) {
        throw new ConfigException("When mode is update, require update_key or id column.");
      }
      client.validateIdOrUpdateKey(task.getUpdateKeyName().orElse(Id.FIELD));
    }

    @Override
    public void add(Page page, Skip skip, KintonePageOutput output) {
      Consumer<Page> consumer = skip == Skip.ALWAYS ? output::upsertPage : output::updatePage;
      consumer.accept(page);
    }
  },
  UPSERT("upsert") {
    @Override
    public void validate(PluginTask task, KintoneClient client) {
      if (!task.getUpdateKeyName().isPresent() && client.getColumn(Id.FIELD) == null) {
        throw new ConfigException("When mode is upsert, require update_key or id column.");
      }
      client.validateIdOrUpdateKey(task.getUpdateKeyName().orElse(Id.FIELD));
    }

    @Override
    public void add(Page page, Skip skip, KintonePageOutput output) {
      output.upsertPage(page);
    }
  };
  private final String value;

  KintoneMode(String value) {
    this.value = value;
  }

  public abstract void validate(PluginTask task, KintoneClient client);

  public abstract void add(Page page, Skip skip, KintonePageOutput output);

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
