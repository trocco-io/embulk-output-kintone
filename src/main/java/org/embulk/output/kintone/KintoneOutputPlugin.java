package org.embulk.output.kintone;

import java.util.List;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

public class KintoneOutputPlugin implements OutputPlugin {
  @Override
  public ConfigDiff transaction(
      ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
    PluginTask task = config.loadConfig(PluginTask.class);
    control.run(task.dump());
    return Exec.newConfigDiff();
  }

  @Override
  public ConfigDiff resume(
      TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
    throw new UnsupportedOperationException("kintone output plugin does not support resuming");
  }

  @Override
  public void cleanup(
      TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {}

  @Override
  public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
    PluginTask task = taskSource.loadTask(PluginTask.class);
    KintoneMode mode = KintoneMode.getKintoneModeByValue(task.getMode());
    switch (mode) {
      case INSERT:
        if (task.getUpdateKeyName().isPresent()) {
          throw new ConfigException("when mode is insert, require no update_key.");
        }
        break;
      case UPDATE:
      case UPSERT:
        if (!task.getUpdateKeyName().isPresent()) {
          throw new ConfigException("when mode is update or upsert, require update_key.");
        }
        break;
      default:
        throw new ConfigException(String.format("Unknown mode '%s'", task.getMode()));
    }
    return new KintonePageOutput(task, schema);
  }
}
