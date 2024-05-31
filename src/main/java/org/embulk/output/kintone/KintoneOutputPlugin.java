package org.embulk.output.kintone;

import java.util.Collections;
import java.util.List;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.kintone.reducer.ReducedPageOutput;
import org.embulk.output.kintone.reducer.Reducer;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

public class KintoneOutputPlugin implements OutputPlugin {
  @Override
  public ConfigDiff transaction(
      ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
    PluginTask task = config.loadConfig(PluginTask.class);
    task.setDerivedColumns(Collections.emptySet());
    List<TaskReport> taskReports = control.run(task.dump());
    return task.getReduceKeyName().isPresent()
        ? new Reducer(task, schema)
            .reduce(taskReports, schema.lookupColumn(task.getReduceKeyName().get()))
        : Exec.newConfigDiff();
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
    return task.getReduceKeyName().isPresent()
        ? new ReducedPageOutput(schema, taskIndex)
        : new KintonePageOutput(task, schema);
  }
}
