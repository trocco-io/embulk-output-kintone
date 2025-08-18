package org.embulk.output.kintone;

import java.util.Collections;
import java.util.List;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.kintone.reducer.ReducedPageOutput;
import org.embulk.output.kintone.reducer.Reducer;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;

public class KintoneOutputPlugin implements OutputPlugin {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();
  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
  private static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

  @Override
  public ConfigDiff transaction(
      ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setDerivedColumns(Collections.emptySet());
    List<TaskReport> taskReports = control.run(task.dump());
    return task.getReduceKeyName().isPresent()
        ? new Reducer(task, schema)
            .reduce(taskReports, schema.lookupColumn(task.getReduceKeyName().get()))
        : CONFIG_MAPPER_FACTORY.newConfigDiff();
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
    PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
    return task.getReduceKeyName().isPresent()
        ? new ReducedPageOutput(schema, taskIndex)
        : new KintonePageOutput(task, schema, taskIndex);
  }
}
