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
  public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  @Override
  public ConfigDiff transaction(
      ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
    ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
    PluginTask task = configMapper.map(config, PluginTask.class);
    task.setDerivedColumns(Collections.emptySet());
    List<TaskReport> taskReports = control.run(task.toTaskSource());
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
    TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
    PluginTask task = taskMapper.map(taskSource, PluginTask.class);
    return task.getReduceKeyName().isPresent()
        ? new ReducedPageOutput(schema, taskIndex)
        : new KintonePageOutput(task, schema);
  }
}
