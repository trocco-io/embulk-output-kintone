package org.embulk.output.kintone;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KintoneOutputPlugin implements OutputPlugin {
  private static final Logger LOGGER = LoggerFactory.getLogger(KintoneOutputPlugin.class);
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
      TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {
    PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

    // Concatenate error files if error output is configured
    task.getErrorRecordsDetailOutputFile().ifPresent(this::concatenateErrorFiles);
  }

  @Override
  public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
    PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
    return task.getReduceKeyName().isPresent()
        ? new ReducedPageOutput(schema, taskIndex)
        : new KintonePageOutput(task, schema, taskIndex);
  }

  private void concatenateErrorFiles(String outputFile) {
    Path outputPath = Paths.get(outputFile);
    Path directory = outputPath.getParent();
    String baseFileName = outputPath.getFileName().toString();

    try {
      List<Path> taskFiles =
          Files.list(directory)
              .filter(path -> path.getFileName().toString().startsWith(baseFileName + "_task"))
              .sorted()
              .collect(java.util.stream.Collectors.toList());

      // If no task files exist, don't create output file
      if (taskFiles.isEmpty()) {
        return;
      }

      try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
        boolean hasContent = false;
        for (Path taskFile : taskFiles) {
          try {
            List<String> lines = Files.readAllLines(taskFile);
            if (!lines.isEmpty()) {
              hasContent = true;
              for (String line : lines) {
                writer.write(line);
                writer.newLine();
              }
            }
            Files.deleteIfExists(taskFile);
          } catch (IOException e) {
            LOGGER.error("Failed to process task file: " + taskFile, e);
          }
        }

        // If no content was written, delete the empty output file
        if (!hasContent) {
          writer.close();
          Files.deleteIfExists(outputPath);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to concatenate error files", e);
    }
  }
}
