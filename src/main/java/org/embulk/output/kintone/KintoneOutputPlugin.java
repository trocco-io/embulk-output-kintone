package org.embulk.output.kintone;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import java.util.Collection;
import java.util.List;

public class KintoneOutputPlugin
        implements OutputPlugin
{
    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  Schema schema, int taskCount,
                                  OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("kintone output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // validation
        switch (task.getMode()) {
            case INSERT:
                Collection<KintoneColumnOption> options = task.getColumnOptions().values();
                for (KintoneColumnOption option : options) {
                    if (option.getUpdateKey()) {
                        throw new IllegalArgumentException(
                                "when mode is insert, require no update_key.");
                    }
                }
                break;
//            case UPDATE:
//                if (task.getColumnOptions().values().stream()
//                        .filter(KintoneColumnOption::getUpdateKey).count() != 1) {
//                    throw new IllegalArgumentException(
//                            "when mode is update/upsert, require one update_key.");
//                }
//                break;
//            case UPSERT:
//                // TODO upsertPage
            default:
                throw new IllegalArgumentException(
                        "mode is insert only.");
        }
        return new KintonePageOutput(task, schema);
    }
}
