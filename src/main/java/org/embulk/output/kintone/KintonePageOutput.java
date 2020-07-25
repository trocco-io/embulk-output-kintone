package org.embulk.output.kintone;

import com.kintone.client.KintoneClient;
import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.model.record.Record;
import org.embulk.config.TaskReport;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import java.util.ArrayList;

public class KintonePageOutput
        implements TransactionalPageOutput
{
    private PageReader pageReader;
    private PluginTask task;
    private KintoneClient client;

    public KintonePageOutput(PluginTask task, Schema schema)
    {
        this.pageReader = new PageReader(schema);
        this.task = task;
    }

    @Override
    public void add(Page page)
    {
        switch (task.getMode()) {
            case INSERT:
                insertPage(page);
                break;
            case UPDATE:
                // TODO updatePage
            case UPSERT:
                // TODO upsertPage
            default:
                throw new UnsupportedOperationException(
                        "kintone output plugin does not support update, upsert");
        }
    }

    @Override
    public void finish()
    {
        // noop
    }

    @Override
    public void close()
    {
        try {
            this.client.close();
        }
        catch (Exception e) {
            throw new RuntimeException("kintone throw exception", e);
        }
    }

    @Override
    public void abort()
    {
        // noop
    }

    @Override
    public TaskReport commit()
    {
        return Exec.newTaskReport();
    }

    public interface Consumer<T>
    {
        public void accept(T t);
    }

    public void connect(final PluginTask task)
    {
        KintoneClientBuilder builder = KintoneClientBuilder.create(task.getDomain());
        if (task.getGuestSpaceId().isPresent()) {
            builder.setGuestSpaceId(task.getGuestSpaceId().or(-1));
        }
        if (task.getBasicAuthUsername().isPresent() && task.getBasicAuthPassword().isPresent()) {
            builder.withBasicAuth(task.getBasicAuthUsername().get(),
                    task.getBasicAuthPassword().get());
        }

        if (task.getUsername().isPresent() && task.getPassword().isPresent()) {
            this.client = builder
                .authByPassword(task.getUsername().get(), task.getPassword().get())
                .build();
        }
        else if (task.getToken().isPresent()) {
            this.client = builder
                .authByApiToken(task.getToken().get())
                .build();
        }
    }

    private void execute(Consumer<KintoneClient> operation)
    {
        connect(task);
        operation.accept(this.client);
    }

    private void insertPage(final Page page)
    {
        execute(client -> {
            try {
                ArrayList<Record> records = new ArrayList<>();
                pageReader.setPage(page);
                KintoneColumnVisitor visitor = new KintoneColumnVisitor(pageReader,
                        task.getColumnOptions());
                while (pageReader.nextRecord()) {
                    Record record = new Record();
                    visitor.setRecord(record);
                    for (Column column : pageReader.getSchema().getColumns()) {
                        column.visit(visitor);
                    }

                    records.add(record);
                    if (records.size() == 100) {
                        client.record().addRecords(task.getAppId(), records);
                        records.clear();
                    }
                }
                if (records.size() > 0) {
                    client.record().addRecords(task.getAppId(), records);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("kintone throw exception", e);
            }
        });
    }
}
