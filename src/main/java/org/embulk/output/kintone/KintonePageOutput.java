package org.embulk.output.kintone;

import com.cybozu.kintone.client.authentication.Auth;
import com.cybozu.kintone.client.connection.Connection;
import com.cybozu.kintone.client.model.record.field.FieldValue;
import com.cybozu.kintone.client.module.record.Record;
import org.embulk.config.TaskReport;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class KintonePageOutput
        implements TransactionalPageOutput
{

    private PageReader pageReader;
    private PluginTask task;
    private Connection conn;

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
//            case UPDATE:
//                updatePage(page);
//                break;
//            case UPSERT:
//                // TODO upsertPage
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
        // noop
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

    public void connect(final PluginTask task) {
        Auth kintoneAuth = new Auth();
        if (task.getUsername().isPresent() && task.getPassword().isPresent()) {
            kintoneAuth.setPasswordAuth(task.getUsername().get(), task.getPassword().get());
        } else if (task.getToken().isPresent()) { kintoneAuth.setApiToken(task.getToken().get());
        }

        if (task.getBasicAuthUsername().isPresent() && task.getBasicAuthPassword().isPresent()) {
            kintoneAuth.setBasicAuth(task.getBasicAuthUsername().get(),
                    task.getBasicAuthPassword().get());
        }

        if (task.getGuestSpaceId().isPresent()) {
            this.conn = new Connection(task.getDomain(), kintoneAuth, task.getGuestSpaceId().or(-1));
        } else {
            this.conn = new Connection(task.getDomain(), kintoneAuth);
        }
    }

    private void execute(Consumer<Connection> operation)
    {
        try {
            connect(task);
            operation.accept(this.conn);
        }
        finally {
            if (conn != null) {
//                this.conn.close();
            }
        }
    }

    private void insertPage(final Page page)
    {
        execute(conn -> {
            try {
                ArrayList<HashMap<String, FieldValue>> records = new ArrayList<>();
                pageReader.setPage(page);
                KintoneColumnVisitor visitor = new KintoneColumnVisitor(pageReader,
                        task.getColumnOptions());
                Record kintoneRecordManager = new Record(conn);
                while (pageReader.nextRecord()) {
                    HashMap record = new HashMap();
                    visitor.setRecord(record);
                    for (Column column : pageReader.getSchema().getColumns()) {
                        column.visit(visitor);
                    }
                    records.add(record);
                    if (records.size() == 100) {
                        kintoneRecordManager.addRecords(task.getAppId(), records);
                        records.clear();
                    }
                }
                if (records.size() > 0) {
                    kintoneRecordManager.addRecords(task.getAppId(), records);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("kintone throw exception", e);
            }
        });
    }

//    private void updatePage(final Page page)
//    {
//        execute(new Consumer<Connection>()
//        {
//            @Override
//            public void accept(Connection conn)
//            {
//                try {
//                    List<Record> records = new ArrayList<>();
//                    pageReader.setPage(page);
//                    KintoneColumnVisitor visitor = new KintoneColumnVisitor(pageReader,
//                            task.getColumnOptions());
//                    while (pageReader.nextRecord()) {
//                        Record record = new Record();
//                        visitor.setRecord(record);
//                        for (Column column : pageReader.getSchema().getColumns()) {
//                            column.visit(visitor);
//                        }
//                        records.add(record);
//                        if (records.size() == 100) {
//                            conn.updateByRecords(task.getAppId(), records);
//                            records.clear();
//                        }
//                    }
//                    if (records.size() > 0) {
//                        conn.updateByRecords(task.getAppId(), records);
//                    }
//                }
//                catch (DBException e) {
//                    throw new RuntimeException("kintone throw exception", e);
//                }
//            }
//        });
//    }
}
