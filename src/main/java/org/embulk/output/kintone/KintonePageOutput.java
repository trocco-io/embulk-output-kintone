package org.embulk.output.kintone;

import com.kintone.client.KintoneClient;
import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.api.record.GetRecordsByCursorResponseBody;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import com.kintone.client.model.record.UpdateKey;
import org.embulk.config.TaskReport;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
        KintoneMode mode = KintoneMode.getKintoneModeByValue(task.getMode());
        switch (mode) {
            case INSERT:
                insertPage(page);
                break;
            case UPDATE:
                updatePage(page);
                break;
            case UPSERT:
                upsertPage(page);
                break;
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unknown mode '%s'",
                        task.getMode()));
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
        if (this.client == null) {
            return;
        }
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
        KintoneClientBuilder builder = KintoneClientBuilder.create("https://" + task.getDomain());
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

    private void updatePage(final Page page)
    {
        execute(client -> {
            try {
                ArrayList<RecordForUpdate> updateRecords = new ArrayList<RecordForUpdate>();
                pageReader.setPage(page);
                KintoneColumnVisitor visitor = new KintoneColumnVisitor(pageReader,
                        task.getColumnOptions());
                while (pageReader.nextRecord()) {
                    Record record = new Record();
                    UpdateKey updateKey = new UpdateKey();
                    visitor.setRecord(record);
                    visitor.setUpdateKey(updateKey);
                    for (Column column : pageReader.getSchema().getColumns()) {
                        column.visit(visitor);
                    }

                    record.removeField(updateKey.getField());
                    updateRecords.add(new RecordForUpdate(updateKey, record));
                    if (updateRecords.size() == 100) {
                        client.record().updateRecords(task.getAppId(), updateRecords);
                        updateRecords.clear();
                    }
                }
                if (updateRecords.size() > 0) {
                    client.record().updateRecords(task.getAppId(), updateRecords);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("kintone throw exception", e);
            }
        });
    }

    private List<Record> getRecordsByUpdateKey(String fieldCode, List<String> queryValues)
    {
        List<Record> allRecords = new ArrayList<Record>();
        List<String> fields = Arrays.asList(fieldCode);
        String cursorId = client.record().createCursor(
            task.getAppId(),
            fields,
            fieldCode + " in (" + String.join(",", queryValues) + ")"
        );
        while (true) {
            GetRecordsByCursorResponseBody resp = client.record().getRecordsByCursor(cursorId);
            List<Record> records = resp.getRecords();
            allRecords.addAll(records);

            if (!resp.hasNext()) {
                break;
            }
        }
        return allRecords;
    }

    abstract class UpsertPage<T>
    {
        public abstract List<T> getUpdateKeyValues(List<String> queryValues);
        public abstract boolean existsRecord(List<T> updateKeyValues, Record record);

        public void upsert(ArrayList<Record> records, ArrayList<UpdateKey> updateKeys)
        {
            if (records.size() != updateKeys.size()) {
                throw new RuntimeException("records.size() != updateKeys.size()");
            }

            List<String> queryValues = updateKeys
                .stream()
                .map(k -> "\"" + k.getValue().toString() + "\"")
                .collect(Collectors.toList());
            List<T> updateKeyValues = getUpdateKeyValues(queryValues);

            ArrayList<Record> insertRecords = new ArrayList<>();
            ArrayList<RecordForUpdate> updateRecords = new ArrayList<RecordForUpdate>();
            for (int i = 0; i < records.size(); i++) {
                Record record = records.get(i);
                UpdateKey updateKey = updateKeys.get(i);

                if (existsRecord(updateKeyValues, record)) {
                    record.removeField(updateKey.getField());
                    updateRecords.add(new RecordForUpdate(updateKey, record));
                }
                else {
                    insertRecords.add(record);
                }

                if (insertRecords.size() == 100) {
                    client.record().addRecords(task.getAppId(), insertRecords);
                    insertRecords.clear();
                }
                else if (updateRecords.size() == 100) {
                    client.record().updateRecords(task.getAppId(), updateRecords);
                    updateRecords.clear();
                }
            }
            if (insertRecords.size() > 0) {
                client.record().addRecords(task.getAppId(), insertRecords);
            }
            if (updateRecords.size() > 0) {
                client.record().updateRecords(task.getAppId(), updateRecords);
            }
        }

        public void run(final Page page)
        {
            execute(client -> {
                try {
                    ArrayList<Record> records = new ArrayList<>();
                    ArrayList<UpdateKey> updateKeys = new ArrayList<>();

                    pageReader.setPage(page);
                    KintoneColumnVisitor visitor = new KintoneColumnVisitor(pageReader,
                            task.getColumnOptions());
                    while (pageReader.nextRecord()) {
                        Record record = new Record();
                        UpdateKey updateKey = new UpdateKey();
                        visitor.setRecord(record);
                        visitor.setUpdateKey(updateKey);
                        for (Column column : pageReader.getSchema().getColumns()) {
                            column.visit(visitor);
                        }
                        records.add(record);
                        updateKeys.add(updateKey);

                        if (records.size() == 10000) {
                            upsert(records, updateKeys);
                            records.clear();
                            updateKeys.clear();
                        }
                    }
                    if (records.size() > 0) {
                        upsert(records, updateKeys);
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException("kintone throw exception", e);
                }
            });
        }
    }

    class UpsertPageByStringKey extends UpsertPage<String>
    {
        private String fieldCode;

        public UpsertPageByStringKey(String fieldCode)
        {
            this.fieldCode = fieldCode;
        }

        public List<String> getUpdateKeyValues(List<String> queryValues)
        {
            return getRecordsByUpdateKey(fieldCode, queryValues)
                .stream()
                .map(r -> r.getSingleLineTextFieldValue(fieldCode))
                .collect(Collectors.toList());
        }

        public boolean existsRecord(List<String> updateKeyValues, Record record)
        {
            return updateKeyValues.contains(record.getSingleLineTextFieldValue(fieldCode));
        }
    }

    class UpsertPageByNumberKey extends UpsertPage<BigDecimal>
    {
        private String fieldCode;

        public UpsertPageByNumberKey(String fieldCode)
        {
            this.fieldCode = fieldCode;
        }

        public List<BigDecimal> getUpdateKeyValues(List<String> queryValues)
        {
            return getRecordsByUpdateKey(fieldCode, queryValues)
                .stream()
                .map(r -> r.getNumberFieldValue(fieldCode))
                .collect(Collectors.toList());
        }

        public boolean existsRecord(List<BigDecimal> updateKeyValues, Record record)
        {
            return updateKeyValues.contains(record.getNumberFieldValue(fieldCode));
        }
    }

    private void upsertPage(final Page page)
    {
        KintoneColumnOption updateKeyColumn = null;
        for (KintoneColumnOption v : task.getColumnOptions().values()) {
            if (v.getUpdateKey()) {
                updateKeyColumn = v;
                break;
            }
        }
        if (updateKeyColumn == null) {
            throw new RuntimeException("when mode is upsert, require update_key");
        }

        UpsertPage runner;
        switch(updateKeyColumn.getType()) {
            case "SINGLE_LINE_TEXT":
                runner = new UpsertPageByStringKey(updateKeyColumn.getFieldCode());
                break;
            case "NUMBER":
                runner = new UpsertPageByNumberKey(updateKeyColumn.getFieldCode());
                break;
            default:
                throw new RuntimeException("The update_key must be 'SINGLE_LINE_TEXT' or 'NUMBER'.");
        }
        runner.run(page);
    }
}
