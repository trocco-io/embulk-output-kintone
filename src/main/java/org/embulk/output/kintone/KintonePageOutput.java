package org.embulk.output.kintone;

import com.kintone.client.KintoneClient;
import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.api.record.GetRecordsByCursorResponseBody;
import com.kintone.client.model.record.FieldType;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class KintonePageOutput
        implements TransactionalPageOutput
{
    public static final int UPSERT_BATCH_SIZE = 10000;
    public static final int CHUNK_SIZE = 100;
    private final PageReader pageReader;
    private final PluginTask task;
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
        void accept(T t);
    }

    public void connect(final PluginTask task)
    {
        KintoneClientBuilder builder = KintoneClientBuilder.create("https://" + task.getDomain());
        if (task.getGuestSpaceId().isPresent()) {
            builder.setGuestSpaceId(task.getGuestSpaceId().orElse(-1));
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
                    if (records.size() == CHUNK_SIZE) {
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
                ArrayList<RecordForUpdate> updateRecords = new ArrayList<>();
                pageReader.setPage(page);

                KintoneColumnVisitor visitor = new KintoneColumnVisitor(pageReader,
                        task.getColumnOptions(),
                        task.getUpdateKeyName().orElseThrow(() -> new RuntimeException("unreachable"))); // Already validated
                while (pageReader.nextRecord()) {
                    Record record = new Record();
                    UpdateKey updateKey = new UpdateKey();
                    visitor.setRecord(record);
                    visitor.setUpdateKey(updateKey);
                    for (Column column : pageReader.getSchema().getColumns()) {
                        column.visit(visitor);
                    }

                    if (updateKey.getValue() == "") {
                        continue;
                    }

                    record.removeField(updateKey.getField());
                    updateRecords.add(new RecordForUpdate(updateKey, record));
                    if (updateRecords.size() == CHUNK_SIZE) {
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

    private void upsertPage(final Page page)
    {
        execute(client -> {
            try {
                ArrayList<Record> records = new ArrayList<>();
                ArrayList<UpdateKey> updateKeys = new ArrayList<>();
                pageReader.setPage(page);

                KintoneColumnVisitor visitor = new KintoneColumnVisitor(pageReader,
                        task.getColumnOptions(),
                        task.getUpdateKeyName().orElseThrow(() -> new RuntimeException("unreachable"))); // Already validated
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

                    if (records.size() == UPSERT_BATCH_SIZE) {
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

    private void upsert(ArrayList<Record> records, ArrayList<UpdateKey> updateKeys)
    {
        if (records.size() != updateKeys.size()) {
            throw new RuntimeException("records.size() != updateKeys.size()");
        }

        List<Record> existingRecords = getExistingRecordsByUpdateKey(updateKeys);

        ArrayList<Record> insertRecords = new ArrayList<>();
        ArrayList<RecordForUpdate> updateRecords = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            Record record = records.get(i);
            UpdateKey updateKey = updateKeys.get(i);

            if (existsRecord(existingRecords, updateKey)) {
                record.removeField(updateKey.getField());
                updateRecords.add(new RecordForUpdate(updateKey, record));
            }
            else {
                insertRecords.add(record);
            }

            if (insertRecords.size() == CHUNK_SIZE) {
                client.record().addRecords(task.getAppId(), insertRecords);
                insertRecords.clear();
            }
            else if (updateRecords.size() == CHUNK_SIZE) {
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


    private List<Record> getExistingRecordsByUpdateKey(ArrayList<UpdateKey> updateKeys)
    {
        String fieldCode = updateKeys.get(0).getField();
        List<String> queryValues = updateKeys
                .stream()
                .filter(k -> k.getValue() != "")
                .map(k -> "\"" + k.getValue().toString() + "\"")
                .collect(Collectors.toList());

        List<Record> allRecords = new ArrayList<>();
        if (queryValues.isEmpty()) {
            return allRecords;
        }
        String cursorId = client.record().createCursor(
            task.getAppId(),
            Collections.singletonList(fieldCode),
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

    private boolean existsRecord(List<Record> distRecords, UpdateKey updateKey)
    {
        String fieldCode = updateKey.getField();
        FieldType type = client.app().getFormFields(task.getAppId()).get(fieldCode).getType();
        switch (type) {
            case SINGLE_LINE_TEXT:
                return distRecords
                        .stream()
                        .anyMatch(d -> d.getSingleLineTextFieldValue(fieldCode).equals(updateKey.getValue().toString()));
            case NUMBER:
                return distRecords
                        .stream()
                        .anyMatch(d -> d.getNumberFieldValue(fieldCode).toPlainString()
                                        .equals(updateKey.getValue().toString()));
            default:
                throw new RuntimeException("The update_key must be 'SINGLE_LINE_TEXT' or 'NUMBER'.");
        }
    }
}
