package org.embulk.output.kintone;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.kintone.client.api.record.GetRecordsByCursorResponseBody;
import com.kintone.client.exception.KintoneApiRuntimeException;
import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import com.kintone.client.model.record.UpdateKey;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.embulk.config.TaskReport;
import org.embulk.output.kintone.util.Lazy;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KintonePageOutput implements TransactionalPageOutput {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final List<String> RETRYABLE_ERROR_CODES =
      Arrays.asList(
          "GAIA_TM12", // 作成できるカーソルの上限に達しているため、カーソルを作成できません。不要なカーソルを削除するか、しばらく経ってから再実行してください。
          "GAIA_RE18", // データベースのロックに失敗したため、変更を保存できませんでした。時間をおいて再度お試しください。
          "GAIA_DA02" // データベースのロックに失敗したため、変更を保存できませんでした。時間をおいて再度お試しください。
          );
  private static final int UPSERT_BATCH_SIZE = 10000;
  private final Map<String, Pair<FieldType, FieldType>> wrongTypeFields = new TreeMap<>();
  private final PluginTask task;
  private final PageReader reader;
  private final Lazy<KintoneClient> client;

  public KintonePageOutput(PluginTask task, Schema schema) {
    this.task = task;
    reader = new PageReader(schema);
    client = KintoneClient.lazy(() -> task, schema);
  }

  @Override
  public void add(Page page) {
    KintoneMode.of(task).add(page, this);
  }

  @Override
  public void finish() {
    // noop
  }

  @Override
  public void close() {
    client.get().close();
  }

  @Override
  public void abort() {
    // noop
  }

  @Override
  public TaskReport commit() {
    wrongTypeFields.forEach(
        (key, value) ->
            LOGGER.warn(
                String.format(
                    "Field type of %s is expected %s but actual %s",
                    key, value.getLeft(), value.getRight())));
    return Exec.newTaskReport();
  }

  private void insert(List<Record> records) {
    executeWithRetry(() -> client.get().record().addRecords(task.getAppId(), records));
  }

  private void update(List<RecordForUpdate> records) {
    executeWithRetry(() -> client.get().record().updateRecords(task.getAppId(), records));
  }

  private <T> T executeWithRetry(Supplier<T> operation) {
    KintoneRetryOption retryOption = task.getRetryOptions();
    try {
      return retryExecutor()
          .withRetryLimit(retryOption.getLimit())
          .withInitialRetryWait(retryOption.getInitialWaitMillis())
          .withMaxRetryWait(retryOption.getMaxWaitMillis())
          .runInterruptible(
              new Retryable<T>() {
                @Override
                public T call() {
                  return operation.get();
                }

                @Override
                public boolean isRetryableException(Exception exception) {
                  if (!(exception instanceof KintoneApiRuntimeException)) {
                    return false;
                  }
                  try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode content =
                        mapper.readTree(((KintoneApiRuntimeException) exception).getContent());
                    String code = content.get("code").textValue();
                    return RETRYABLE_ERROR_CODES.contains(code);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                }

                @Override
                public void onRetry(
                    Exception exception, int retryCount, int retryLimit, int retryWait) {
                  String message =
                      String.format(
                          "Retrying %d/%d after %d seconds. Message: %s",
                          retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                  if (retryCount % 3 == 0) {
                    LOGGER.warn(message, exception);
                  } else {
                    LOGGER.warn(message);
                  }
                }

                @Override
                public void onGiveup(Exception firstException, Exception lastException) {}
              });
    } catch (RetryGiveupException | InterruptedException e) {
      throw new RuntimeException("kintone throw exception", e);
    }
  }

  public void insertPage(Page page) {
    List<Record> records = new ArrayList<>();
    reader.setPage(page);
    KintoneColumnVisitor visitor =
        new KintoneColumnVisitor(
            reader,
            task.getDerivedColumns(),
            task.getColumnOptions(),
            task.getPreferNulls(),
            task.getIgnoreNulls(),
            task.getReduceKeyName().orElse(null));
    while (reader.nextRecord()) {
      Record record = new Record();
      visitor.setRecord(record);
      reader.getSchema().visitColumns(visitor);
      putWrongTypeFields(record);
      records.add(record);
      if (records.size() == task.getChunkSize()) {
        insert(records);
        records.clear();
      }
    }
    if (!records.isEmpty()) {
      insert(records);
    }
  }

  public void updatePage(Page page) {
    List<RecordForUpdate> records = new ArrayList<>();
    reader.setPage(page);
    KintoneColumnVisitor visitor =
        new KintoneColumnVisitor(
            reader,
            task.getDerivedColumns(),
            task.getColumnOptions(),
            task.getPreferNulls(),
            task.getIgnoreNulls(),
            task.getReduceKeyName().orElse(null),
            task.getUpdateKeyName().orElse(null));
    while (reader.nextRecord()) {
      Record record = new Record();
      UpdateKey updateKey = new UpdateKey();
      visitor.setRecord(record);
      visitor.setUpdateKey(updateKey);
      reader.getSchema().visitColumns(visitor);
      putWrongTypeFields(record);
      if (updateKey.getValue() == null || updateKey.getValue().toString().isEmpty()) {
        LOGGER.warn("Record skipped because no update key value was specified");
        continue;
      }
      records.add(new RecordForUpdate(updateKey, record.removeField(updateKey.getField())));
      if (records.size() == task.getChunkSize()) {
        update(records);
        records.clear();
      }
    }
    if (!records.isEmpty()) {
      update(records);
    }
  }

  public void upsertPage(Page page) {
    List<Record> records = new ArrayList<>();
    List<UpdateKey> updateKeys = new ArrayList<>();
    reader.setPage(page);
    KintoneColumnVisitor visitor =
        new KintoneColumnVisitor(
            reader,
            task.getDerivedColumns(),
            task.getColumnOptions(),
            task.getPreferNulls(),
            task.getIgnoreNulls(),
            task.getReduceKeyName().orElse(null),
            task.getUpdateKeyName().orElse(null));
    while (reader.nextRecord()) {
      Record record = new Record();
      UpdateKey updateKey = new UpdateKey();
      visitor.setRecord(record);
      visitor.setUpdateKey(updateKey);
      reader.getSchema().visitColumns(visitor);
      putWrongTypeFields(record);
      records.add(record);
      updateKeys.add(updateKey);
      if (records.size() == UPSERT_BATCH_SIZE) {
        upsert(records, updateKeys);
        records.clear();
        updateKeys.clear();
      }
    }
    if (!records.isEmpty()) {
      upsert(records, updateKeys);
    }
  }

  private void upsert(List<Record> records, List<UpdateKey> updateKeys) {
    if (records.size() != updateKeys.size()) {
      throw new RuntimeException("records.size() != updateKeys.size()");
    }
    List<String> existingValues = executeWithRetry(() -> getExistingValuesByUpdateKey(updateKeys));
    List<Record> insertRecords = new ArrayList<>();
    List<RecordForUpdate> updateRecords = new ArrayList<>();
    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      UpdateKey updateKey = updateKeys.get(i);
      if (existsRecord(existingValues, updateKey)) {
        updateRecords.add(new RecordForUpdate(updateKey, record.removeField(updateKey.getField())));
      } else {
        insertRecords.add(record);
      }
      if (insertRecords.size() == task.getChunkSize()) {
        insert(insertRecords);
        insertRecords.clear();
      } else if (updateRecords.size() == task.getChunkSize()) {
        update(updateRecords);
        updateRecords.clear();
      }
    }
    if (!insertRecords.isEmpty()) {
      insert(insertRecords);
    }
    if (!updateRecords.isEmpty()) {
      update(updateRecords);
    }
  }

  private List<String> getExistingValuesByUpdateKey(List<UpdateKey> updateKeys) {
    List<String> queryValues =
        updateKeys.stream()
            .filter(k -> k.getValue() != null && !k.getValue().toString().isEmpty())
            .map(k -> "\"" + k.getValue() + "\"")
            .collect(Collectors.toList());
    if (queryValues.isEmpty()) {
      return Collections.emptyList();
    }
    String columnName = task.getUpdateKeyName().orElse(null);
    KintoneColumnOption option = task.getColumnOptions().get(columnName);
    String fieldCode = option != null ? option.getFieldCode() : columnName;
    KintoneColumnType type = KintoneColumnType.valueOf(getFieldType(fieldCode).name());
    return getExistingValues(fieldCode, record -> type.getValue(record, fieldCode), queryValues);
  }

  private List<String> getExistingValues(
      String fieldCode, Function<Record, Object> toValue, List<String> queryValues) {
    String cursorId =
        client
            .get()
            .record()
            .createCursor(
                task.getAppId(),
                Collections.singletonList(fieldCode),
                fieldCode + " in (" + String.join(",", queryValues) + ")");
    List<Record> records = new ArrayList<>();
    while (true) {
      GetRecordsByCursorResponseBody cursor = client.get().record().getRecordsByCursor(cursorId);
      records.addAll(cursor.getRecords());
      if (!cursor.hasNext()) {
        break;
      }
    }
    return records.stream()
        .map(toValue)
        .map(KintonePageOutput::toString)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private void putWrongTypeFields(Record record) {
    record.getFieldCodes(true).stream()
        .map(
            fieldCode ->
                Maps.immutableEntry(
                    fieldCode, Pair.of(record.getFieldType(fieldCode), getFieldType(fieldCode))))
        .filter(entry -> entry.getValue().getLeft() != entry.getValue().getRight())
        .forEach(entry -> wrongTypeFields.put(entry.getKey(), entry.getValue()));
  }

  private FieldType getFieldType(String fieldCode) {
    return client.get().getFieldType(fieldCode);
  }

  private static boolean existsRecord(List<String> existingValues, UpdateKey updateKey) {
    String value = toString(updateKey.getValue());
    return value != null && existingValues.stream().anyMatch(v -> v.equals(value));
  }

  private static String toString(Object value) {
    return value == null
        ? null
        : value instanceof BigDecimal ? ((BigDecimal) value).toPlainString() : value.toString();
  }
}
