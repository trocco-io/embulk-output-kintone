package org.embulk.output.kintone;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.kintone.client.KintoneClient;
import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.api.record.GetRecordsByCursorResponseBody;
import com.kintone.client.exception.KintoneApiRuntimeException;
import com.kintone.client.model.app.field.FieldProperty;
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
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
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
  private KintoneClient client;
  private Map<String, FieldProperty> formFields;

  public KintonePageOutput(PluginTask task, Schema schema) {
    this.task = task;
    reader = new PageReader(schema);
  }

  @Override
  public void add(Page page) {
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
        throw new UnsupportedOperationException(String.format("Unknown mode '%s'", task.getMode()));
    }
  }

  @Override
  public void finish() {
    // noop
  }

  @Override
  public void close() {
    if (client == null) {
      return; // Not connected
    }
    try {
      client.close();
    } catch (Exception e) {
      throw new RuntimeException("kintone throw exception", e);
    }
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

  private void connectIfNeeded() {
    if (client != null) {
      return; // Already connected
    }
    KintoneClientBuilder builder = KintoneClientBuilder.create("https://" + task.getDomain());
    if (task.getGuestSpaceId().isPresent()) {
      builder.setGuestSpaceId(task.getGuestSpaceId().get());
    }
    if (task.getBasicAuthUsername().isPresent() && task.getBasicAuthPassword().isPresent()) {
      builder.withBasicAuth(task.getBasicAuthUsername().get(), task.getBasicAuthPassword().get());
    }
    if (task.getUsername().isPresent() && task.getPassword().isPresent()) {
      builder.authByPassword(task.getUsername().get(), task.getPassword().get());
    } else if (task.getToken().isPresent()) {
      builder.authByApiToken(task.getToken().get());
    } else {
      throw new ConfigException("Username and password or token must be configured.");
    }
    client = builder.build();
    formFields = client.app().getFormFields(task.getAppId());
  }

  private void insert(List<Record> records) {
    executeWithRetry(() -> client.record().addRecords(task.getAppId(), records));
  }

  private void update(List<RecordForUpdate> records) {
    executeWithRetry(() -> client.record().updateRecords(task.getAppId(), records));
  }

  private <T> T executeWithRetry(Supplier<T> operation) {
    connectIfNeeded();
    KintoneRetryOption retryOption = task.getRetryOptions();
    try {
      return retryExecutor()
          .withRetryLimit(retryOption.getLimit())
          .withInitialRetryWait(retryOption.getInitialWaitMillis())
          .withMaxRetryWait(retryOption.getMaxWaitMillis())
          .runInterruptible(
              new Retryable<T>() {
                @Override
                public T call() throws Exception {
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
                    Exception exception, int retryCount, int retryLimit, int retryWait)
                    throws RetryGiveupException {
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
                public void onGiveup(Exception firstException, Exception lastException)
                    throws RetryGiveupException {}
              });
    } catch (RetryGiveupException | InterruptedException e) {
      throw new RuntimeException("kintone throw exception", e);
    }
  }

  private void insertPage(Page page) {
    List<Record> records = new ArrayList<>();
    reader.setPage(page);
    KintoneColumnVisitor visitor =
        new KintoneColumnVisitor(
            reader,
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

  private void updatePage(Page page) {
    List<RecordForUpdate> records = new ArrayList<>();
    reader.setPage(page);
    KintoneColumnVisitor visitor =
        new KintoneColumnVisitor(
            reader,
            task.getColumnOptions(),
            task.getPreferNulls(),
            task.getIgnoreNulls(),
            task.getReduceKeyName().orElse(null),
            task.getUpdateKeyName()
                .orElseThrow(() -> new RuntimeException("unreachable"))); // Already validated
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

  private void upsertPage(Page page) {
    List<Record> records = new ArrayList<>();
    List<UpdateKey> updateKeys = new ArrayList<>();
    reader.setPage(page);
    KintoneColumnVisitor visitor =
        new KintoneColumnVisitor(
            reader,
            task.getColumnOptions(),
            task.getPreferNulls(),
            task.getIgnoreNulls(),
            task.getReduceKeyName().orElse(null),
            task.getUpdateKeyName()
                .orElseThrow(() -> new RuntimeException("unreachable"))); // Already validated
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
    String fieldCode =
        updateKeys.stream()
            .map(UpdateKey::getField)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    if (fieldCode == null) {
      return Collections.emptyList();
    }
    Function<Record, String> fieldValueAsString;
    FieldType fieldType = getFieldType(fieldCode);
    if (fieldType == FieldType.SINGLE_LINE_TEXT) {
      fieldValueAsString = record -> record.getSingleLineTextFieldValue(fieldCode);
    } else if (fieldType == FieldType.NUMBER) {
      fieldValueAsString = record -> toString(record.getNumberFieldValue(fieldCode));
    } else {
      throw new ConfigException("The update_key must be 'SINGLE_LINE_TEXT' or 'NUMBER'.");
    }
    List<String> queryValues =
        updateKeys.stream()
            .filter(k -> k.getValue() != null && !k.getValue().toString().isEmpty())
            .map(k -> "\"" + k.getValue() + "\"")
            .collect(Collectors.toList());
    if (queryValues.isEmpty()) {
      return Collections.emptyList();
    }
    String cursorId =
        client
            .record()
            .createCursor(
                task.getAppId(),
                Collections.singletonList(fieldCode),
                fieldCode + " in (" + String.join(",", queryValues) + ")");
    List<Record> allRecords = new ArrayList<>();
    while (true) {
      GetRecordsByCursorResponseBody resp = client.record().getRecordsByCursor(cursorId);
      List<Record> records = resp.getRecords();
      allRecords.addAll(records);
      if (!resp.hasNext()) {
        break;
      }
    }
    return allRecords.stream()
        .map(fieldValueAsString)
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
    connectIfNeeded();
    FieldProperty field = formFields.get(fieldCode);
    return field == null ? null : field.getType();
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
