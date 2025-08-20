package org.embulk.output.kintone;

import static org.embulk.util.retryhelper.RetryExecutor.retryExecutor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.kintone.client.api.record.GetRecordsByCursorResponseBody;
import com.kintone.client.exception.KintoneApiRuntimeException;
import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.embulk.config.TaskReport;
import org.embulk.output.kintone.record.Id;
import org.embulk.output.kintone.record.IdOrUpdateKey;
import org.embulk.output.kintone.record.Skip;
import org.embulk.output.kintone.util.Lazy;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KintonePageOutput implements TransactionalPageOutput {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final List<String> RETRYABLE_ERROR_CODES =
      Arrays.asList(
          "GAIA_TM12", // Cannot create a cursor because the maximum number of cursors has been
          // reached. Delete unnecessary cursors or retry after some time.
          "GAIA_RE18", // Changes could not be saved due to database lock failure. Please try again
          // after some time.
          "GAIA_DA02" // Changes could not be saved due to database lock failure. Please try again
          // after some time.
          );
  private static final int UPSERT_BATCH_SIZE = 10000;
  private final Map<String, Pair<FieldType, FieldType>> wrongTypeFields = new TreeMap<>();
  private final PluginTask task;
  private final PageReader reader;
  private final Lazy<KintoneClient> client;
  private final ErrorFileLogger errorFileLogger;
  private final int taskIndex;

  public KintonePageOutput(PluginTask task, Schema schema) {
    this(task, schema, 0);
  }

  public KintonePageOutput(PluginTask task, Schema schema, int taskIndex) {
    this.task = task;
    this.taskIndex = taskIndex;
    reader = new PageReader(schema);
    client = KintoneClient.lazy(() -> task, schema);

    // Initialize error file logger
    this.errorFileLogger =
        new ErrorFileLogger(task.getErrorRecordsDetailOutputFile().orElse(null), taskIndex);
  }

  @Override
  public void add(Page page) {
    KintoneMode.of(task).add(page, task.getSkipIfNonExistingIdOrUpdateKey(), this);
  }

  @Override
  public void finish() {
    // noop
  }

  @Override
  public void close() {
    // Close error file logger
    if (errorFileLogger != null) {
      try {
        errorFileLogger.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close error file logger", e);
      }
    }

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
    executeWithRetry(() -> client.get().record().addRecords(task.getAppId(), records), records);
  }

  private void update(List<RecordForUpdate> records) {
    executeWithRetry(() -> client.get().record().updateRecords(task.getAppId(), records), records);
  }

  private <T> T executeWithRetry(Supplier<T> operation) {
    return executeWithRetry(operation, null);
  }

  private <T> T executeWithRetry(Supplier<T> operation, List<?> records) {
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
                  try {
                    return operation.get();
                  } catch (KintoneApiRuntimeException e) {
                    // Log error details
                    if (errorFileLogger != null && records != null) {
                      logApiError(e, records);
                    }
                    throw e;
                  }
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
    Skip skip = task.getSkipIfNonExistingIdOrUpdateKey();
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
            task.getUpdateKeyName().orElse(Id.FIELD));
    while (reader.nextRecord()) {
      Record record = new Record();
      IdOrUpdateKey idOrUpdateKey = new IdOrUpdateKey();
      visitor.setRecord(record);
      visitor.setIdOrUpdateKey(idOrUpdateKey);
      reader.getSchema().visitColumns(visitor);
      putWrongTypeFields(record);
      if (skip == Skip.NEVER && !idOrUpdateKey.isPresent()) {
        throw new RuntimeException("No id or update key value was specified");
      } else if (!idOrUpdateKey.isPresent()) {
        LOGGER.warn("Record skipped because no id or update key value was specified");
        continue;
      }
      records.add(idOrUpdateKey.forUpdate(record));
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
    List<IdOrUpdateKey> idOrUpdateKeys = new ArrayList<>();
    reader.setPage(page);
    KintoneColumnVisitor visitor =
        new KintoneColumnVisitor(
            reader,
            task.getDerivedColumns(),
            task.getColumnOptions(),
            task.getPreferNulls(),
            task.getIgnoreNulls(),
            task.getReduceKeyName().orElse(null),
            task.getUpdateKeyName().orElse(Id.FIELD));
    while (reader.nextRecord()) {
      Record record = new Record();
      IdOrUpdateKey idOrUpdateKey = new IdOrUpdateKey();
      visitor.setRecord(record);
      visitor.setIdOrUpdateKey(idOrUpdateKey);
      reader.getSchema().visitColumns(visitor);
      putWrongTypeFields(record);
      records.add(record);
      idOrUpdateKeys.add(idOrUpdateKey);
      if (records.size() == UPSERT_BATCH_SIZE) {
        upsert(records, idOrUpdateKeys);
        records.clear();
        idOrUpdateKeys.clear();
      }
    }
    if (!records.isEmpty()) {
      upsert(records, idOrUpdateKeys);
    }
  }

  private void upsert(List<Record> records, List<IdOrUpdateKey> idOrUpdateKeys) {
    if (records.size() != idOrUpdateKeys.size()) {
      throw new RuntimeException("records.size() != idOrUpdateKeys.size()");
    }
    Skip skip = task.getSkipIfNonExistingIdOrUpdateKey();
    String columnName = task.getUpdateKeyName().orElse(Id.FIELD);
    boolean isId = columnName.equals(Id.FIELD);
    List<String> existingValues =
        executeWithRetry(() -> getExistingValuesByIdOrUpdateKey(idOrUpdateKeys, columnName));
    List<Record> insertRecords = new ArrayList<>();
    List<RecordForUpdate> updateRecords = new ArrayList<>();
    for (int i = 0; i < records.size(); i++) {
      RecordForUpdate recordForUpdate = null;
      Record record = records.get(i);
      IdOrUpdateKey idOrUpdateKey = idOrUpdateKeys.get(i);
      if (existsRecord(existingValues, idOrUpdateKey)) {
        recordForUpdate = idOrUpdateKey.forUpdate(record);
      } else if (skip == Skip.ALWAYS && idOrUpdateKey.isPresent()) {
        LOGGER.warn(
            "Record skipped because non existing id or update key '"
                + idOrUpdateKey.getValue()
                + "' was specified");
        continue;
      } else if (skip == Skip.ALWAYS && !idOrUpdateKey.isPresent()) {
        LOGGER.warn("Record skipped because no id or update key value was specified");
        continue;
      } else if (skip == Skip.AUTO && idOrUpdateKey.isIdPresent()) {
        LOGGER.warn(
            "Record skipped because non existing id '"
                + idOrUpdateKey.getValue()
                + "' was specified");
        continue;
      } else if (skip == Skip.AUTO && !isId && !idOrUpdateKey.isUpdateKeyPresent()) {
        LOGGER.warn("Record skipped because no update key value was specified");
        continue;
      } else if (idOrUpdateKey.isIdPresent()) {
        LOGGER.warn(
            "Record inserted though non existing id '"
                + idOrUpdateKey.getValue()
                + "' was specified");
      } else if (!isId && !idOrUpdateKey.isUpdateKeyPresent()) {
        LOGGER.warn("Record inserted though no update key value was specified");
      }
      if (recordForUpdate != null) {
        updateRecords.add(recordForUpdate);
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

  private List<String> getExistingValuesByIdOrUpdateKey(
      List<IdOrUpdateKey> idOrUpdateKeys, String columnName) {
    List<String> queryValues =
        idOrUpdateKeys.stream()
            .filter(IdOrUpdateKey::isPresent)
            .map(k -> "\"" + k.getValue() + "\"")
            .collect(Collectors.toList());
    if (queryValues.isEmpty()) {
      return Collections.emptyList();
    }
    return columnName.equals(Id.FIELD)
        ? getExistingValuesById(queryValues)
        : getExistingValuesByUpdateKey(columnName, queryValues);
  }

  private List<String> getExistingValuesById(List<String> queryValues) {
    return getExistingValues(Id.FIELD, Record::getId, queryValues);
  }

  private List<String> getExistingValuesByUpdateKey(String columnName, List<String> queryValues) {
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

  private static boolean existsRecord(List<String> existingValues, IdOrUpdateKey idOrUpdateKey) {
    String value = toString(idOrUpdateKey.getValue());
    return value != null && existingValues.stream().anyMatch(v -> v.equals(value));
  }

  private static String toString(Object value) {
    return value == null
        ? null
        : value instanceof BigDecimal ? ((BigDecimal) value).toPlainString() : value.toString();
  }

  /**
   * Extracts record index from field name in Kintone API error response.
   *
   * @param fieldName Field name in format like "records[14].TINY_TEXT_10.value" or
   *     "records[0].field_code"
   * @return Record index (e.g., 14 for "records[14].TINY_TEXT_10.value"), or -1 if cannot be parsed
   *     <p>Examples: - "records[14].TINY_TEXT_10.value" -> returns 14 - "records[0].field_code" ->
   *     returns 0 - "records[123].some_field" -> returns 123 - "invalid_format" -> returns -1 -
   *     "records[].field" -> returns -1
   */
  private int extractRecordIndex(String fieldName) {
    if (!fieldName.startsWith("records[")) {
      return -1;
    }

    int startIdx = fieldName.indexOf('[') + 1;
    int endIdx = fieldName.indexOf(']');
    if (startIdx <= 0 || endIdx <= startIdx) {
      return -1;
    }

    try {
      return Integer.parseInt(fieldName.substring(startIdx, endIdx));
    } catch (NumberFormatException ex) {
      LOGGER.warn("Failed to parse record index from error field: " + fieldName);
      return -1;
    }
  }

  /**
   * Builds error message by combining base message with field-specific error details.
   *
   * @param baseMessage Base error message
   * @param fieldName Field name in format like "records[14].TINY_TEXT_10.value"
   * @param fieldError Field error JSON node
   * @return Constructed error message
   */
  private String buildErrorMessage(String baseMessage, String fieldName, JsonNode fieldError) {
    StringBuilder fullMessage = new StringBuilder(baseMessage);

    // Extract field name (].{field} part)
    int endIdx = fieldName.indexOf(']');
    if (endIdx != -1 && endIdx + 2 < fieldName.length()) {
      String affectedField = fieldName.substring(endIdx + 2);

      if (fieldError.has("messages")) {
        fieldError
            .get("messages")
            .forEach(
                msg -> {
                  fullMessage
                      .append(" ")
                      .append(affectedField)
                      .append(": ")
                      .append(msg.textValue());
                });
      }
    }

    return fullMessage.toString();
  }

  /** Logs Kintone API errors to file */
  private void logApiError(KintoneApiRuntimeException e, List<?> records) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode errorResponse = mapper.readTree(e.getContent());
      String errorCode = errorResponse.has("code") ? errorResponse.get("code").textValue() : "";
      String errorMessage =
          errorResponse.has("message") ? errorResponse.get("message").textValue() : "";
      // Parse errors field in error response
      if (errorResponse.has("errors")) {
        JsonNode errors = errorResponse.get("errors");

        // Pre-convert all records to maps for easier handling
        List<Map<String, Object>> recordMaps = convertRecordsToMaps(records);

        errors
            .fieldNames()
            .forEachRemaining(
                fieldName -> {
                  int recordIndex = extractRecordIndex(fieldName);
                  if (recordIndex == -1 || recordIndex >= recordMaps.size()) {
                    return;
                  }

                  Map<String, Object> recordData = recordMaps.get(recordIndex);

                  JsonNode fieldError = errors.get(fieldName);
                  String fullMessage = buildErrorMessage(errorMessage, fieldName, fieldError);

                  // Log error with map data directly
                  errorFileLogger.logError(recordData, errorCode, fullMessage);
                });
      } else {
        // Log as general error when errors field is not present
        LOGGER.warn(
            "Kintone API error without field-level errors: " + errorCode + " - " + errorMessage);
      }
    } catch (IOException ex) {
      LOGGER.error("Failed to parse Kintone API error response", ex);
    }
  }

  /** Converts list of records to list of maps for easier handling */
  private List<Map<String, Object>> convertRecordsToMaps(List<?> records) {
    List<Map<String, Object>> recordMaps = new ArrayList<>();

    for (Object recordObj : records) {
      Record record = null;

      if (recordObj instanceof Record) {
        record = (Record) recordObj;
      } else if (recordObj instanceof RecordForUpdate) {
        record = ((RecordForUpdate) recordObj).getRecord();
      }

      if (record != null) {
        Map<String, Object> recordData = extractAllFieldsFromRecord(record);
        recordMaps.add(recordData);
      } else {
        // Add empty map for unknown types to maintain index consistency
        recordMaps.add(new HashMap<>());
      }
    }

    return recordMaps;
  }

  /** Extracts all fields from a Record object to Map */
  private Map<String, Object> extractAllFieldsFromRecord(Record record) {
    Map<String, Object> fields = new HashMap<>();

    if (record == null) {
      return fields;
    }

    record
        .getFieldCodes(true)
        .forEach(
            fieldCode -> {
              Object value = record.getFieldValue(fieldCode);
              if (value != null) {
                Object actualValue = extractActualValue(value);
                fields.put(fieldCode, actualValue);
              } else {
                fields.put(fieldCode, null);
              }
            });

    return fields;
  }

  /** Extracts actual value from FieldValue object using pure reflection */
  private Object extractActualValue(Object value) {
    if (value == null) {
      return null;
    }

    Object extractedValue = tryExtractValue(value);
    if (extractedValue != null) {
      return extractedValue;
    }

    // Fallback for unknown types
    return value.toString();
  }

  /** Attempts to extract value using reflection with smart handling of different field types */
  private Object tryExtractValue(Object value) {
    try {
      // First try getValue() method (most common case)
      Method getValueMethod = value.getClass().getMethod("getValue");
      Object result = getValueMethod.invoke(value);

      // Convert to string if not null for consistency
      return result != null ? result.toString() : null;

    } catch (Exception e) {
      // If getValue() doesn't exist or fails, try getValues()
      try {
        Method getValuesMethod = value.getClass().getMethod("getValues");
        Object valuesResult = getValuesMethod.invoke(value);

        // Handle special cases that need code extraction
        return extractCodesIfNeeded(value, valuesResult);

      } catch (Exception e2) {
        // Neither method exists or accessible
        return null;
      }
    }
  }

  /** Extracts codes from select field values if they have a getCode() method */
  private Object extractCodesIfNeeded(Object fieldValue, Object valuesResult) {
    if (valuesResult == null) {
      return null;
    }

    // If it's a simple list (like CheckBox, MultiSelect), return as-is
    if (!(valuesResult instanceof List)) {
      return valuesResult;
    }

    @SuppressWarnings("unchecked")
    List<Object> valuesList = (List<Object>) valuesResult;

    if (valuesList.isEmpty()) {
      return valuesList;
    }

    // Check if the first item has a getCode() method
    Object firstItem = valuesList.get(0);
    try {
      Method getCodeMethod = firstItem.getClass().getMethod("getCode");

      // Extract codes from all items
      List<String> codes = new ArrayList<>();
      for (Object item : valuesList) {
        String code = (String) getCodeMethod.invoke(item);
        codes.add(code);
      }
      return codes;

    } catch (Exception e) {
      // No getCode() method, return the original list
      return valuesList;
    }
  }
}
