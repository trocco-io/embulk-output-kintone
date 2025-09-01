package org.embulk.output.kintone;

import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.FieldValue;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.UpdateKey;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.embulk.config.ConfigException;
import org.embulk.output.kintone.record.Id;
import org.embulk.output.kintone.record.IdOrUpdateKey;
import org.embulk.output.kintone.util.Lazy;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KintoneColumnVisitor implements ColumnVisitor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final List<String> BUILTIN_FIELD_CODES = Arrays.asList(Id.FIELD, "$revision");
  private final Lazy<KintoneClient> client;
  private final PageReader reader;
  private final Set<Column> derived;
  private final Map<String, KintoneColumnOption> options;
  private final boolean preferNulls;
  private final boolean ignoreNulls;
  private final String reduceKeyName;
  private final String updateKeyName;
  private Record record;
  private IdOrUpdateKey idOrUpdateKey;

  public KintoneColumnVisitor(
      Lazy<KintoneClient> client,
      PageReader reader,
      Set<Column> derived,
      Map<String, KintoneColumnOption> options,
      boolean preferNulls,
      boolean ignoreNulls,
      String reduceKeyName) {
    this(client, reader, derived, options, preferNulls, ignoreNulls, reduceKeyName, null);
  }

  public KintoneColumnVisitor(
      Lazy<KintoneClient> client,
      PageReader reader,
      Set<Column> derived,
      Map<String, KintoneColumnOption> options,
      boolean preferNulls,
      boolean ignoreNulls,
      String reduceKeyName,
      String updateKeyName) {
    this.client = client;
    this.reader = reader;
    this.derived = derived;
    this.options = options;
    this.preferNulls = preferNulls;
    this.ignoreNulls = ignoreNulls;
    this.reduceKeyName = reduceKeyName;
    this.updateKeyName = updateKeyName;
  }

  public void setRecord(Record record) {
    this.record = record;
  }

  public void setIdOrUpdateKey(IdOrUpdateKey idOrUpdateKey) {
    this.idOrUpdateKey = idOrUpdateKey;
  }

  @Override
  public void booleanColumn(Column column) {
    if (isReduced(column) || isIgnoreNull(column)) {
      return;
    }
    String fieldCode = getFieldCode(column);
    if (isBuiltin(fieldCode)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = getType(option, column.getName());
    if (isPreferNull(column)) {
      setNull(type, fieldCode, updateKey);
    } else if (reader.isNull(column)) {
      setBoolean(type, fieldCode, updateKey, false, option);
    } else {
      setBoolean(type, fieldCode, updateKey, reader.getBoolean(column), option);
    }
  }

  @Override
  public void longColumn(Column column) {
    if (isReduced(column) || isIgnoreNull(column)) {
      return;
    }
    Id id = getId(column);
    if (id != null) {
      setIdValue(id, column);
      return;
    }
    String fieldCode = getFieldCode(column);
    if (isBuiltin(fieldCode)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = getType(option, column.getName());
    if (isPreferNull(column)) {
      setNull(type, fieldCode, updateKey);
    } else if (reader.isNull(column)) {
      setLong(type, fieldCode, updateKey, 0, option);
    } else {
      setLong(type, fieldCode, updateKey, reader.getLong(column), option);
    }
  }

  @Override
  public void doubleColumn(Column column) {
    if (isReduced(column) || isIgnoreNull(column)) {
      return;
    }
    String fieldCode = getFieldCode(column);
    if (isBuiltin(fieldCode)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = getType(option, column.getName());
    if (isPreferNull(column)) {
      setNull(type, fieldCode, updateKey);
    } else if (reader.isNull(column)) {
      setDouble(type, fieldCode, updateKey, 0, option);
    } else {
      setDouble(type, fieldCode, updateKey, reader.getDouble(column), option);
    }
  }

  @Override
  public void stringColumn(Column column) {
    if (isReduced(column) || isIgnoreNull(column)) {
      return;
    }
    String fieldCode = getFieldCode(column);
    if (isBuiltin(fieldCode)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    LOGGER.debug("column name: {}, type: {}", column.getName(), column.getType());
    KintoneColumnType type = getType(option, column.getName());
    if (isPreferNull(column)) {
      setNull(type, fieldCode, updateKey);
    } else if (reader.isNull(column)) {
      setString(type, fieldCode, updateKey, "", option);
    } else {
      setString(type, fieldCode, updateKey, reader.getString(column), option);
    }
  }

  @Override
  public void timestampColumn(Column column) {
    if (isReduced(column) || isIgnoreNull(column)) {
      return;
    }
    String fieldCode = getFieldCode(column);
    if (isBuiltin(fieldCode)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = getType(option, column.getName());
    if (isPreferNull(column)) {
      setNull(type, fieldCode, updateKey);
    } else if (reader.isNull(column)) {
      setTimestamp(type, fieldCode, updateKey, Timestamp.ofInstant(Instant.EPOCH), option);
    } else {
      setTimestamp(type, fieldCode, updateKey, reader.getTimestamp(column), option);
    }
  }

  @Override
  public void jsonColumn(Column column) {
    if (isReduced(column) || isIgnoreNull(column)) {
      return;
    }
    String fieldCode = getFieldCode(column);
    if (isBuiltin(fieldCode)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = getType(option, column.getName());
    if (isPreferNull(column)) {
      setNull(type, fieldCode, updateKey);
    } else if (reader.isNull(column)) {
      setJson(type, fieldCode, updateKey, ValueFactory.newString(""), option);
    } else {
      setJson(type, fieldCode, updateKey, reader.getJson(column), option);
    }
  }

  private void setIdValue(Id id, Column column) {
    if (isPreferNull(column)) {
      id.setValue(null);
    } else if (reader.isNull(column)) {
      id.setValue(0L);
    } else {
      id.setValue(reader.getLong(column));
    }
  }

  private void setNull(KintoneColumnType type, String fieldCode, UpdateKey updateKey) {
    if (updateKey != null) {
      type.setUpdateKey(updateKey, fieldCode);
    }
    record.putField(fieldCode, type.getFieldValue());
  }

  private void setBoolean(
      KintoneColumnType type,
      String fieldCode,
      UpdateKey updateKey,
      boolean value,
      KintoneColumnOption option) {
    FieldValue fieldValue = type.getFieldValue(value, option);
    if (updateKey != null) {
      type.setUpdateKey(updateKey, fieldCode, fieldValue);
    }
    record.putField(fieldCode, fieldValue);
  }

  private void setLong(
      KintoneColumnType type,
      String fieldCode,
      UpdateKey updateKey,
      long value,
      KintoneColumnOption option) {
    FieldValue fieldValue = type.getFieldValue(value, option);
    if (updateKey != null) {
      type.setUpdateKey(updateKey, fieldCode, fieldValue);
    }
    record.putField(fieldCode, fieldValue);
  }

  private void setDouble(
      KintoneColumnType type,
      String fieldCode,
      UpdateKey updateKey,
      double value,
      KintoneColumnOption option) {
    FieldValue fieldValue = type.getFieldValue(value, option);
    if (updateKey != null) {
      type.setUpdateKey(updateKey, fieldCode, fieldValue);
    }
    record.putField(fieldCode, fieldValue);
  }

  private void setString(
      KintoneColumnType type,
      String fieldCode,
      UpdateKey updateKey,
      String value,
      KintoneColumnOption option) {
    FieldValue fieldValue = type.getFieldValue(value, option);
    if (updateKey != null) {
      type.setUpdateKey(updateKey, fieldCode, fieldValue);
    }
    record.putField(fieldCode, fieldValue);
  }

  private void setTimestamp(
      KintoneColumnType type,
      String fieldCode,
      UpdateKey updateKey,
      Timestamp value,
      KintoneColumnOption option) {
    FieldValue fieldValue = type.getFieldValue(value, option);
    if (updateKey != null) {
      type.setUpdateKey(updateKey, fieldCode, fieldValue);
    }
    record.putField(fieldCode, fieldValue);
  }

  private void setJson(
      KintoneColumnType type,
      String fieldCode,
      UpdateKey updateKey,
      Value value,
      KintoneColumnOption option) {
    FieldValue fieldValue = type.getFieldValue(value, option);
    if (updateKey != null) {
      type.setUpdateKey(updateKey, fieldCode, fieldValue);
    }
    record.putField(fieldCode, fieldValue);
  }

  private String getFieldCode(Column column) {
    KintoneColumnOption option = getOption(column);
    return option != null ? option.getFieldCode() : column.getName();
  }

  public KintoneColumnType getType(KintoneColumnOption option, String fieldCode) {
    if (option != null) {
      return KintoneColumnType.valueOf(option.getType());
    }

    FieldType fieldType = this.client.get().getFieldType(fieldCode);
    if (fieldType == null) {
      throw new ConfigException("The field '" + fieldCode + "' does not exist.");
    }

    return KintoneColumnType.valueOf(fieldType.toString());
  }

  private KintoneColumnOption getOption(Column column) {
    return options.get(column.getName());
  }

  private Id getId(Column column) {
    return column.getName().equals(updateKeyName) && updateKeyName.equals(Id.FIELD)
        ? idOrUpdateKey.getId()
        : null;
  }

  private UpdateKey getUpdateKey(Column column) {
    return column.getName().equals(updateKeyName) && !updateKeyName.equals(Id.FIELD)
        ? idOrUpdateKey.getUpdateKey()
        : null;
  }

  private boolean isReduced(Column column) {
    return reduceKeyName != null && column.getName().matches("^.*\\..*$");
  }

  private boolean isDerived(Column column) {
    return reduceKeyName != null && derived.contains(column);
  }

  private boolean isIgnoreNull(Column column) {
    return ignoreNulls && reader.isNull(column);
  }

  private boolean isPreferNull(Column column) {
    return preferNulls && reader.isNull(column);
  }

  private static boolean isBuiltin(String fieldCode) {
    return BUILTIN_FIELD_CODES.contains(fieldCode);
  }
}
