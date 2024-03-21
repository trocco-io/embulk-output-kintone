package org.embulk.output.kintone;

import com.kintone.client.model.record.FieldValue;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.UpdateKey;
import java.time.Instant;
import java.util.Map;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class KintoneColumnVisitor implements ColumnVisitor {
  private final PageReader reader;
  private final Map<String, KintoneColumnOption> options;
  private final boolean preferNulls;
  private final boolean ignoreNulls;
  private final String updateKeyName;
  private Record record;
  private UpdateKey updateKey;

  public KintoneColumnVisitor(
      PageReader reader,
      Map<String, KintoneColumnOption> options,
      boolean preferNulls,
      boolean ignoreNulls) {
    this(reader, options, preferNulls, ignoreNulls, null);
  }

  public KintoneColumnVisitor(
      PageReader reader,
      Map<String, KintoneColumnOption> options,
      boolean preferNulls,
      boolean ignoreNulls,
      String updateKeyName) {
    this.reader = reader;
    this.options = options;
    this.preferNulls = preferNulls;
    this.ignoreNulls = ignoreNulls;
    this.updateKeyName = updateKeyName;
  }

  public void setRecord(Record record) {
    this.record = record;
  }

  public void setUpdateKey(UpdateKey updateKey) {
    this.updateKey = updateKey;
  }

  @Override
  public void booleanColumn(Column column) {
    if (isIgnoreNull(column)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = KintoneColumnType.getType(option, KintoneColumnType.NUMBER);
    String fieldCode = getFieldCode(column);
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
    if (isIgnoreNull(column)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = KintoneColumnType.getType(option, KintoneColumnType.NUMBER);
    String fieldCode = getFieldCode(column);
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
    if (isIgnoreNull(column)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = KintoneColumnType.getType(option, KintoneColumnType.NUMBER);
    String fieldCode = getFieldCode(column);
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
    if (isIgnoreNull(column)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType defaultType =
        updateKey != null ? KintoneColumnType.SINGLE_LINE_TEXT : KintoneColumnType.MULTI_LINE_TEXT;
    KintoneColumnType type = KintoneColumnType.getType(option, defaultType);
    String fieldCode = getFieldCode(column);
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
    if (isIgnoreNull(column)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = KintoneColumnType.getType(option, KintoneColumnType.DATETIME);
    String fieldCode = getFieldCode(column);
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
    if (isIgnoreNull(column)) {
      return;
    }
    KintoneColumnOption option = getOption(column);
    UpdateKey updateKey = getUpdateKey(column);
    KintoneColumnType type = KintoneColumnType.getType(option, KintoneColumnType.MULTI_LINE_TEXT);
    String fieldCode = getFieldCode(column);
    if (isPreferNull(column)) {
      setNull(type, fieldCode, updateKey);
    } else if (reader.isNull(column)) {
      setJson(type, fieldCode, updateKey, ValueFactory.newString(""), option);
    } else {
      setJson(type, fieldCode, updateKey, reader.getJson(column), option);
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

  private KintoneColumnOption getOption(Column column) {
    return options.get(column.getName());
  }

  private UpdateKey getUpdateKey(Column column) {
    return updateKeyName != null && updateKeyName.equals(column.getName()) ? updateKey : null;
  }

  private boolean isIgnoreNull(Column column) {
    return ignoreNulls && reader.isNull(column);
  }

  private boolean isPreferNull(Column column) {
    return preferNulls && reader.isNull(column);
  }
}
