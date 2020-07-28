package org.embulk.output.kintone;

import com.cybozu.kintone.client.model.app.form.FieldType;
import com.cybozu.kintone.client.model.record.field.FieldValue;
import com.cybozu.kintone.client.model.record.RecordUpdateKey;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.Map;

public class KintoneColumnVisitor
        implements ColumnVisitor
{
    private PageReader pageReader;
    private HashMap<String, FieldValue> record;
    private HashMap<String, String> updateKey;
    private Map<String, KintoneColumnOption> columnOptions;

    public KintoneColumnVisitor(PageReader pageReader,
                                Map<String, KintoneColumnOption> columnOptions)
    {
        this.pageReader = pageReader;
        this.columnOptions = columnOptions;
    }

    public void setRecord(HashMap<String, FieldValue> record)
    {
        this.record = record;
    }

    public void setUpdateKey(HashMap<String, String> updateKey)
    {
        this.updateKey = updateKey;
    }

    private void setValue(String fieldCode, Object value, FieldType type, boolean isUpdateKey)
    {
        if (value == null) {
            return;
        }

        if (isUpdateKey) {
            updateKey.put("fieldCode", fieldCode);
            updateKey.put("fieldValue", String.valueOf(value));
        }
        else {
            FieldValue fieldValue = new FieldValue();
            fieldValue.setType(type);
            record.put(fieldCode, fieldValue);
            fieldValue.setValue(String.valueOf(value));
        }
    }

    private FieldType getType(Column column, FieldType defaultType)
    {
        KintoneColumnOption option = columnOptions.get(column.getName());
        if (option == null) {
            return defaultType;
        }
        else {
            return FieldType.valueOf(option.getType());
        }
    }

    private String getFieldCode(Column column)
    {
        KintoneColumnOption option = columnOptions.get(column.getName());
        if (option == null) {
            return column.getName();
        }
        else {
            return option.getFieldCode();
        }
    }

    private DateTimeZone getTimezone(Column column)
    {
        KintoneColumnOption option = columnOptions.get(column.getName());
        return DateTimeZone.forID(option.getTimezone().get());
    }

    private boolean isUpdateKey(Column column)
    {
        KintoneColumnOption option = columnOptions.get(column.getName());
        if (option == null) {
            return false;
        }
        return option.getUpdateKey();
    }

    @Override
    public void booleanColumn(Column column)
    {
        String fieldCode = getFieldCode(column);
        FieldType type = getType(column, FieldType.NUMBER);
        setValue(fieldCode, pageReader.getBoolean(column), type, isUpdateKey(column));
    }

    @Override
    public void longColumn(Column column)
    {
        String fieldCode = getFieldCode(column);
        FieldType type = getType(column, FieldType.NUMBER);
        setValue(fieldCode, pageReader.getLong(column), type, isUpdateKey(column));
    }

    @Override
    public void doubleColumn(Column column)
    {
        String fieldCode = getFieldCode(column);
        FieldType type = getType(column, FieldType.NUMBER);
        setValue(fieldCode, pageReader.getDouble(column), type, isUpdateKey(column));
    }

    @Override
    public void stringColumn(Column column)
    {
        String fieldCode = getFieldCode(column);
        FieldType type = getType(column, FieldType.MULTI_LINE_TEXT);
        setValue(fieldCode, pageReader.getString(column), type, isUpdateKey(column));
    }

    @Override
    public void timestampColumn(Column column)
    {
        String fieldCode = getFieldCode(column);
        FieldType type = getType(column, FieldType.DATETIME);
        Timestamp value = pageReader.getTimestamp(column);
        if (value == null) {
            return;
        }
        switch (type) {
            case DATE: {
                String format = "%Y-%m-%d";
                DateTimeZone timezone = getTimezone(column);
                TimestampFormatter formatter = new TimestampFormatter(format, timezone);
                String date = formatter.format(value);
                setValue(fieldCode, date, type, isUpdateKey(column));
                break;
            }
            case DATETIME: {
                String format = "%Y-%m-%dT%H:%M:%S%z";
                DateTimeZone timezone = DateTimeZone.forID("UTC");
                TimestampFormatter formatter = new TimestampFormatter(format, timezone);
                String dateTime = formatter.format(value);
                setValue(fieldCode, dateTime, type, isUpdateKey(column));
                break;
            }
            default: {
                setValue(fieldCode, value, type, isUpdateKey(column));
            }
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        String fieldCode = getFieldCode(column);
        FieldType type = getType(column, FieldType.MULTI_LINE_TEXT);
        setValue(fieldCode, pageReader.getJson(column), type, isUpdateKey(column));
    }
}
