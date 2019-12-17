package org.embulk.output.kintone;

import com.cybozu.kintone.client.model.app.form.FieldType;
import com.cybozu.kintone.client.model.record.field.FieldValue;
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
    private HashMap record;
    private Map<String, KintoneColumnOption> columnOptions;

    public KintoneColumnVisitor(PageReader pageReader,
                                Map<String, KintoneColumnOption> columnOptions)
    {
        this.pageReader = pageReader;
        this.columnOptions = columnOptions;
    }

    public void setRecord(HashMap record)
    {
        this.record = record;
    }

    private void setValue(Column column, Object value, FieldType type)
    {
        if (value == null) {
            return;
        }
        FieldValue fieldValue = new FieldValue();
        KintoneColumnOption option = columnOptions.get(column.getName());
        if (option == null) {
            fieldValue.setType(type);
            record.put(column.getName(), fieldValue);
        }
        else {
            fieldValue.setType(FieldType.valueOf(option.getType()));
            record.put(option.getFieldCode(), fieldValue);
        }
        fieldValue.setValue(String.valueOf(value));
    }

    @Override
    public void booleanColumn(Column column)
    {
        setValue(column, pageReader.getBoolean(column), FieldType.NUMBER);
    }

    @Override
    public void longColumn(Column column)
    {
        setValue(column, pageReader.getLong(column), FieldType.NUMBER);
    }

    @Override
    public void doubleColumn(Column column)
    {
        setValue(column, pageReader.getDouble(column), FieldType.NUMBER);
    }

    @Override
    public void stringColumn(Column column)
    {
        setValue(column, pageReader.getString(column), FieldType.MULTI_LINE_TEXT);
    }

    @Override
    public void timestampColumn(Column column)
    {
        Timestamp value = pageReader.getTimestamp(column);
        if (value == null) {
            return;
        }
        KintoneColumnOption option = columnOptions.get(column.getName());
        FieldType fieldType;
        if (option == null) {
            fieldType = FieldType.DATETIME;
        }
        else {
            fieldType = FieldType.valueOf(option.getType());
        }

        switch (fieldType) {
            case DATE: {
                String format = "%Y-%m-%d";
                DateTimeZone timezone = DateTimeZone.forID(option.getTimezone().get());
                TimestampFormatter formatter = new TimestampFormatter(format, timezone);
                String date = formatter.format(value);
                setValue(column, date, fieldType);
                break;
            }
            case DATETIME: {
                String format = "%Y-%m-%dT%H:%M:%S%z";
                DateTimeZone timezone = DateTimeZone.forID("UTC");
                TimestampFormatter formatter = new TimestampFormatter(format, timezone);
                String date = formatter.format(value);
                setValue(column, date, fieldType);
                break;
            }
            default: {
                setValue(column, value, fieldType);
                break;
            }
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        setValue(column, pageReader.getJson(column), FieldType.MULTI_LINE_TEXT);
    }
}
