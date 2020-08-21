package org.embulk.output.kintone;

import com.kintone.client.model.record.CheckBoxFieldValue;
import com.kintone.client.model.record.DateFieldValue;
import com.kintone.client.model.record.DateTimeFieldValue;
import com.kintone.client.model.record.DropDownFieldValue;
import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.FieldValue;
import com.kintone.client.model.record.LinkFieldValue;
import com.kintone.client.model.record.MultiLineTextFieldValue;
import com.kintone.client.model.record.NumberFieldValue;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.SingleLineTextFieldValue;
import com.kintone.client.model.record.UpdateKey;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

public class KintoneColumnVisitor
        implements ColumnVisitor
{
    private PageReader pageReader;
    private Record record;
    private UpdateKey updateKey;
    private Map<String, KintoneColumnOption> columnOptions;

    public KintoneColumnVisitor(PageReader pageReader,
                                Map<String, KintoneColumnOption> columnOptions)
    {
        this.pageReader = pageReader;
        this.columnOptions = columnOptions;
    }

    public void setRecord(Record record)
    {
        this.record = record;
    }

    public void setUpdateKey(UpdateKey updateKey)
    {
        this.updateKey = updateKey;
    }

    private void setValue(String fieldCode, Object value, FieldType type, boolean isUpdateKey)
    {
        if (value == null) {
            return;
        }

        if (isUpdateKey) {
            updateKey
                .setField(fieldCode)
                .setValue(String.valueOf(value));
        }
        else {
            String stringValue = String.valueOf(value);
            FieldValue fieldValue = null;
            switch (type) {
                case NUMBER:
                    fieldValue = new NumberFieldValue(new BigDecimal(stringValue));
                    break;
                case MULTI_LINE_TEXT:
                    fieldValue = new MultiLineTextFieldValue(stringValue);
                    break;
                case DROP_DOWN:
                    fieldValue = new DropDownFieldValue(stringValue);
                    break;
                case LINK:
                    fieldValue = new LinkFieldValue(stringValue);
                    break;
                default:
                    fieldValue = new SingleLineTextFieldValue(stringValue);
            }
            record.putField(fieldCode, fieldValue);
        }
    }

    private void setTimestampValue(String fieldCode, Instant instant, ZoneId zoneId, FieldType type)
    {
        FieldValue fieldValue = null;
        ZonedDateTime datetime = instant.atZone(zoneId);
        switch (type) {
            case DATE:
                fieldValue = new DateFieldValue(datetime.toLocalDate());
                break;
            case DATETIME:
                fieldValue = new DateTimeFieldValue(datetime);
        }
        record.putField(fieldCode, fieldValue);
    }

    private void setCheckBoxValue(String fieldCode, Object value, String valueSeparator)
    {
        String str = String.valueOf(value);
        record.putField(
            fieldCode,
            new CheckBoxFieldValue(str.split(valueSeparator, 0))
        );
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

    private ZoneId getZoneId(Column column)
    {
        KintoneColumnOption option = columnOptions.get(column.getName());
        return ZoneId.of(option.getTimezone().get());
    }

    private boolean isUpdateKey(Column column)
    {
        KintoneColumnOption option = columnOptions.get(column.getName());
        if (option == null) {
            return false;
        }
        return option.getUpdateKey();
    }

    private String getValueSeparator(Column column)
    {
        KintoneColumnOption option = columnOptions.get(column.getName());
        if (option == null) {
            return ",";
        }
        return option.getValueSeparator();
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
        if (type == FieldType.CHECK_BOX) {
            setCheckBoxValue(fieldCode, pageReader.getString(column), getValueSeparator(column));
            return;
        }
        setValue(fieldCode, pageReader.getString(column), type, isUpdateKey(column));
    }

    @Override
    public void timestampColumn(Column column)
    {
        Timestamp value = pageReader.getTimestamp(column);
        if (value == null) {
            return;
        }

        String fieldCode = getFieldCode(column);
        FieldType type = getType(column, FieldType.DATETIME);
        ZoneId zoneId = getZoneId(column);
        if (type == FieldType.DATETIME) {
            zoneId = ZoneId.of("UTC");
        }
        setTimestampValue(fieldCode, value.getInstant(), zoneId, type);
    }

    @Override
    public void jsonColumn(Column column)
    {
        String fieldCode = getFieldCode(column);
        FieldType type = getType(column, FieldType.MULTI_LINE_TEXT);
        setValue(fieldCode, pageReader.getJson(column), type, isUpdateKey(column));
    }
}
