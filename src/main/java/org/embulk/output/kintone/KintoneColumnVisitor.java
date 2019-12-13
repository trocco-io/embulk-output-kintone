package org.embulk.output.kintone;

import com.cybozu.kintone.client.model.app.form.FieldType;
import com.cybozu.kintone.client.model.record.field.FieldValue;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;

import java.text.SimpleDateFormat;
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

    private void setValue(Column column, Object value)
    {
        if (value == null) {
            return;
        }
        KintoneColumnOption option = columnOptions.get(column.getName());
        if (option == null) {
            return;
        }

        FieldValue fieldValue = new FieldValue();
        fieldValue.setType(FieldType.valueOf(option.getType()));
        fieldValue.setValue(String.valueOf(value));
        record.put(option.getFieldCode(), fieldValue);
    }

    @Override
    public void booleanColumn(Column column)
    {
        setValue(column, pageReader.getBoolean(column));
    }

    @Override
    public void longColumn(Column column)
    {
        setValue(column, pageReader.getLong(column));
    }

    @Override
    public void doubleColumn(Column column)
    {
        setValue(column, pageReader.getDouble(column));
    }

    @Override
    public void stringColumn(Column column)
    {
        setValue(column, pageReader.getString(column));
    }

    @Override
    public void timestampColumn(Column column)
    {
        Timestamp value = pageReader.getTimestamp(column);
        if (value == null) {
            return;
        }
        KintoneColumnOption option = columnOptions.get(column.getName());
        if (option == null) {
            return;
        }
        FieldType fieldType = FieldType.valueOf(option.getType());

        switch (fieldType) {
            case DATE:
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String date = sdf.format(value);
                setValue(column, date);
        }
//        Timestamp value = pageReader.getTimestamp(column);
//        if (value == null) {
//            return;
//        }
//        KintoneColumnOption option = columnOptions.get(column.getName());
//        if (option == null) {
//            return;
//        }
//
//        FieldType fieldType = FieldType.valueOf(option.getType());
//        Date date = new Date(value.toEpochMilli());
//
//        switch (fieldType) {
//            case DATE:
//                record.setDate(option.getFieldCode(), date);
//                break;
//            case DATETIME:
//                record.setDateTime(option.getFieldCode(), date);
//                break;
//            default:
//                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss\'Z\'");
//                Field field = new Field(option.getFieldCode(), fieldType, format.format(date));
//                record.addField(option.getFieldCode(), field);
//        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        setValue(column, pageReader.getJson(column));
    }
}
