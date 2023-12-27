package org.embulk.output.kintone;

import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.UpdateKey;
import java.util.Map;
import java.util.function.BiConsumer;
import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public class KintoneColumnVisitorVerifier {
  private final Schema schema;
  private final Map<String, KintoneColumnOption> columnOptions;
  private final PageReader pageReader;
  private final KintoneColumnVisitor visitor;

  public KintoneColumnVisitorVerifier(
      Schema schema,
      Map<String, KintoneColumnOption> columnOptions,
      String updateKeyName,
      Page page) {
    this.schema = schema;
    this.columnOptions = columnOptions;
    pageReader = new PageReader(schema);
    pageReader.setPage(page);
    visitor = new KintoneColumnVisitor(pageReader, columnOptions, updateKeyName);
  }

  public void verify(BiConsumer<Record, UpdateKey> consumer) {
    if (!pageReader.nextRecord()) {
      throw new IllegalStateException();
    }
    Record record = new Record();
    visitor.setRecord(record);
    UpdateKey updateKey = new UpdateKey();
    visitor.setUpdateKey(updateKey);
    schema.visitColumns(visitor);
    schema.getColumns().forEach(column -> verify(record, column));
    consumer.accept(record, updateKey);
  }

  private void verify(Record record, Column column) {
    FieldType expected = FieldType.valueOf(columnOptions.get(column.getName()).getType());
    FieldType actual = record.getFieldType(column.getName());
    if (!expected.equals(actual)) {
      System.out.printf(
          "%s: Expected type is %s, but actual type is %s%n", column.getName(), expected, actual);
    }
  }
}
