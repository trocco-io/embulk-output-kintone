package org.embulk.output.kintone;

import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.UpdateKey;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.validation.UnexpectedTypeException;
import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public class KintoneColumnVisitorVerifier {
  private final Schema schema;
  private final Map<String, KintoneColumnOption> options;
  private final PageReader reader;
  private final KintoneColumnVisitor visitor;

  public KintoneColumnVisitorVerifier(
      Schema schema,
      Set<Column> derived,
      Map<String, KintoneColumnOption> options,
      String reduceKeyName,
      String updateKeyName,
      Page page) {
    this(schema, derived, options, false, false, reduceKeyName, updateKeyName, page);
  }

  public KintoneColumnVisitorVerifier(
      Schema schema,
      Set<Column> derived,
      Map<String, KintoneColumnOption> options,
      boolean preferNulls,
      boolean ignoreNulls,
      String reduceKeyName,
      String updateKeyName,
      Page page) {
    this.schema = schema;
    this.options = options;
    reader = new PageReader(schema);
    reader.setPage(page);
    visitor =
        new KintoneColumnVisitor(
            reader, derived, options, preferNulls, ignoreNulls, reduceKeyName, updateKeyName);
  }

  public void verify() {
    verify((record, updateKey) -> {});
  }

  public void verify(BiConsumer<Record, UpdateKey> consumer) {
    verify(consumer, false);
  }

  public void verify(BiConsumer<Record, UpdateKey> consumer, boolean nullable) {
    if (!reader.nextRecord()) {
      throw new IllegalStateException();
    }
    Record record = new Record();
    visitor.setRecord(record);
    UpdateKey updateKey = new UpdateKey();
    visitor.setUpdateKey(updateKey);
    schema.visitColumns(visitor);
    schema.getColumns().forEach(column -> verify(record, column, nullable));
    consumer.accept(record, updateKey);
  }

  private void verify(Record record, Column column, boolean nullable) {
    FieldType expected = FieldType.valueOf(options.get(column.getName()).getType());
    FieldType actual = record.getFieldType(column.getName());
    if (actual == null && nullable) {
      return;
    }
    if (expected != actual) {
      throw new UnexpectedTypeException(
          String.format(
              "%s: Expected type is %s, but actual type is %s%n",
              column.getName(), expected, actual));
    }
  }
}
