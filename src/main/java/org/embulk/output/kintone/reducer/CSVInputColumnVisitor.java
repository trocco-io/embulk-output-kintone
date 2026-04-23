package org.embulk.output.kintone.reducer;

import static org.embulk.output.kintone.util.Compatibility.PARSER;
import static org.embulk.output.kintone.util.Compatibility.setJson;
import static org.embulk.output.kintone.util.Compatibility.setTimestamp;

import java.time.Instant;
import java.util.List;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;

public class CSVInputColumnVisitor implements ColumnVisitor {
  private final PageBuilder builder;
  private final List<String> values;

  public CSVInputColumnVisitor(PageBuilder builder, List<String> values) {
    this.builder = builder;
    this.values = values;
  }

  @Override
  public void booleanColumn(Column column) {
    String value = values.get(column.getIndex());
    if (value == null) {
      builder.setNull(column);
    } else {
      builder.setBoolean(column, Boolean.parseBoolean(value));
    }
  }

  @Override
  public void longColumn(Column column) {
    String value = values.get(column.getIndex());
    if (value == null) {
      builder.setNull(column);
    } else {
      builder.setLong(column, Long.parseLong(value));
    }
  }

  @Override
  public void doubleColumn(Column column) {
    String value = values.get(column.getIndex());
    if (value == null) {
      builder.setNull(column);
    } else {
      builder.setDouble(column, Double.parseDouble(value));
    }
  }

  @Override
  public void stringColumn(Column column) {
    String value = values.get(column.getIndex());
    if (value == null) {
      builder.setNull(column);
    } else {
      builder.setString(column, value);
    }
  }

  @Override
  public void timestampColumn(Column column) {
    String value = values.get(column.getIndex());
    if (value == null) {
      builder.setNull(column);
    } else {
      setTimestamp(builder, column, Instant.parse(value));
    }
  }

  @Override
  public void jsonColumn(Column column) {
    String value = values.get(column.getIndex());
    if (value == null) {
      builder.setNull(column);
    } else {
      setJson(builder, column, PARSER.parse(value));
    }
  }
}
