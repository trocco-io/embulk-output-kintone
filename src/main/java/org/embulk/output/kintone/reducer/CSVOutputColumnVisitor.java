package org.embulk.output.kintone.reducer;

import java.io.IOException;
import org.apache.commons.csv.CSVPrinter;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;

public class CSVOutputColumnVisitor implements ColumnVisitor {
  private final PageReader reader;
  private final CSVPrinter printer;

  public CSVOutputColumnVisitor(PageReader reader, CSVPrinter printer) {
    this.reader = reader;
    this.printer = printer;
  }

  @Override
  public void booleanColumn(Column column) {
    if (reader.isNull(column)) {
      print(null);
    } else {
      print(reader.getBoolean(column));
    }
  }

  @Override
  public void longColumn(Column column) {
    if (reader.isNull(column)) {
      print(null);
    } else {
      print(reader.getLong(column));
    }
  }

  @Override
  public void doubleColumn(Column column) {
    if (reader.isNull(column)) {
      print(null);
    } else {
      print(reader.getDouble(column));
    }
  }

  @Override
  public void stringColumn(Column column) {
    if (reader.isNull(column)) {
      print(null);
    } else {
      print(reader.getString(column));
    }
  }

  @Override
  public void timestampColumn(Column column) {
    if (reader.isNull(column)) {
      print(null);
    } else {
      print(reader.getTimestamp(column).getInstant());
    }
  }

  @Override
  public void jsonColumn(Column column) {
    if (reader.isNull(column)) {
      print(null);
    } else {
      print(reader.getJson(column).toJson());
    }
  }

  private void print(Object value) {
    try {
      printer.print(value);
    } catch (IOException e) {
      throw new ReduceException(e);
    }
  }
}
