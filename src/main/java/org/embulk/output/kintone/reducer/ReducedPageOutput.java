package org.embulk.output.kintone.reducer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.commons.csv.CSVPrinter;
import org.embulk.config.TaskReport;
import org.embulk.output.kintone.KintoneOutputPlugin;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducedPageOutput implements TransactionalPageOutput {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final PageReader reader;
  private final File file;
  private final CSVPrinter printer;
  private final ColumnVisitor visitor;

  public ReducedPageOutput(Schema schema, int taskIndex) {
    reader = new PageReader(schema);
    file = file(taskIndex);
    printer = printer(file);
    visitor = new CSVOutputColumnVisitor(reader, printer);
  }

  @Override
  public void add(Page page) {
    reader.setPage(page);
    while (reader.nextRecord()) visitColumns();
  }

  @Override
  public void finish() {}

  @Override
  public void close() {
    reader.close();
    close(printer);
  }

  @Override
  public void abort() {}

  @Override
  public TaskReport commit() {
    return Exec.newTaskReport().set("path", file.getPath());
  }

  private void visitColumns() {
    reader.getSchema().visitColumns(visitor);
    println(printer);
  }

  private static File file(int taskIndex) {
    try {
      return File.createTempFile(
          String.format("%s.", KintoneOutputPlugin.class.getName()),
          String.format(".%d", taskIndex));
    } catch (IOException e) {
      throw new ReduceException(e);
    }
  }

  private static CSVPrinter printer(File file) {
    try {
      return new CSVPrinter(
          new OutputStreamWriter(Files.newOutputStream(file.toPath()), StandardCharsets.UTF_8),
          Reducer.FORMAT);
    } catch (IOException e) {
      throw new ReduceException(e);
    }
  }

  private static void println(CSVPrinter printer) {
    try {
      printer.println();
    } catch (IOException e) {
      throw new ReduceException(e);
    }
  }

  private static void close(Closeable closeable) {
    try {
      closeable.close();
    } catch (IOException e) {
      LOGGER.warn("close error", e);
    }
  }
}
