package org.embulk.output.kintone;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.embulk.deps.buffer.PooledBufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public class OutputPageBuilder implements PageOutput {
  private final List<String> names;
  private final PageBuilder builder;
  private Page page;

  public static Page build(Schema schema, Function<OutputPageBuilder, Page> function) {
    Page page;
    try (OutputPageBuilder builder = new OutputPageBuilder(schema)) {
      page = function.apply(builder);
    }
    return page;
  }

  public OutputPageBuilder(Schema schema) {
    names = schema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
    builder = new PageBuilder(PooledBufferAllocator.create(), schema, this);
  }

  public OutputPageBuilder setNull(String name) {
    builder.setNull(names.indexOf(name));
    return this;
  }

  public OutputPageBuilder setBoolean(String name, boolean value) {
    builder.setBoolean(names.indexOf(name), value);
    return this;
  }

  public OutputPageBuilder setLong(String name, long value) {
    builder.setLong(names.indexOf(name), value);
    return this;
  }

  public OutputPageBuilder setDouble(String name, double value) {
    builder.setDouble(names.indexOf(name), value);
    return this;
  }

  public OutputPageBuilder setString(String name, String value) {
    builder.setString(names.indexOf(name), value);
    return this;
  }

  public OutputPageBuilder setJson(String name, Value value) {
    builder.setJson(names.indexOf(name), value);
    return this;
  }

  public OutputPageBuilder setTimestamp(String name, Timestamp value) {
    builder.setTimestamp(names.indexOf(name), value);
    return this;
  }

  public OutputPageBuilder addRecord() {
    builder.addRecord();
    return this;
  }

  public Page build() {
    builder.flush();
    builder.close();
    return page;
  }

  @Override
  public void add(Page page) {
    if (this.page != null) {
      throw new IllegalStateException();
    }
    this.page = page;
  }

  @Override
  public void finish() {}

  @Override
  public void close() {}
}
