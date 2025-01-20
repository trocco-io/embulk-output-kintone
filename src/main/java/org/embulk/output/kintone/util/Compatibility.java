package org.embulk.output.kintone.util;

import java.time.Instant;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;

@SuppressWarnings("deprecation") // TODO: For compatibility with Embulk v0.9
public class Compatibility {
  public static final org.embulk.util.json.JsonParser PARSER =
      new org.embulk.util.json.JsonParser(); // TODO: Use JsonValueParser

  public static Instant getTimestamp(PageReader reader, Column column) {
    org.embulk.spi.time.Timestamp value =
        reader.getTimestamp(column); // TODO: Use PageReader#getTimestampInstant
    return value != null ? value.getInstant() : null;
  }

  public static Value getJson(PageReader reader, Column column) {
    return reader.getJson(column); // TODO: Use PageReader#getJsonValue
  }

  public static void setTimestamp(PageBuilder builder, Column column, Instant value) {
    builder.setTimestamp( // TODO: Use Instant for PageBuilder#setTimestamp
        column, value != null ? org.embulk.spi.time.Timestamp.ofInstant(value) : null);
  }

  public static void setJson(PageBuilder builder, Column column, Value value) {
    builder.setJson(column, value); // TODO: Use JsonValue for PageBuilder#setJson
  }
}
