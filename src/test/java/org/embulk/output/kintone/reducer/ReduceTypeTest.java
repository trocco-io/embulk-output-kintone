package org.embulk.output.kintone.reducer;

import static org.embulk.output.kintone.KintoneColumnType.CHECK_BOX;
import static org.embulk.output.kintone.KintoneColumnType.DATE;
import static org.embulk.output.kintone.KintoneColumnType.DATETIME;
import static org.embulk.output.kintone.KintoneColumnType.DROP_DOWN;
import static org.embulk.output.kintone.KintoneColumnType.LINK;
import static org.embulk.output.kintone.KintoneColumnType.MULTI_LINE_TEXT;
import static org.embulk.output.kintone.KintoneColumnType.MULTI_SELECT;
import static org.embulk.output.kintone.KintoneColumnType.NUMBER;
import static org.embulk.output.kintone.KintoneColumnType.RADIO_BUTTON;
import static org.embulk.output.kintone.KintoneColumnType.RICH_TEXT;
import static org.embulk.output.kintone.KintoneColumnType.SINGLE_LINE_TEXT;
import static org.embulk.output.kintone.KintoneColumnType.TIME;
import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import org.embulk.output.kintone.KintoneColumnOptionBuilder;
import org.embulk.output.kintone.KintoneColumnType;
import org.embulk.spi.Column;
import org.embulk.spi.type.Type;
import org.junit.Test;

public class ReduceTypeTest {
  @Test
  public void value() {
    // spotless:off
    assertJson(BOOLEAN, SINGLE_LINE_TEXT, "true", "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":\"true\"}");
    assertJson(BOOLEAN, NUMBER, "true", "{\"type\":\"NUMBER\",\"value\":\"1\"}");
    assertJson(LONG, SINGLE_LINE_TEXT, "123", "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":\"123\"}");
    assertJson(LONG, NUMBER, "123", "{\"type\":\"NUMBER\",\"value\":\"123\"}");
    assertJson(LONG, DATE, "946684799", "{\"type\":\"DATE\",\"value\":\"1999-12-31\"}");
    assertJson(LONG, DATE, "Asia/Tokyo", "946684799", "{\"type\":\"DATE\",\"value\":\"2000-01-01\"}");
    assertJson(LONG, DATE, "US/Pacific", "946684799", "{\"type\":\"DATE\",\"value\":\"1999-12-31\"}");
    assertJson(LONG, TIME, "946684799", "{\"type\":\"TIME\",\"value\":\"23:59:59\"}");
    assertJson(LONG, TIME, "Asia/Tokyo", "946684799", "{\"type\":\"TIME\",\"value\":\"08:59:59\"}");
    assertJson(LONG, TIME, "US/Pacific", "946684799", "{\"type\":\"TIME\",\"value\":\"15:59:59\"}");
    assertJson(LONG, DATETIME, "946684799", "{\"type\":\"DATETIME\",\"value\":\"1999-12-31T23:59:59Z\"}");
    assertJson(DOUBLE, SINGLE_LINE_TEXT, "123", "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":\"123.0\"}");
    assertJson(DOUBLE, NUMBER, "123", "{\"type\":\"NUMBER\",\"value\":\"123.0\"}");
    assertJson(DOUBLE, DATE, "946684799", "{\"type\":\"DATE\",\"value\":\"1999-12-31\"}");
    assertJson(DOUBLE, DATE, "Asia/Tokyo", "946684799", "{\"type\":\"DATE\",\"value\":\"2000-01-01\"}");
    assertJson(DOUBLE, DATE, "US/Pacific", "946684799", "{\"type\":\"DATE\",\"value\":\"1999-12-31\"}");
    assertJson(DOUBLE, TIME, "946684799", "{\"type\":\"TIME\",\"value\":\"23:59:59\"}");
    assertJson(DOUBLE, TIME, "Asia/Tokyo", "946684799", "{\"type\":\"TIME\",\"value\":\"08:59:59\"}");
    assertJson(DOUBLE, TIME, "US/Pacific", "946684799", "{\"type\":\"TIME\",\"value\":\"15:59:59\"}");
    assertJson(DOUBLE, DATETIME, "946684799", "{\"type\":\"DATETIME\",\"value\":\"1999-12-31T23:59:59Z\"}");
    assertJson(STRING, SINGLE_LINE_TEXT, "abc", "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":\"abc\"}");
    assertJson(STRING, MULTI_LINE_TEXT, "abc", "{\"type\":\"MULTI_LINE_TEXT\",\"value\":\"abc\"}");
    assertJson(STRING, RICH_TEXT, "abc", "{\"type\":\"RICH_TEXT\",\"value\":\"abc\"}");
    assertJson(STRING, NUMBER, "123", "{\"type\":\"NUMBER\",\"value\":\"123\"}");
    assertJson(STRING, CHECK_BOX, "123,abc", "{\"type\":\"CHECK_BOX\",\"value\":[\"123\",\"abc\"]}");
    assertJson(STRING, RADIO_BUTTON, "abc", "{\"type\":\"RADIO_BUTTON\",\"value\":\"abc\"}");
    assertJson(STRING, MULTI_SELECT, "123,abc", "{\"type\":\"MULTI_SELECT\",\"value\":[\"123\",\"abc\"]}");
    assertJson(STRING, DROP_DOWN, "abc", "{\"type\":\"DROP_DOWN\",\"value\":\"abc\"}");
    assertJson(STRING, DATE, "1999-12-31", "{\"type\":\"DATE\",\"value\":\"1999-12-31\"}");
    assertJson(STRING, DATE, "Asia/Tokyo", "1999-12-31", "{\"type\":\"DATE\",\"value\":\"1999-12-31\"}");
    assertJson(STRING, DATE, "US/Pacific", "1999-12-31", "{\"type\":\"DATE\",\"value\":\"1999-12-30\"}");
    assertJson(STRING, TIME, "23:59:59", "{\"type\":\"TIME\",\"value\":\"23:59:59\"}");
    assertJson(STRING, TIME, "Asia/Tokyo", "23:59:59", "{\"type\":\"TIME\",\"value\":\"08:59:59\"}");
    assertJson(STRING, TIME, "US/Pacific", "23:59:59", "{\"type\":\"TIME\",\"value\":\"15:59:59\"}");
    assertJson(STRING, DATETIME, "1999-12-31T23:59:59Z", "{\"type\":\"DATETIME\",\"value\":\"1999-12-31T23:59:59Z\"}");
    assertJson(STRING, LINK, "abc", "{\"type\":\"LINK\",\"value\":\"abc\"}");
    assertJson(TIMESTAMP, SINGLE_LINE_TEXT, "1999-12-31T23:59:59Z", "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":\"1999-12-31T23:59:59Z\"}");
    assertJson(TIMESTAMP, NUMBER, "1999-12-31T23:59:59Z", "{\"type\":\"NUMBER\",\"value\":\"946684799\"}");
    assertJson(TIMESTAMP, DATE, "1999-12-31T23:59:59Z", "{\"type\":\"DATE\",\"value\":\"1999-12-31\"}");
    assertJson(TIMESTAMP, DATE, "Asia/Tokyo", "1999-12-31T23:59:59Z", "{\"type\":\"DATE\",\"value\":\"2000-01-01\"}");
    assertJson(TIMESTAMP, DATE, "US/Pacific", "1999-12-31T23:59:59Z", "{\"type\":\"DATE\",\"value\":\"1999-12-31\"}");
    assertJson(TIMESTAMP, TIME, "1999-12-31T23:59:59Z", "{\"type\":\"TIME\",\"value\":\"23:59:59\"}");
    assertJson(TIMESTAMP, TIME, "Asia/Tokyo", "1999-12-31T23:59:59Z", "{\"type\":\"TIME\",\"value\":\"08:59:59\"}");
    assertJson(TIMESTAMP, TIME, "US/Pacific", "1999-12-31T23:59:59Z", "{\"type\":\"TIME\",\"value\":\"15:59:59\"}");
    assertJson(TIMESTAMP, DATETIME, "1999-12-31T23:59:59Z", "{\"type\":\"DATETIME\",\"value\":\"1999-12-31T23:59:59Z\"}");
    assertJson(JSON, SINGLE_LINE_TEXT, "\"abc\"", "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":\"\\\"abc\\\"\"}");
    assertJson(JSON, MULTI_LINE_TEXT, "\"abc\"", "{\"type\":\"MULTI_LINE_TEXT\",\"value\":\"\\\"abc\\\"\"}");
    // spotless:on
  }

  @Test
  public void valueNull() {
    assertJson(BOOLEAN, SINGLE_LINE_TEXT, null, "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":null}");
    assertJson(BOOLEAN, NUMBER, null, "{\"type\":\"NUMBER\",\"value\":null}");
    assertJson(LONG, SINGLE_LINE_TEXT, null, "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":null}");
    assertJson(LONG, NUMBER, null, "{\"type\":\"NUMBER\",\"value\":null}");
    assertJson(LONG, DATE, null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(LONG, DATE, "Asia/Tokyo", null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(LONG, DATE, "US/Pacific", null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(LONG, TIME, null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(LONG, TIME, "Asia/Tokyo", null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(LONG, TIME, "US/Pacific", null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(LONG, DATETIME, null, "{\"type\":\"DATETIME\",\"value\":null}");
    assertJson(DOUBLE, SINGLE_LINE_TEXT, null, "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":null}");
    assertJson(DOUBLE, NUMBER, null, "{\"type\":\"NUMBER\",\"value\":null}");
    assertJson(DOUBLE, DATE, null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(DOUBLE, DATE, "Asia/Tokyo", null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(DOUBLE, DATE, "US/Pacific", null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(DOUBLE, TIME, null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(DOUBLE, TIME, "Asia/Tokyo", null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(DOUBLE, TIME, "US/Pacific", null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(DOUBLE, DATETIME, null, "{\"type\":\"DATETIME\",\"value\":null}");
    assertJson(STRING, SINGLE_LINE_TEXT, null, "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":null}");
    assertJson(STRING, MULTI_LINE_TEXT, null, "{\"type\":\"MULTI_LINE_TEXT\",\"value\":null}");
    assertJson(STRING, RICH_TEXT, null, "{\"type\":\"RICH_TEXT\",\"value\":null}");
    assertJson(STRING, NUMBER, null, "{\"type\":\"NUMBER\",\"value\":null}");
    assertJson(STRING, CHECK_BOX, null, "{\"type\":\"CHECK_BOX\",\"value\":null}");
    assertJson(STRING, RADIO_BUTTON, null, "{\"type\":\"RADIO_BUTTON\",\"value\":null}");
    assertJson(STRING, MULTI_SELECT, null, "{\"type\":\"MULTI_SELECT\",\"value\":null}");
    assertJson(STRING, DROP_DOWN, null, "{\"type\":\"DROP_DOWN\",\"value\":null}");
    assertJson(STRING, DATE, null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(STRING, DATE, "Asia/Tokyo", null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(STRING, DATE, "US/Pacific", null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(STRING, TIME, null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(STRING, TIME, "Asia/Tokyo", null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(STRING, TIME, "US/Pacific", null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(STRING, DATETIME, null, "{\"type\":\"DATETIME\",\"value\":null}");
    assertJson(STRING, LINK, null, "{\"type\":\"LINK\",\"value\":null}");
    assertJson(TIMESTAMP, SINGLE_LINE_TEXT, null, "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":null}");
    assertJson(TIMESTAMP, NUMBER, null, "{\"type\":\"NUMBER\",\"value\":null}");
    assertJson(TIMESTAMP, DATE, null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(TIMESTAMP, DATE, "Asia/Tokyo", null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(TIMESTAMP, DATE, "US/Pacific", null, "{\"type\":\"DATE\",\"value\":null}");
    assertJson(TIMESTAMP, TIME, null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(TIMESTAMP, TIME, "Asia/Tokyo", null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(TIMESTAMP, TIME, "US/Pacific", null, "{\"type\":\"TIME\",\"value\":null}");
    assertJson(TIMESTAMP, DATETIME, null, "{\"type\":\"DATETIME\",\"value\":null}");
    assertJson(JSON, SINGLE_LINE_TEXT, null, "{\"type\":\"SINGLE_LINE_TEXT\",\"value\":null}");
    assertJson(JSON, MULTI_LINE_TEXT, null, "{\"type\":\"MULTI_LINE_TEXT\",\"value\":null}");
  }

  private static void assertJson(Type from, KintoneColumnType to, String value, String expected) {
    assertJson(from, to, "UTC", value, expected);
  }

  private static void assertJson(
      Type from, KintoneColumnType to, String timezone, String value, String expected) {
    assertThat(
        ReduceType.value(
                new Column(0, "", from),
                Collections.singletonList(value),
                new KintoneColumnOptionBuilder()
                    .setType(to.name())
                    .setTimezone(timezone)
                    .setValueSeparator(",")
                    .build(),
                null)
            .toJson(),
        is(expected));
  }
}
