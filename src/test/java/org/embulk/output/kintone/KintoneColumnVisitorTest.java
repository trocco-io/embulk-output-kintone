package org.embulk.output.kintone;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.junit.Test;
import org.msgpack.value.ValueFactory;

public class KintoneColumnVisitorTest {
  @Test
  public void test() {
    Schema schema = build(Schema.builder());
    KintoneColumnVisitorVerifier verifier =
        new KintoneColumnVisitorVerifier(
            schema,
            build(ImmutableMap.builder()),
            "STRING|SINGLE_LINE_TEXT",
            OutputPageBuilder.build(schema, KintoneColumnVisitorTest::build));
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), is("false"));
          assertThat(record.getNumberFieldValue("BOOLEAN"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), is(""));
          assertThat(record.getNumberFieldValue("LONG"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), is("0.0"));
          assertThat(record.getNumberFieldValue("DOUBLE"), is(number("0.0")));
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), is(""));
          assertThat(record.getMultiLineTextFieldValue("STRING"), is(""));
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), nullValue());
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list("null")));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), is(""));
          assertThat(record.getLinkFieldValue("STRING|LINK"), is(""));
          assertThat(record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), nullValue());
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), nullValue());
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), nullValue());
          assertThat(record.getDateTimeFieldValue("TIMESTAMP"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), is(""));
          assertThat(record.getMultiLineTextFieldValue("JSON"), is(""));
          assertThat(updateKey.getField(), is("STRING|SINGLE_LINE_TEXT"));
          assertThat(updateKey.getValue(), is(""));
        });
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), is("false"));
          assertThat(record.getNumberFieldValue("BOOLEAN"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), is("0"));
          assertThat(record.getNumberFieldValue("LONG"), is(number("0")));
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), is("0.0"));
          assertThat(record.getNumberFieldValue("DOUBLE"), is(number("0.0")));
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), is(""));
          assertThat(record.getMultiLineTextFieldValue("STRING"), is(""));
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), nullValue());
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list()));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), is(""));
          assertThat(record.getLinkFieldValue("STRING|LINK"), is(""));
          assertThat(record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), is(date("1969-12-31")));
          assertThat(
              record.getDateTimeFieldValue("TIMESTAMP"), is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), is(""));
          assertThat(record.getMultiLineTextFieldValue("JSON"), is(""));
          assertThat(updateKey.getField(), is("STRING|SINGLE_LINE_TEXT"));
          assertThat(updateKey.getValue(), is(""));
        });
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), is("true"));
          assertThat(record.getNumberFieldValue("BOOLEAN"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), is("123"));
          assertThat(record.getNumberFieldValue("LONG"), is(number("123")));
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), is("123.0"));
          assertThat(record.getNumberFieldValue("DOUBLE"), is(number("123.0")));
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), is("abc"));
          assertThat(record.getMultiLineTextFieldValue("STRING"), is("abc"));
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), is(number("123")));
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list("123", "abc")));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), is("abc"));
          assertThat(record.getLinkFieldValue("STRING|LINK"), is("abc"));
          assertThat(record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), is(date("1999-12-31")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), is(date("1999-12-31")));
          assertThat(
              record.getDateTimeFieldValue("TIMESTAMP"), is(dateTime("1999-12-31T23:59:59Z")));
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), is("abc"));
          assertThat(record.getMultiLineTextFieldValue("JSON"), is("abc"));
          assertThat(updateKey.getField(), is("STRING|SINGLE_LINE_TEXT"));
          assertThat(updateKey.getValue(), is("abc"));
        });
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), is("false"));
          assertThat(record.getNumberFieldValue("BOOLEAN"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), is("456"));
          assertThat(record.getNumberFieldValue("LONG"), is(number("456")));
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), is("456.0"));
          assertThat(record.getNumberFieldValue("DOUBLE"), is(number("456.0")));
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), is("def"));
          assertThat(record.getMultiLineTextFieldValue("STRING"), is("def"));
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), is(number("456")));
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list("456", "def")));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), is("def"));
          assertThat(record.getLinkFieldValue("STRING|LINK"), is("def"));
          assertThat(record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), is(date("1999-12-31")));
          assertThat(
              record.getDateTimeFieldValue("TIMESTAMP"), is(dateTime("2000-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), is("def"));
          assertThat(record.getMultiLineTextFieldValue("JSON"), is("def"));
          assertThat(updateKey.getField(), is("STRING|SINGLE_LINE_TEXT"));
          assertThat(updateKey.getValue(), is("def"));
        });
  }

  private static Schema build(Schema.Builder builder) {
    return builder
        .add("BOOLEAN|SINGLE_LINE_TEXT", Types.BOOLEAN)
        // java.lang.NumberFormatException at java.math.BigDecimal.<init>
        // .add("BOOLEAN", Types.BOOLEAN)
        .add("LONG|SINGLE_LINE_TEXT", Types.LONG)
        .add("LONG", Types.LONG)
        .add("DOUBLE|SINGLE_LINE_TEXT", Types.DOUBLE)
        .add("DOUBLE", Types.DOUBLE)
        .add("STRING|SINGLE_LINE_TEXT", Types.STRING)
        .add("STRING", Types.STRING)
        .add("STRING|NUMBER", Types.STRING)
        .add("STRING|CHECK_BOX", Types.STRING)
        .add("STRING|DROP_DOWN", Types.STRING)
        .add("STRING|LINK", Types.STRING)
        .add("TIMESTAMP|SINGLE_LINE_TEXT", Types.TIMESTAMP)
        .add("TIMESTAMP|DATE", Types.TIMESTAMP)
        .add("TIMESTAMP|DATE|JST", Types.TIMESTAMP)
        .add("TIMESTAMP|DATE|PST", Types.TIMESTAMP)
        .add("TIMESTAMP", Types.TIMESTAMP)
        .add("JSON|SINGLE_LINE_TEXT", Types.JSON)
        .add("JSON", Types.JSON)
        .build();
  }

  private static Map<String, KintoneColumnOption> build(
      ImmutableMap.Builder<String, KintoneColumnOption> builder) {
    return builder
        .put(build("BOOLEAN|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("BOOLEAN", it -> it.setType("NUMBER")))
        .put(build("LONG|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("LONG", it -> it.setType("NUMBER")))
        .put(build("DOUBLE|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("DOUBLE", it -> it.setType("NUMBER")))
        .put(build("STRING|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("STRING", it -> it.setType("MULTI_LINE_TEXT")))
        .put(build("STRING|NUMBER", it -> it.setType("NUMBER")))
        .put(build("STRING|CHECK_BOX", it -> it.setType("CHECK_BOX").setValueSeparator(",")))
        .put(build("STRING|DROP_DOWN", it -> it.setType("DROP_DOWN")))
        .put(build("STRING|LINK", it -> it.setType("LINK")))
        .put(build("TIMESTAMP|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("TIMESTAMP|DATE", it -> it.setType("DATE").setTimezone("UTC")))
        .put(build("TIMESTAMP|DATE|JST", it -> it.setType("DATE").setTimezone("Asia/Tokyo")))
        .put(build("TIMESTAMP|DATE|PST", it -> it.setType("DATE").setTimezone("US/Pacific")))
        .put(build("TIMESTAMP", it -> it.setType("DATETIME")))
        .put(build("JSON|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("JSON", it -> it.setType("MULTI_LINE_TEXT")))
        .build();
  }

  private static Map.Entry<String, KintoneColumnOption> build(
      String name, Function<KintoneColumnOptionBuilder, KintoneColumnOptionBuilder> function) {
    return new AbstractMap.SimpleImmutableEntry<>(
        name, function.apply(new KintoneColumnOptionBuilder().setFieldCode(name)).build());
  }

  private static Page build(OutputPageBuilder builder) {
    return builder
        .setNull("BOOLEAN|SINGLE_LINE_TEXT")
        // .setNull("BOOLEAN")
        .setNull("LONG|SINGLE_LINE_TEXT")
        .setNull("LONG")
        .setNull("DOUBLE|SINGLE_LINE_TEXT")
        .setNull("DOUBLE")
        .setNull("STRING|SINGLE_LINE_TEXT")
        .setNull("STRING")
        .setNull("STRING|NUMBER")
        .setNull("STRING|CHECK_BOX")
        .setNull("STRING|DROP_DOWN")
        .setNull("STRING|LINK")
        .setNull("TIMESTAMP|SINGLE_LINE_TEXT")
        .setNull("TIMESTAMP|DATE")
        .setNull("TIMESTAMP|DATE|JST")
        .setNull("TIMESTAMP|DATE|PST")
        .setNull("TIMESTAMP")
        .setNull("JSON|SINGLE_LINE_TEXT")
        .setNull("JSON")
        .addRecord()
        .setBoolean("BOOLEAN|SINGLE_LINE_TEXT", false)
        // .setBoolean("BOOLEAN", false)
        .setLong("LONG|SINGLE_LINE_TEXT", 0)
        .setLong("LONG", 0)
        .setDouble("DOUBLE|SINGLE_LINE_TEXT", 0)
        .setDouble("DOUBLE", 0)
        .setString("STRING|SINGLE_LINE_TEXT", "")
        .setString("STRING", "")
        .setString("STRING|NUMBER", "")
        .setString("STRING|CHECK_BOX", "")
        .setString("STRING|DROP_DOWN", "")
        .setString("STRING|LINK", "")
        .setTimestamp("TIMESTAMP|SINGLE_LINE_TEXT", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|DATE", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|DATE|JST", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|DATE|PST", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP", Timestamp.ofInstant(Instant.EPOCH))
        .setJson("JSON|SINGLE_LINE_TEXT", ValueFactory.newString(""))
        .setJson("JSON", ValueFactory.newString(""))
        .addRecord()
        .setBoolean("BOOLEAN|SINGLE_LINE_TEXT", true)
        // .setBoolean("BOOLEAN", true)
        .setLong("LONG|SINGLE_LINE_TEXT", 123)
        .setLong("LONG", 123)
        .setDouble("DOUBLE|SINGLE_LINE_TEXT", 123)
        .setDouble("DOUBLE", 123)
        .setString("STRING|SINGLE_LINE_TEXT", "abc")
        .setString("STRING", "abc")
        .setString("STRING|NUMBER", "123")
        .setString("STRING|CHECK_BOX", "123,abc")
        .setString("STRING|DROP_DOWN", "abc")
        .setString("STRING|LINK", "abc")
        .setTimestamp("TIMESTAMP|SINGLE_LINE_TEXT", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|DATE", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|DATE|JST", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|DATE|PST", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP", timestamp("1999-12-31T23:59:59Z"))
        .setJson("JSON|SINGLE_LINE_TEXT", ValueFactory.newString("abc"))
        .setJson("JSON", ValueFactory.newString("abc"))
        .addRecord()
        .setBoolean("BOOLEAN|SINGLE_LINE_TEXT", false)
        // .setBoolean("BOOLEAN", false)
        .setLong("LONG|SINGLE_LINE_TEXT", 456)
        .setLong("LONG", 456)
        .setDouble("DOUBLE|SINGLE_LINE_TEXT", 456)
        .setDouble("DOUBLE", 456)
        .setString("STRING|SINGLE_LINE_TEXT", "def")
        .setString("STRING", "def")
        .setString("STRING|NUMBER", "456")
        .setString("STRING|CHECK_BOX", "456,def")
        .setString("STRING|DROP_DOWN", "def")
        .setString("STRING|LINK", "def")
        .setTimestamp("TIMESTAMP|SINGLE_LINE_TEXT", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|DATE", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|DATE|JST", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|DATE|PST", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP", timestamp("2000-01-01T00:00:00Z"))
        .setJson("JSON|SINGLE_LINE_TEXT", ValueFactory.newString("def"))
        .setJson("JSON", ValueFactory.newString("def"))
        .addRecord()
        .build();
  }

  private static BigDecimal number(String value) {
    return new BigDecimal(value);
  }

  @SafeVarargs
  private static <T> List<T> list(T... a) {
    return Arrays.asList(a);
  }

  private static LocalDate date(CharSequence text) {
    return LocalDate.parse(text);
  }

  private static ZonedDateTime dateTime(CharSequence text) {
    return ZonedDateTime.parse(text).withZoneSameInstant(ZoneId.of("UTC"));
  }

  private static Timestamp timestamp(CharSequence text) {
    return Timestamp.ofInstant(Instant.parse(text));
  }
}
