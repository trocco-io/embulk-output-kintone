package org.embulk.output.kintone;

import static org.embulk.output.kintone.deserializer.DeserializerTest.assertTableRows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.TableRow;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.embulk.output.kintone.deserializer.DeserializerTest;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.junit.Test;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class KintoneColumnVisitorTest {
  private static final JsonParser PARSER = new JsonParser();
  private static final String[] ROWS = {
    DeserializerTest.TABLE_ROW.apply(0L),
    DeserializerTest.TABLE_ROW.apply(1L),
    DeserializerTest.TABLE_ROW.apply(2L),
    DeserializerTest.TABLE_ROW.apply(3L),
    DeserializerTest.TABLE_ROW.apply(4L),
    DeserializerTest.TABLE_ROW.apply(5L),
  };

  @Test
  public void test() {
    KintoneColumnVisitorVerifier verifier = verifier(null, "STRING|SINGLE_LINE_TEXT");
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), is("false"));
          assertThat(record.getNumberFieldValue("BOOLEAN"), is(number("0")));
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), is("0"));
          assertThat(record.getNumberFieldValue("LONG"), is(number("0")));
          assertThat(record.getDateFieldValue("LONG|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("LONG|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("LONG|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("LONG|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("LONG|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("LONG|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("LONG|DATETIME"), is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), is("0.0"));
          assertThat(record.getNumberFieldValue("DOUBLE"), is(number("0.0")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("DOUBLE|DATETIME"),
              is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), is(""));
          assertThat(record.getMultiLineTextFieldValue("STRING"), is(""));
          assertThat(record.getRichTextFieldValue("STRING|RICH_TEXT"), is(""));
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), is(number("0")));
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list()));
          assertThat(record.getRadioButtonFieldValue("STRING|RADIO_BUTTON"), is(""));
          assertThat(record.getMultiSelectFieldValue("STRING|MULTI_SELECT"), is(list()));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), is(""));
          assertThat(record.getDateFieldValue("STRING|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("STRING|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("STRING|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("STRING|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("STRING|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("STRING|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("STRING|DATETIME"),
              is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getLinkFieldValue("STRING|LINK"), is(""));
          assertThat(record.getSubtableFieldValue("STRING|SUBTABLE"), is(list()));
          assertThat(
              record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"),
              is("1970-01-01T00:00:00Z"));
          assertThat(record.getNumberFieldValue("TIMESTAMP|NUMBER"), is(number("0")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("TIMESTAMP"), is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), is("\"\""));
          assertThat(record.getMultiLineTextFieldValue("JSON"), is("\"\""));
          assertThat(record.getSubtableFieldValue("JSON|SUBTABLE"), is(list()));
          assertThat(
              record.getSingleLineTextFieldValue("JSON|SUBTABLE.SINGLE_LINE_TEXT"), is("\"\""));
          assertThat(updateKey.getField(), is("STRING|SINGLE_LINE_TEXT"));
          assertThat(updateKey.getValue(), is(""));
        });
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), is("false"));
          assertThat(record.getNumberFieldValue("BOOLEAN"), is(number("0")));
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), is("0"));
          assertThat(record.getNumberFieldValue("LONG"), is(number("0")));
          assertThat(record.getDateFieldValue("LONG|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("LONG|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("LONG|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("LONG|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("LONG|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("LONG|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("LONG|DATETIME"), is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), is("0.0"));
          assertThat(record.getNumberFieldValue("DOUBLE"), is(number("0.0")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("DOUBLE|DATETIME"),
              is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), is(""));
          assertThat(record.getMultiLineTextFieldValue("STRING"), is(""));
          assertThat(record.getRichTextFieldValue("STRING|RICH_TEXT"), is(""));
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), is(number("0")));
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list()));
          assertThat(record.getRadioButtonFieldValue("STRING|RADIO_BUTTON"), is(""));
          assertThat(record.getMultiSelectFieldValue("STRING|MULTI_SELECT"), is(list()));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), is(""));
          assertThat(record.getDateFieldValue("STRING|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("STRING|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("STRING|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("STRING|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("STRING|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("STRING|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("STRING|DATETIME"),
              is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getLinkFieldValue("STRING|LINK"), is(""));
          assertThat(record.getSubtableFieldValue("STRING|SUBTABLE"), is(list()));
          assertThat(
              record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"),
              is("1970-01-01T00:00:00Z"));
          assertThat(record.getNumberFieldValue("TIMESTAMP|NUMBER"), is(number("0")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("TIMESTAMP"), is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), is("\"\""));
          assertThat(record.getMultiLineTextFieldValue("JSON"), is("\"\""));
          assertThat(record.getSubtableFieldValue("JSON|SUBTABLE"), is(list()));
          assertThat(
              record.getSingleLineTextFieldValue("JSON|SUBTABLE.SINGLE_LINE_TEXT"), is("\"\""));
          assertThat(updateKey.getField(), is("STRING|SINGLE_LINE_TEXT"));
          assertThat(updateKey.getValue(), is(""));
        });
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), is("true"));
          assertThat(record.getNumberFieldValue("BOOLEAN"), is(number("1")));
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), is("123"));
          assertThat(record.getNumberFieldValue("LONG"), is(number("123")));
          assertThat(record.getDateFieldValue("LONG|DATE"), is(date("1999-12-31")));
          assertThat(record.getDateFieldValue("LONG|DATE|JST"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("LONG|DATE|PST"), is(date("1999-12-31")));
          assertThat(record.getTimeFieldValue("LONG|TIME"), is(time("23:59:59")));
          assertThat(record.getTimeFieldValue("LONG|TIME|JST"), is(time("08:59:59")));
          assertThat(record.getTimeFieldValue("LONG|TIME|PST"), is(time("15:59:59")));
          assertThat(
              record.getDateTimeFieldValue("LONG|DATETIME"), is(dateTime("1999-12-31T23:59:59Z")));
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), is("123.0"));
          assertThat(record.getNumberFieldValue("DOUBLE"), is(number("123.0")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE"), is(date("1999-12-31")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|JST"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|PST"), is(date("1999-12-31")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME"), is(time("23:59:59")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|JST"), is(time("08:59:59")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|PST"), is(time("15:59:59")));
          assertThat(
              record.getDateTimeFieldValue("DOUBLE|DATETIME"),
              is(dateTime("1999-12-31T23:59:59Z")));
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), is("abc"));
          assertThat(record.getMultiLineTextFieldValue("STRING"), is("abc"));
          assertThat(record.getRichTextFieldValue("STRING|RICH_TEXT"), is("abc"));
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), is(number("123")));
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list("123", "abc")));
          assertThat(record.getRadioButtonFieldValue("STRING|RADIO_BUTTON"), is("abc"));
          assertThat(
              record.getMultiSelectFieldValue("STRING|MULTI_SELECT"), is(list("123", "abc")));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), is("abc"));
          assertThat(record.getDateFieldValue("STRING|DATE"), is(date("1999-12-31")));
          assertThat(record.getDateFieldValue("STRING|DATE|JST"), is(date("1999-12-31")));
          assertThat(record.getDateFieldValue("STRING|DATE|PST"), is(date("1999-12-30")));
          assertThat(record.getTimeFieldValue("STRING|TIME"), is(time("23:59:59")));
          assertThat(record.getTimeFieldValue("STRING|TIME|JST"), is(time("08:59:59")));
          assertThat(record.getTimeFieldValue("STRING|TIME|PST"), is(time("15:59:59")));
          assertThat(
              record.getDateTimeFieldValue("STRING|DATETIME"),
              is(dateTime("1999-12-31T23:59:59Z")));
          assertThat(record.getLinkFieldValue("STRING|LINK"), is("abc"));
          assertTableRows(record.getSubtableFieldValue("STRING|SUBTABLE"), rows(0L, 1L, 2L));
          assertThat(
              record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"),
              is("1999-12-31T23:59:59Z"));
          assertThat(record.getNumberFieldValue("TIMESTAMP|NUMBER"), is(number("946684799")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), is(date("1999-12-31")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), is(date("1999-12-31")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME"), is(time("23:59:59")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|JST"), is(time("08:59:59")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|PST"), is(time("15:59:59")));
          assertThat(
              record.getDateTimeFieldValue("TIMESTAMP"), is(dateTime("1999-12-31T23:59:59Z")));
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), is("\"abc\""));
          assertThat(record.getMultiLineTextFieldValue("JSON"), is("\"abc\""));
          assertTableRows(record.getSubtableFieldValue("JSON|SUBTABLE"), rows(0L, 1L, 2L));
          assertThat(
              record.getSingleLineTextFieldValue("JSON|SUBTABLE.SINGLE_LINE_TEXT"), is("\"abc\""));
          assertThat(updateKey.getField(), is("STRING|SINGLE_LINE_TEXT"));
          assertThat(updateKey.getValue(), is("abc"));
        });
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), is("false"));
          assertThat(record.getNumberFieldValue("BOOLEAN"), is(number("0")));
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), is("456"));
          assertThat(record.getNumberFieldValue("LONG"), is(number("456")));
          assertThat(record.getDateFieldValue("LONG|DATE"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("LONG|DATE|JST"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("LONG|DATE|PST"), is(date("1999-12-31")));
          assertThat(record.getTimeFieldValue("LONG|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("LONG|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("LONG|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("LONG|DATETIME"), is(dateTime("2000-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), is("456.0"));
          assertThat(record.getNumberFieldValue("DOUBLE"), is(number("456.0")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|JST"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|PST"), is(date("1999-12-31")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("DOUBLE|DATETIME"),
              is(dateTime("2000-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), is("def"));
          assertThat(record.getMultiLineTextFieldValue("STRING"), is("def"));
          assertThat(record.getRichTextFieldValue("STRING|RICH_TEXT"), is("def"));
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), is(number("456")));
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list("456", "def")));
          assertThat(record.getRadioButtonFieldValue("STRING|RADIO_BUTTON"), is("def"));
          assertThat(
              record.getMultiSelectFieldValue("STRING|MULTI_SELECT"), is(list("456", "def")));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), is("def"));
          assertThat(record.getDateFieldValue("STRING|DATE"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("STRING|DATE|JST"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("STRING|DATE|PST"), is(date("1999-12-31")));
          assertThat(record.getTimeFieldValue("STRING|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("STRING|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("STRING|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("STRING|DATETIME"),
              is(dateTime("2000-01-01T00:00:00Z")));
          assertThat(record.getLinkFieldValue("STRING|LINK"), is("def"));
          assertTableRows(record.getSubtableFieldValue("STRING|SUBTABLE"), rows(3L, 4L, 5L));
          assertThat(
              record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"),
              is("2000-01-01T00:00:00Z"));
          assertThat(record.getNumberFieldValue("TIMESTAMP|NUMBER"), is(number("946684800")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), is(date("2000-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), is(date("1999-12-31")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("TIMESTAMP"), is(dateTime("2000-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), is("\"def\""));
          assertThat(record.getMultiLineTextFieldValue("JSON"), is("\"def\""));
          assertTableRows(record.getSubtableFieldValue("JSON|SUBTABLE"), rows(3L, 4L, 5L));
          assertThat(
              record.getSingleLineTextFieldValue("JSON|SUBTABLE.SINGLE_LINE_TEXT"), is("\"def\""));
          assertThat(updateKey.getField(), is("STRING|SINGLE_LINE_TEXT"));
          assertThat(updateKey.getValue(), is("def"));
        });
  }

  @Test
  public void testPreferNulls() {
    KintoneColumnVisitorVerifier verifier = verifier(null, "LONG", true, false);
    verifier.verify(
        (record, updateKey) -> {
          assertThat(
              record.getFieldType("BOOLEAN|SINGLE_LINE_TEXT"), is(FieldType.SINGLE_LINE_TEXT));
          assertThat(record.getFieldType("BOOLEAN"), is(FieldType.NUMBER));
          assertThat(record.getFieldType("LONG|SINGLE_LINE_TEXT"), is(FieldType.SINGLE_LINE_TEXT));
          assertThat(record.getFieldType("LONG"), is(FieldType.NUMBER));
          assertThat(record.getFieldType("LONG|DATE"), is(FieldType.DATE));
          assertThat(record.getFieldType("LONG|DATE|JST"), is(FieldType.DATE));
          assertThat(record.getFieldType("LONG|DATE|PST"), is(FieldType.DATE));
          assertThat(record.getFieldType("LONG|TIME"), is(FieldType.TIME));
          assertThat(record.getFieldType("LONG|TIME|JST"), is(FieldType.TIME));
          assertThat(record.getFieldType("LONG|TIME|PST"), is(FieldType.TIME));
          assertThat(record.getFieldType("LONG|DATETIME"), is(FieldType.DATETIME));
          assertThat(
              record.getFieldType("DOUBLE|SINGLE_LINE_TEXT"), is(FieldType.SINGLE_LINE_TEXT));
          assertThat(record.getFieldType("DOUBLE"), is(FieldType.NUMBER));
          assertThat(record.getFieldType("DOUBLE|DATE"), is(FieldType.DATE));
          assertThat(record.getFieldType("DOUBLE|DATE|JST"), is(FieldType.DATE));
          assertThat(record.getFieldType("DOUBLE|DATE|PST"), is(FieldType.DATE));
          assertThat(record.getFieldType("DOUBLE|TIME"), is(FieldType.TIME));
          assertThat(record.getFieldType("DOUBLE|TIME|JST"), is(FieldType.TIME));
          assertThat(record.getFieldType("DOUBLE|TIME|PST"), is(FieldType.TIME));
          assertThat(record.getFieldType("DOUBLE|DATETIME"), is(FieldType.DATETIME));
          assertThat(
              record.getFieldType("STRING|SINGLE_LINE_TEXT"), is(FieldType.SINGLE_LINE_TEXT));
          assertThat(record.getFieldType("STRING"), is(FieldType.MULTI_LINE_TEXT));
          assertThat(record.getFieldType("STRING|RICH_TEXT"), is(FieldType.RICH_TEXT));
          assertThat(record.getFieldType("STRING|NUMBER"), is(FieldType.NUMBER));
          assertThat(record.getFieldType("STRING|CHECK_BOX"), is(FieldType.CHECK_BOX));
          assertThat(record.getFieldType("STRING|RADIO_BUTTON"), is(FieldType.RADIO_BUTTON));
          assertThat(record.getFieldType("STRING|MULTI_SELECT"), is(FieldType.MULTI_SELECT));
          assertThat(record.getFieldType("STRING|DROP_DOWN"), is(FieldType.DROP_DOWN));
          assertThat(record.getFieldType("STRING|DATE"), is(FieldType.DATE));
          assertThat(record.getFieldType("STRING|DATE|JST"), is(FieldType.DATE));
          assertThat(record.getFieldType("STRING|DATE|PST"), is(FieldType.DATE));
          assertThat(record.getFieldType("STRING|TIME"), is(FieldType.TIME));
          assertThat(record.getFieldType("STRING|TIME|JST"), is(FieldType.TIME));
          assertThat(record.getFieldType("STRING|TIME|PST"), is(FieldType.TIME));
          assertThat(record.getFieldType("STRING|DATETIME"), is(FieldType.DATETIME));
          assertThat(record.getFieldType("STRING|LINK"), is(FieldType.LINK));
          assertThat(record.getFieldType("STRING|SUBTABLE"), is(FieldType.SUBTABLE));
          assertThat(
              record.getFieldType("TIMESTAMP|SINGLE_LINE_TEXT"), is(FieldType.SINGLE_LINE_TEXT));
          assertThat(record.getFieldType("TIMESTAMP|NUMBER"), is(FieldType.NUMBER));
          assertThat(record.getFieldType("TIMESTAMP|DATE"), is(FieldType.DATE));
          assertThat(record.getFieldType("TIMESTAMP|DATE|JST"), is(FieldType.DATE));
          assertThat(record.getFieldType("TIMESTAMP|DATE|PST"), is(FieldType.DATE));
          assertThat(record.getFieldType("TIMESTAMP|TIME"), is(FieldType.TIME));
          assertThat(record.getFieldType("TIMESTAMP|TIME|JST"), is(FieldType.TIME));
          assertThat(record.getFieldType("TIMESTAMP|TIME|PST"), is(FieldType.TIME));
          assertThat(record.getFieldType("TIMESTAMP"), is(FieldType.DATETIME));
          assertThat(record.getFieldType("JSON|SINGLE_LINE_TEXT"), is(FieldType.SINGLE_LINE_TEXT));
          assertThat(record.getFieldType("JSON"), is(FieldType.MULTI_LINE_TEXT));
          assertThat(record.getFieldType("JSON|SUBTABLE"), is(FieldType.SUBTABLE));
          assertThat(
              record.getFieldType("JSON|SUBTABLE.SINGLE_LINE_TEXT"),
              is(FieldType.SINGLE_LINE_TEXT));
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getNumberFieldValue("BOOLEAN"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getNumberFieldValue("LONG"), nullValue());
          assertThat(record.getDateFieldValue("LONG|DATE"), nullValue());
          assertThat(record.getDateFieldValue("LONG|DATE|JST"), nullValue());
          assertThat(record.getDateFieldValue("LONG|DATE|PST"), nullValue());
          assertThat(record.getTimeFieldValue("LONG|TIME"), nullValue());
          assertThat(record.getTimeFieldValue("LONG|TIME|JST"), nullValue());
          assertThat(record.getTimeFieldValue("LONG|TIME|PST"), nullValue());
          assertThat(record.getDateTimeFieldValue("LONG|DATETIME"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getNumberFieldValue("DOUBLE"), nullValue());
          assertThat(record.getDateFieldValue("DOUBLE|DATE"), nullValue());
          assertThat(record.getDateFieldValue("DOUBLE|DATE|JST"), nullValue());
          assertThat(record.getDateFieldValue("DOUBLE|DATE|PST"), nullValue());
          assertThat(record.getTimeFieldValue("DOUBLE|TIME"), nullValue());
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|JST"), nullValue());
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|PST"), nullValue());
          assertThat(record.getDateTimeFieldValue("DOUBLE|DATETIME"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getMultiLineTextFieldValue("STRING"), nullValue());
          assertThat(record.getRichTextFieldValue("STRING|RICH_TEXT"), nullValue());
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), nullValue());
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list()));
          assertThat(record.getRadioButtonFieldValue("STRING|RADIO_BUTTON"), nullValue());
          assertThat(record.getMultiSelectFieldValue("STRING|MULTI_SELECT"), is(list()));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), nullValue());
          assertThat(record.getDateFieldValue("STRING|DATE"), nullValue());
          assertThat(record.getDateFieldValue("STRING|DATE|JST"), nullValue());
          assertThat(record.getDateFieldValue("STRING|DATE|PST"), nullValue());
          assertThat(record.getTimeFieldValue("STRING|TIME"), nullValue());
          assertThat(record.getTimeFieldValue("STRING|TIME|JST"), nullValue());
          assertThat(record.getTimeFieldValue("STRING|TIME|PST"), nullValue());
          assertThat(record.getDateTimeFieldValue("STRING|DATETIME"), nullValue());
          assertThat(record.getLinkFieldValue("STRING|LINK"), nullValue());
          assertThat(record.getSubtableFieldValue("STRING|SUBTABLE"), is(list()));
          assertThat(record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getNumberFieldValue("TIMESTAMP|NUMBER"), nullValue());
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), nullValue());
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), nullValue());
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), nullValue());
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME"), nullValue());
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|JST"), nullValue());
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|PST"), nullValue());
          assertThat(record.getDateTimeFieldValue("TIMESTAMP"), nullValue());
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getMultiLineTextFieldValue("JSON"), nullValue());
          assertThat(record.getSubtableFieldValue("JSON|SUBTABLE"), is(list()));
          assertThat(
              record.getSingleLineTextFieldValue("JSON|SUBTABLE.SINGLE_LINE_TEXT"), nullValue());
          assertThat(updateKey.getField(), is("LONG"));
          assertThat(updateKey.getValue(), nullValue());
        });
  }

  @Test
  public void testIgnoreNulls() {
    KintoneColumnVisitorVerifier verifier = verifier("JSON", "JSON", false, true);
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getFieldValue("BOOLEAN"), nullValue());
          assertThat(record.getFieldValue("LONG|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getFieldValue("LONG"), nullValue());
          assertThat(record.getFieldValue("LONG|DATE"), nullValue());
          assertThat(record.getFieldValue("LONG|DATE|JST"), nullValue());
          assertThat(record.getFieldValue("LONG|DATE|PST"), nullValue());
          assertThat(record.getFieldValue("LONG|TIME"), nullValue());
          assertThat(record.getFieldValue("LONG|TIME|JST"), nullValue());
          assertThat(record.getFieldValue("LONG|TIME|PST"), nullValue());
          assertThat(record.getFieldValue("LONG|DATETIME"), nullValue());
          assertThat(record.getFieldValue("DOUBLE|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getFieldValue("DOUBLE"), nullValue());
          assertThat(record.getFieldValue("DOUBLE|DATE"), nullValue());
          assertThat(record.getFieldValue("DOUBLE|DATE|JST"), nullValue());
          assertThat(record.getFieldValue("DOUBLE|DATE|PST"), nullValue());
          assertThat(record.getFieldValue("DOUBLE|TIME"), nullValue());
          assertThat(record.getFieldValue("DOUBLE|TIME|JST"), nullValue());
          assertThat(record.getFieldValue("DOUBLE|TIME|PST"), nullValue());
          assertThat(record.getFieldValue("DOUBLE|DATETIME"), nullValue());
          assertThat(record.getFieldValue("STRING|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getFieldValue("STRING"), nullValue());
          assertThat(record.getFieldValue("STRING|RICH_TEXT"), nullValue());
          assertThat(record.getFieldValue("STRING|NUMBER"), nullValue());
          assertThat(record.getFieldValue("STRING|CHECK_BOX"), nullValue());
          assertThat(record.getFieldValue("STRING|RADIO_BUTTON"), nullValue());
          assertThat(record.getFieldValue("STRING|MULTI_SELECT"), nullValue());
          assertThat(record.getFieldValue("STRING|DROP_DOWN"), nullValue());
          assertThat(record.getFieldValue("STRING|DATE"), nullValue());
          assertThat(record.getFieldValue("STRING|DATE|JST"), nullValue());
          assertThat(record.getFieldValue("STRING|DATE|PST"), nullValue());
          assertThat(record.getFieldValue("STRING|TIME"), nullValue());
          assertThat(record.getFieldValue("STRING|TIME|JST"), nullValue());
          assertThat(record.getFieldValue("STRING|TIME|PST"), nullValue());
          assertThat(record.getFieldValue("STRING|DATETIME"), nullValue());
          assertThat(record.getFieldValue("STRING|LINK"), nullValue());
          assertThat(record.getFieldValue("STRING|SUBTABLE"), nullValue());
          assertThat(record.getFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getFieldValue("TIMESTAMP|NUMBER"), nullValue());
          assertThat(record.getFieldValue("TIMESTAMP|DATE"), nullValue());
          assertThat(record.getFieldValue("TIMESTAMP|DATE|JST"), nullValue());
          assertThat(record.getFieldValue("TIMESTAMP|DATE|PST"), nullValue());
          assertThat(record.getFieldValue("TIMESTAMP|TIME"), nullValue());
          assertThat(record.getFieldValue("TIMESTAMP|TIME|JST"), nullValue());
          assertThat(record.getFieldValue("TIMESTAMP|TIME|PST"), nullValue());
          assertThat(record.getFieldValue("TIMESTAMP"), nullValue());
          assertThat(record.getFieldValue("JSON|SINGLE_LINE_TEXT"), nullValue());
          assertThat(record.getFieldValue("JSON"), nullValue());
          assertThat(record.getFieldValue("JSON|SUBTABLE"), nullValue());
          assertThat(record.getFieldValue("JSON|SUBTABLE.SINGLE_LINE_TEXT"), nullValue());
          assertThat(updateKey.getField(), nullValue());
          assertThat(updateKey.getValue(), nullValue());
        },
        true);
  }

  @Test
  public void testReduceKey() {
    KintoneColumnVisitorVerifier verifier = verifier("JSON|SUBTABLE", null);
    verifier.verify(
        (record, updateKey) -> {
          assertThat(record.getSingleLineTextFieldValue("BOOLEAN|SINGLE_LINE_TEXT"), is("false"));
          assertThat(record.getNumberFieldValue("BOOLEAN"), is(number("0")));
          assertThat(record.getSingleLineTextFieldValue("LONG|SINGLE_LINE_TEXT"), is("0"));
          assertThat(record.getNumberFieldValue("LONG"), is(number("0")));
          assertThat(record.getDateFieldValue("LONG|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("LONG|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("LONG|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("LONG|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("LONG|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("LONG|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("LONG|DATETIME"), is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("DOUBLE|SINGLE_LINE_TEXT"), is("0.0"));
          assertThat(record.getNumberFieldValue("DOUBLE"), is(number("0.0")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("DOUBLE|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("DOUBLE|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("DOUBLE|DATETIME"),
              is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("STRING|SINGLE_LINE_TEXT"), is(""));
          assertThat(record.getMultiLineTextFieldValue("STRING"), is(""));
          assertThat(record.getRichTextFieldValue("STRING|RICH_TEXT"), is(""));
          assertThat(record.getNumberFieldValue("STRING|NUMBER"), is(number("0")));
          assertThat(record.getCheckBoxFieldValue("STRING|CHECK_BOX"), is(list()));
          assertThat(record.getRadioButtonFieldValue("STRING|RADIO_BUTTON"), is(""));
          assertThat(record.getMultiSelectFieldValue("STRING|MULTI_SELECT"), is(list()));
          assertThat(record.getDropDownFieldValue("STRING|DROP_DOWN"), is(""));
          assertThat(record.getDateFieldValue("STRING|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("STRING|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("STRING|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("STRING|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("STRING|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("STRING|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("STRING|DATETIME"),
              is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getLinkFieldValue("STRING|LINK"), is(""));
          assertThat(record.getSubtableFieldValue("STRING|SUBTABLE"), is(list()));
          assertThat(
              record.getSingleLineTextFieldValue("TIMESTAMP|SINGLE_LINE_TEXT"),
              is("1970-01-01T00:00:00Z"));
          assertThat(record.getNumberFieldValue("TIMESTAMP|NUMBER"), is(number("0")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|JST"), is(date("1970-01-01")));
          assertThat(record.getDateFieldValue("TIMESTAMP|DATE|PST"), is(date("1969-12-31")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME"), is(time("00:00:00")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|JST"), is(time("09:00:00")));
          assertThat(record.getTimeFieldValue("TIMESTAMP|TIME|PST"), is(time("16:00:00")));
          assertThat(
              record.getDateTimeFieldValue("TIMESTAMP"), is(dateTime("1970-01-01T00:00:00Z")));
          assertThat(record.getSingleLineTextFieldValue("JSON|SINGLE_LINE_TEXT"), is("\"\""));
          assertThat(record.getMultiLineTextFieldValue("JSON"), is("\"\""));
          assertThat(record.getSubtableFieldValue("JSON|SUBTABLE"), is(list()));
          assertThat(record.getFieldValue("JSON|SUBTABLE.SINGLE_LINE_TEXT"), nullValue());
          assertThat(updateKey.getField(), nullValue());
          assertThat(updateKey.getValue(), nullValue());
        },
        true);
  }

  @Test
  public void testUpdateKey() {
    assertThrows(UnsupportedOperationException.class, () -> verifier(null, "TIMESTAMP").verify());
    KintoneColumnVisitorVerifier verifier = verifier(null, "TIMESTAMP|NUMBER");
    verifier.verify(
        (record, updateKey) -> {
          assertThat(updateKey.getField(), is("TIMESTAMP|NUMBER"));
          assertThat(updateKey.getValue(), is(number("0")));
        });
    verifier.verify(
        (record, updateKey) -> {
          assertThat(updateKey.getField(), is("TIMESTAMP|NUMBER"));
          assertThat(updateKey.getValue(), is(number("0")));
        });
    verifier.verify(
        (record, updateKey) -> {
          assertThat(updateKey.getField(), is("TIMESTAMP|NUMBER"));
          assertThat(updateKey.getValue(), is(number("946684799")));
        });
    verifier.verify(
        (record, updateKey) -> {
          assertThat(updateKey.getField(), is("TIMESTAMP|NUMBER"));
          assertThat(updateKey.getValue(), is(number("946684800")));
        });
  }

  private static KintoneColumnVisitorVerifier verifier(String reduceKeyName, String updateKeyName) {
    Schema schema = build(Schema.builder());
    return new KintoneColumnVisitorVerifier(
        schema,
        build(ImmutableMap.builder()),
        reduceKeyName,
        updateKeyName,
        OutputPageBuilder.build(schema, KintoneColumnVisitorTest::build));
  }

  private static KintoneColumnVisitorVerifier verifier(
      String reduceKeyName, String updateKeyName, boolean preferNulls, boolean ignoreNulls) {
    Schema schema = build(Schema.builder());
    return new KintoneColumnVisitorVerifier(
        schema,
        build(ImmutableMap.builder()),
        preferNulls,
        ignoreNulls,
        reduceKeyName,
        updateKeyName,
        OutputPageBuilder.build(schema, KintoneColumnVisitorTest::build));
  }

  private static Schema build(Schema.Builder builder) {
    return builder
        .add("BOOLEAN|SINGLE_LINE_TEXT", Types.BOOLEAN)
        .add("BOOLEAN", Types.BOOLEAN)
        .add("LONG|SINGLE_LINE_TEXT", Types.LONG)
        .add("LONG", Types.LONG)
        .add("LONG|DATE", Types.LONG)
        .add("LONG|DATE|JST", Types.LONG)
        .add("LONG|DATE|PST", Types.LONG)
        .add("LONG|TIME", Types.LONG)
        .add("LONG|TIME|JST", Types.LONG)
        .add("LONG|TIME|PST", Types.LONG)
        .add("LONG|DATETIME", Types.LONG)
        .add("DOUBLE|SINGLE_LINE_TEXT", Types.DOUBLE)
        .add("DOUBLE", Types.DOUBLE)
        .add("DOUBLE|DATE", Types.DOUBLE)
        .add("DOUBLE|DATE|JST", Types.DOUBLE)
        .add("DOUBLE|DATE|PST", Types.DOUBLE)
        .add("DOUBLE|TIME", Types.DOUBLE)
        .add("DOUBLE|TIME|JST", Types.DOUBLE)
        .add("DOUBLE|TIME|PST", Types.DOUBLE)
        .add("DOUBLE|DATETIME", Types.DOUBLE)
        .add("STRING|SINGLE_LINE_TEXT", Types.STRING)
        .add("STRING", Types.STRING)
        .add("STRING|RICH_TEXT", Types.STRING)
        .add("STRING|NUMBER", Types.STRING)
        .add("STRING|CHECK_BOX", Types.STRING)
        .add("STRING|RADIO_BUTTON", Types.STRING)
        .add("STRING|MULTI_SELECT", Types.STRING)
        .add("STRING|DROP_DOWN", Types.STRING)
        .add("STRING|DATE", Types.STRING)
        .add("STRING|DATE|JST", Types.STRING)
        .add("STRING|DATE|PST", Types.STRING)
        .add("STRING|TIME", Types.STRING)
        .add("STRING|TIME|JST", Types.STRING)
        .add("STRING|TIME|PST", Types.STRING)
        .add("STRING|DATETIME", Types.STRING)
        .add("STRING|LINK", Types.STRING)
        .add("STRING|SUBTABLE", Types.STRING)
        .add("TIMESTAMP|SINGLE_LINE_TEXT", Types.TIMESTAMP)
        .add("TIMESTAMP|NUMBER", Types.TIMESTAMP)
        .add("TIMESTAMP|DATE", Types.TIMESTAMP)
        .add("TIMESTAMP|DATE|JST", Types.TIMESTAMP)
        .add("TIMESTAMP|DATE|PST", Types.TIMESTAMP)
        .add("TIMESTAMP|TIME", Types.TIMESTAMP)
        .add("TIMESTAMP|TIME|JST", Types.TIMESTAMP)
        .add("TIMESTAMP|TIME|PST", Types.TIMESTAMP)
        .add("TIMESTAMP", Types.TIMESTAMP)
        .add("JSON|SINGLE_LINE_TEXT", Types.JSON)
        .add("JSON", Types.JSON)
        .add("JSON|SUBTABLE", Types.JSON)
        .add("JSON|SUBTABLE.SINGLE_LINE_TEXT", Types.JSON)
        .build();
  }

  private static Map<String, KintoneColumnOption> build(
      ImmutableMap.Builder<String, KintoneColumnOption> builder) {
    return builder
        .put(build("BOOLEAN|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("BOOLEAN", it -> it.setType("NUMBER")))
        .put(build("LONG|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("LONG", it -> it.setType("NUMBER")))
        .put(build("LONG|DATE", it -> it.setType("DATE").setTimezone("UTC")))
        .put(build("LONG|DATE|JST", it -> it.setType("DATE").setTimezone("Asia/Tokyo")))
        .put(build("LONG|DATE|PST", it -> it.setType("DATE").setTimezone("US/Pacific")))
        .put(build("LONG|TIME", it -> it.setType("TIME").setTimezone("UTC")))
        .put(build("LONG|TIME|JST", it -> it.setType("TIME").setTimezone("Asia/Tokyo")))
        .put(build("LONG|TIME|PST", it -> it.setType("TIME").setTimezone("US/Pacific")))
        .put(build("LONG|DATETIME", it -> it.setType("DATETIME")))
        .put(build("DOUBLE|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("DOUBLE", it -> it.setType("NUMBER")))
        .put(build("DOUBLE|DATE", it -> it.setType("DATE").setTimezone("UTC")))
        .put(build("DOUBLE|DATE|JST", it -> it.setType("DATE").setTimezone("Asia/Tokyo")))
        .put(build("DOUBLE|DATE|PST", it -> it.setType("DATE").setTimezone("US/Pacific")))
        .put(build("DOUBLE|TIME", it -> it.setType("TIME").setTimezone("UTC")))
        .put(build("DOUBLE|TIME|JST", it -> it.setType("TIME").setTimezone("Asia/Tokyo")))
        .put(build("DOUBLE|TIME|PST", it -> it.setType("TIME").setTimezone("US/Pacific")))
        .put(build("DOUBLE|DATETIME", it -> it.setType("DATETIME")))
        .put(build("STRING|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("STRING", it -> it.setType("MULTI_LINE_TEXT")))
        .put(build("STRING|RICH_TEXT", it -> it.setType("RICH_TEXT")))
        .put(build("STRING|NUMBER", it -> it.setType("NUMBER")))
        .put(build("STRING|CHECK_BOX", it -> it.setType("CHECK_BOX").setValueSeparator(",")))
        .put(build("STRING|RADIO_BUTTON", it -> it.setType("RADIO_BUTTON")))
        .put(build("STRING|MULTI_SELECT", it -> it.setType("MULTI_SELECT").setValueSeparator(",")))
        .put(build("STRING|DROP_DOWN", it -> it.setType("DROP_DOWN")))
        .put(build("STRING|DATE", it -> it.setType("DATE").setTimezone("UTC")))
        .put(build("STRING|DATE|JST", it -> it.setType("DATE").setTimezone("Asia/Tokyo")))
        .put(build("STRING|DATE|PST", it -> it.setType("DATE").setTimezone("US/Pacific")))
        .put(build("STRING|TIME", it -> it.setType("TIME").setTimezone("UTC")))
        .put(build("STRING|TIME|JST", it -> it.setType("TIME").setTimezone("Asia/Tokyo")))
        .put(build("STRING|TIME|PST", it -> it.setType("TIME").setTimezone("US/Pacific")))
        .put(build("STRING|DATETIME", it -> it.setType("DATETIME")))
        .put(build("STRING|LINK", it -> it.setType("LINK")))
        .put(build("STRING|SUBTABLE", it -> it.setType("SUBTABLE")))
        .put(build("TIMESTAMP|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("TIMESTAMP|NUMBER", it -> it.setType("NUMBER")))
        .put(build("TIMESTAMP|DATE", it -> it.setType("DATE").setTimezone("UTC")))
        .put(build("TIMESTAMP|DATE|JST", it -> it.setType("DATE").setTimezone("Asia/Tokyo")))
        .put(build("TIMESTAMP|DATE|PST", it -> it.setType("DATE").setTimezone("US/Pacific")))
        .put(build("TIMESTAMP|TIME", it -> it.setType("TIME").setTimezone("UTC")))
        .put(build("TIMESTAMP|TIME|JST", it -> it.setType("TIME").setTimezone("Asia/Tokyo")))
        .put(build("TIMESTAMP|TIME|PST", it -> it.setType("TIME").setTimezone("US/Pacific")))
        .put(build("TIMESTAMP", it -> it.setType("DATETIME")))
        .put(build("JSON|SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
        .put(build("JSON", it -> it.setType("MULTI_LINE_TEXT")))
        .put(build("JSON|SUBTABLE", it -> it.setType("SUBTABLE")))
        .put(build("JSON|SUBTABLE.SINGLE_LINE_TEXT", it -> it.setType("SINGLE_LINE_TEXT")))
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
        .setNull("BOOLEAN")
        .setNull("LONG|SINGLE_LINE_TEXT")
        .setNull("LONG")
        .setNull("LONG|DATE")
        .setNull("LONG|DATE|JST")
        .setNull("LONG|DATE|PST")
        .setNull("LONG|TIME")
        .setNull("LONG|TIME|JST")
        .setNull("LONG|TIME|PST")
        .setNull("LONG|DATETIME")
        .setNull("DOUBLE|SINGLE_LINE_TEXT")
        .setNull("DOUBLE")
        .setNull("DOUBLE|DATE")
        .setNull("DOUBLE|DATE|JST")
        .setNull("DOUBLE|DATE|PST")
        .setNull("DOUBLE|TIME")
        .setNull("DOUBLE|TIME|JST")
        .setNull("DOUBLE|TIME|PST")
        .setNull("DOUBLE|DATETIME")
        .setNull("STRING|SINGLE_LINE_TEXT")
        .setNull("STRING")
        .setNull("STRING|RICH_TEXT")
        .setNull("STRING|NUMBER")
        .setNull("STRING|CHECK_BOX")
        .setNull("STRING|RADIO_BUTTON")
        .setNull("STRING|MULTI_SELECT")
        .setNull("STRING|DROP_DOWN")
        .setNull("STRING|DATE")
        .setNull("STRING|DATE|JST")
        .setNull("STRING|DATE|PST")
        .setNull("STRING|TIME")
        .setNull("STRING|TIME|JST")
        .setNull("STRING|TIME|PST")
        .setNull("STRING|DATETIME")
        .setNull("STRING|LINK")
        .setNull("STRING|SUBTABLE")
        .setNull("TIMESTAMP|SINGLE_LINE_TEXT")
        .setNull("TIMESTAMP|NUMBER")
        .setNull("TIMESTAMP|DATE")
        .setNull("TIMESTAMP|DATE|JST")
        .setNull("TIMESTAMP|DATE|PST")
        .setNull("TIMESTAMP|TIME")
        .setNull("TIMESTAMP|TIME|JST")
        .setNull("TIMESTAMP|TIME|PST")
        .setNull("TIMESTAMP")
        .setNull("JSON|SINGLE_LINE_TEXT")
        .setNull("JSON")
        .setNull("JSON|SUBTABLE")
        .setNull("JSON|SUBTABLE.SINGLE_LINE_TEXT")
        .addRecord()
        .setBoolean("BOOLEAN|SINGLE_LINE_TEXT", false)
        .setBoolean("BOOLEAN", false)
        .setLong("LONG|SINGLE_LINE_TEXT", 0)
        .setLong("LONG", 0)
        .setLong("LONG|DATE", 0)
        .setLong("LONG|DATE|JST", 0)
        .setLong("LONG|DATE|PST", 0)
        .setLong("LONG|TIME", 0)
        .setLong("LONG|TIME|JST", 0)
        .setLong("LONG|TIME|PST", 0)
        .setLong("LONG|DATETIME", 0)
        .setDouble("DOUBLE|SINGLE_LINE_TEXT", 0)
        .setDouble("DOUBLE", 0)
        .setDouble("DOUBLE|DATE", 0)
        .setDouble("DOUBLE|DATE|JST", 0)
        .setDouble("DOUBLE|DATE|PST", 0)
        .setDouble("DOUBLE|TIME", 0)
        .setDouble("DOUBLE|TIME|JST", 0)
        .setDouble("DOUBLE|TIME|PST", 0)
        .setDouble("DOUBLE|DATETIME", 0)
        .setString("STRING|SINGLE_LINE_TEXT", "")
        .setString("STRING", "")
        .setString("STRING|RICH_TEXT", "")
        .setString("STRING|NUMBER", "")
        .setString("STRING|CHECK_BOX", "")
        .setString("STRING|RADIO_BUTTON", "")
        .setString("STRING|MULTI_SELECT", "")
        .setString("STRING|DROP_DOWN", "")
        .setString("STRING|DATE", "")
        .setString("STRING|DATE|JST", "")
        .setString("STRING|DATE|PST", "")
        .setString("STRING|TIME", "")
        .setString("STRING|TIME|JST", "")
        .setString("STRING|TIME|PST", "")
        .setString("STRING|DATETIME", "")
        .setString("STRING|LINK", "")
        .setString("STRING|SUBTABLE", "")
        .setTimestamp("TIMESTAMP|SINGLE_LINE_TEXT", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|NUMBER", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|DATE", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|DATE|JST", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|DATE|PST", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|TIME", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|TIME|JST", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP|TIME|PST", Timestamp.ofInstant(Instant.EPOCH))
        .setTimestamp("TIMESTAMP", Timestamp.ofInstant(Instant.EPOCH))
        .setJson("JSON|SINGLE_LINE_TEXT", ValueFactory.newString(""))
        .setJson("JSON", ValueFactory.newString(""))
        .setJson("JSON|SUBTABLE", ValueFactory.newString(""))
        .setJson("JSON|SUBTABLE.SINGLE_LINE_TEXT", ValueFactory.newString(""))
        .addRecord()
        .setBoolean("BOOLEAN|SINGLE_LINE_TEXT", true)
        .setBoolean("BOOLEAN", true)
        .setLong("LONG|SINGLE_LINE_TEXT", 123)
        .setLong("LONG", 123)
        .setLong("LONG|DATE", 946684799)
        .setLong("LONG|DATE|JST", 946684799)
        .setLong("LONG|DATE|PST", 946684799)
        .setLong("LONG|TIME", 946684799)
        .setLong("LONG|TIME|JST", 946684799)
        .setLong("LONG|TIME|PST", 946684799)
        .setLong("LONG|DATETIME", 946684799)
        .setDouble("DOUBLE|SINGLE_LINE_TEXT", 123)
        .setDouble("DOUBLE", 123)
        .setDouble("DOUBLE|DATE", 946684799)
        .setDouble("DOUBLE|DATE|JST", 946684799)
        .setDouble("DOUBLE|DATE|PST", 946684799)
        .setDouble("DOUBLE|TIME", 946684799)
        .setDouble("DOUBLE|TIME|JST", 946684799)
        .setDouble("DOUBLE|TIME|PST", 946684799)
        .setDouble("DOUBLE|DATETIME", 946684799)
        .setString("STRING|SINGLE_LINE_TEXT", "abc")
        .setString("STRING", "abc")
        .setString("STRING|RICH_TEXT", "abc")
        .setString("STRING|NUMBER", "123")
        .setString("STRING|CHECK_BOX", "123,abc")
        .setString("STRING|RADIO_BUTTON", "abc")
        .setString("STRING|MULTI_SELECT", "123,abc")
        .setString("STRING|DROP_DOWN", "abc")
        .setString("STRING|DATE", "1999-12-31")
        .setString("STRING|DATE|JST", "1999-12-31")
        .setString("STRING|DATE|PST", "1999-12-31")
        .setString("STRING|TIME", "23:59:59")
        .setString("STRING|TIME|JST", "23:59:59")
        .setString("STRING|TIME|PST", "23:59:59")
        .setString("STRING|DATETIME", "1999-12-31T23:59:59Z")
        .setString("STRING|LINK", "abc")
        .setString("STRING|SUBTABLE", String.format("[%s,%s,%s]", ROWS[0], ROWS[1], ROWS[2]))
        .setTimestamp("TIMESTAMP|SINGLE_LINE_TEXT", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|NUMBER", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|DATE", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|DATE|JST", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|DATE|PST", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|TIME", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|TIME|JST", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP|TIME|PST", timestamp("1999-12-31T23:59:59Z"))
        .setTimestamp("TIMESTAMP", timestamp("1999-12-31T23:59:59Z"))
        .setJson("JSON|SINGLE_LINE_TEXT", ValueFactory.newString("abc"))
        .setJson("JSON", ValueFactory.newString("abc"))
        .setJson(
            "JSON|SUBTABLE", ValueFactory.newArray(value(ROWS[0]), value(ROWS[1]), value(ROWS[2])))
        .setJson("JSON|SUBTABLE.SINGLE_LINE_TEXT", ValueFactory.newString("abc"))
        .addRecord()
        .setBoolean("BOOLEAN|SINGLE_LINE_TEXT", false)
        .setBoolean("BOOLEAN", false)
        .setLong("LONG|SINGLE_LINE_TEXT", 456)
        .setLong("LONG", 456)
        .setLong("LONG|DATE", 946684800)
        .setLong("LONG|DATE|JST", 946684800)
        .setLong("LONG|DATE|PST", 946684800)
        .setLong("LONG|TIME", 946684800)
        .setLong("LONG|TIME|JST", 946684800)
        .setLong("LONG|TIME|PST", 946684800)
        .setLong("LONG|DATETIME", 946684800)
        .setDouble("DOUBLE|SINGLE_LINE_TEXT", 456)
        .setDouble("DOUBLE", 456)
        .setDouble("DOUBLE|DATE", 946684800)
        .setDouble("DOUBLE|DATE|JST", 946684800)
        .setDouble("DOUBLE|DATE|PST", 946684800)
        .setDouble("DOUBLE|TIME", 946684800)
        .setDouble("DOUBLE|TIME|JST", 946684800)
        .setDouble("DOUBLE|TIME|PST", 946684800)
        .setDouble("DOUBLE|DATETIME", 946684800)
        .setString("STRING|SINGLE_LINE_TEXT", "def")
        .setString("STRING", "def")
        .setString("STRING|RICH_TEXT", "def")
        .setString("STRING|NUMBER", "456")
        .setString("STRING|CHECK_BOX", "456,def")
        .setString("STRING|RADIO_BUTTON", "def")
        .setString("STRING|MULTI_SELECT", "456,def")
        .setString("STRING|DROP_DOWN", "def")
        .setString("STRING|DATE", "2000-01-01")
        .setString("STRING|DATE|JST", "2000-01-01")
        .setString("STRING|DATE|PST", "2000-01-01")
        .setString("STRING|TIME", "00:00:00")
        .setString("STRING|TIME|JST", "00:00:00")
        .setString("STRING|TIME|PST", "00:00:00")
        .setString("STRING|DATETIME", "2000-01-01T00:00:00Z")
        .setString("STRING|LINK", "def")
        .setString("STRING|SUBTABLE", String.format("[%s,%s,%s]", ROWS[3], ROWS[4], ROWS[5]))
        .setTimestamp("TIMESTAMP|SINGLE_LINE_TEXT", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|NUMBER", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|DATE", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|DATE|JST", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|DATE|PST", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|TIME", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|TIME|JST", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP|TIME|PST", timestamp("2000-01-01T00:00:00Z"))
        .setTimestamp("TIMESTAMP", timestamp("2000-01-01T00:00:00Z"))
        .setJson("JSON|SINGLE_LINE_TEXT", ValueFactory.newString("def"))
        .setJson("JSON", ValueFactory.newString("def"))
        .setJson(
            "JSON|SUBTABLE", ValueFactory.newArray(value(ROWS[3]), value(ROWS[4]), value(ROWS[5])))
        .setJson("JSON|SUBTABLE.SINGLE_LINE_TEXT", ValueFactory.newString("def"))
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

  private static List<TableRow> rows(Long... ids) {
    return Arrays.stream(ids).map(DeserializerTest::tableRow).collect(Collectors.toList());
  }

  private static LocalDate date(CharSequence text) {
    return LocalDate.parse(text);
  }

  private static LocalTime time(CharSequence text) {
    return LocalTime.parse(text);
  }

  private static ZonedDateTime dateTime(CharSequence text) {
    return ZonedDateTime.parse(text);
  }

  private static Timestamp timestamp(CharSequence text) {
    return Timestamp.ofInstant(Instant.parse(text));
  }

  private static Value value(String json) {
    return PARSER.parse(json);
  }
}
