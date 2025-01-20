package org.embulk.output.kintone;

import static org.embulk.output.kintone.KintoneColumnType.CHECK_BOX;
import static org.embulk.output.kintone.KintoneColumnType.DATE;
import static org.embulk.output.kintone.KintoneColumnType.DATETIME;
import static org.embulk.output.kintone.KintoneColumnType.DROP_DOWN;
import static org.embulk.output.kintone.KintoneColumnType.FILE;
import static org.embulk.output.kintone.KintoneColumnType.GROUP_SELECT;
import static org.embulk.output.kintone.KintoneColumnType.LINK;
import static org.embulk.output.kintone.KintoneColumnType.MULTI_LINE_TEXT;
import static org.embulk.output.kintone.KintoneColumnType.MULTI_SELECT;
import static org.embulk.output.kintone.KintoneColumnType.NUMBER;
import static org.embulk.output.kintone.KintoneColumnType.ORGANIZATION_SELECT;
import static org.embulk.output.kintone.KintoneColumnType.RADIO_BUTTON;
import static org.embulk.output.kintone.KintoneColumnType.RICH_TEXT;
import static org.embulk.output.kintone.KintoneColumnType.SINGLE_LINE_TEXT;
import static org.embulk.output.kintone.KintoneColumnType.SUBTABLE;
import static org.embulk.output.kintone.KintoneColumnType.TIME;
import static org.embulk.output.kintone.KintoneColumnType.USER_SELECT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.kintone.client.model.Group;
import com.kintone.client.model.Organization;
import com.kintone.client.model.User;
import com.kintone.client.model.record.CheckBoxFieldValue;
import com.kintone.client.model.record.DateFieldValue;
import com.kintone.client.model.record.DateTimeFieldValue;
import com.kintone.client.model.record.DropDownFieldValue;
import com.kintone.client.model.record.GroupSelectFieldValue;
import com.kintone.client.model.record.LinkFieldValue;
import com.kintone.client.model.record.MultiLineTextFieldValue;
import com.kintone.client.model.record.MultiSelectFieldValue;
import com.kintone.client.model.record.NumberFieldValue;
import com.kintone.client.model.record.OrganizationSelectFieldValue;
import com.kintone.client.model.record.RadioButtonFieldValue;
import com.kintone.client.model.record.RichTextFieldValue;
import com.kintone.client.model.record.SingleLineTextFieldValue;
import com.kintone.client.model.record.SubtableFieldValue;
import com.kintone.client.model.record.TableRow;
import com.kintone.client.model.record.TimeFieldValue;
import com.kintone.client.model.record.UserSelectFieldValue;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.embulk.output.kintone.deserializer.DeserializerTest;
import org.junit.Test;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class KintoneColumnTypeTest {
  private static final Instant EPOCH = Instant.EPOCH;
  private static final Value EMPTY = ValueFactory.newString("");

  @Test
  public void testSupportedTypes() {
    // spotless:off
    assertThat(((SingleLineTextFieldValue) SINGLE_LINE_TEXT.getFieldValue(false, null)).getValue(), is("false"));
    assertThat(((SingleLineTextFieldValue) SINGLE_LINE_TEXT.getFieldValue(0L, null)).getValue(), is("0"));
    assertThat(((SingleLineTextFieldValue) SINGLE_LINE_TEXT.getFieldValue(0.0d, null)).getValue(), is("0.0"));
    assertThat(((SingleLineTextFieldValue) SINGLE_LINE_TEXT.getFieldValue("", null)).getValue(), is(""));
    assertThat(((SingleLineTextFieldValue) SINGLE_LINE_TEXT.getFieldValue(EPOCH, null)).getValue(), is("1970-01-01T00:00:00Z"));
    assertThat(((SingleLineTextFieldValue) SINGLE_LINE_TEXT.getFieldValue(EMPTY, null)).getValue(), is("\"\""));
    assertThat(((MultiLineTextFieldValue) MULTI_LINE_TEXT.getFieldValue(false, null)).getValue(), is("false"));
    assertThat(((MultiLineTextFieldValue) MULTI_LINE_TEXT.getFieldValue(0L, null)).getValue(), is("0"));
    assertThat(((MultiLineTextFieldValue) MULTI_LINE_TEXT.getFieldValue(0.0d, null)).getValue(), is("0.0"));
    assertThat(((MultiLineTextFieldValue) MULTI_LINE_TEXT.getFieldValue("", null)).getValue(), is(""));
    assertThat(((MultiLineTextFieldValue) MULTI_LINE_TEXT.getFieldValue(EPOCH, null)).getValue(), is("1970-01-01T00:00:00Z"));
    assertThat(((MultiLineTextFieldValue) MULTI_LINE_TEXT.getFieldValue(EMPTY, null)).getValue(), is("\"\""));
    assertThat(((RichTextFieldValue) RICH_TEXT.getFieldValue(false, null)).getValue(), is("false"));
    assertThat(((RichTextFieldValue) RICH_TEXT.getFieldValue(0L, null)).getValue(), is("0"));
    assertThat(((RichTextFieldValue) RICH_TEXT.getFieldValue(0.0d, null)).getValue(), is("0.0"));
    assertThat(((RichTextFieldValue) RICH_TEXT.getFieldValue("", null)).getValue(), is(""));
    assertThat(((RichTextFieldValue) RICH_TEXT.getFieldValue(EPOCH, null)).getValue(), is("1970-01-01T00:00:00Z"));
    assertThat(((RichTextFieldValue) RICH_TEXT.getFieldValue(EMPTY, null)).getValue(), is("\"\""));
    assertThat(((NumberFieldValue) NUMBER.getFieldValue(false, null)).getValue(), is(number("0")));
    assertThat(((NumberFieldValue) NUMBER.getFieldValue(0L, null)).getValue(), is(number("0")));
    assertThat(((NumberFieldValue) NUMBER.getFieldValue(0.0d, null)).getValue(), is(number("0.0")));
    assertThat(((NumberFieldValue) NUMBER.getFieldValue("", null)).getValue(), is(number("0")));
    assertThat(((NumberFieldValue) NUMBER.getFieldValue(EPOCH, null)).getValue(), is(number("0")));
    assertThat(((CheckBoxFieldValue) CHECK_BOX.getFieldValue("", null)).getValues(), is(list()));
    assertThat(((RadioButtonFieldValue) RADIO_BUTTON.getFieldValue("", null)).getValue(), is(""));
    assertThat(((MultiSelectFieldValue) MULTI_SELECT.getFieldValue("", null)).getValues(), is(list()));
    assertThat(((DropDownFieldValue) DROP_DOWN.getFieldValue("", null)).getValue(), is(""));
    assertThat(((UserSelectFieldValue) USER_SELECT.getFieldValue("", null)).getValues(), is(users()));
    assertThat(((OrganizationSelectFieldValue) ORGANIZATION_SELECT.getFieldValue("", null)).getValues(), is(organizations()));
    assertThat(((GroupSelectFieldValue) GROUP_SELECT.getFieldValue("", null)).getValues(), is(groups()));
    assertThat(((DateFieldValue) DATE.getFieldValue(0L, null)).getValue(), is(date("1970-01-01")));
    assertThat(((DateFieldValue) DATE.getFieldValue(0.0d, null)).getValue(), is(date("1970-01-01")));
    assertThat(((DateFieldValue) DATE.getFieldValue("", null)).getValue(), is(date("1970-01-01")));
    assertThat(((DateFieldValue) DATE.getFieldValue(EPOCH, null)).getValue(), is(date("1970-01-01")));
    assertThat(((TimeFieldValue) TIME.getFieldValue(0L, null)).getValue(), is(time("00:00:00")));
    assertThat(((TimeFieldValue) TIME.getFieldValue(0.0d, null)).getValue(), is(time("00:00:00")));
    assertThat(((TimeFieldValue) TIME.getFieldValue("", null)).getValue(), is(time("00:00:00")));
    assertThat(((TimeFieldValue) TIME.getFieldValue(EPOCH, null)).getValue(), is(time("00:00:00")));
    assertThat(((DateTimeFieldValue) DATETIME.getFieldValue(0L, null)).getValue(), is(dateTime("1970-01-01T00:00:00Z")));
    assertThat(((DateTimeFieldValue) DATETIME.getFieldValue(0.0d, null)).getValue(), is(dateTime("1970-01-01T00:00:00Z")));
    assertThat(((DateTimeFieldValue) DATETIME.getFieldValue("", null)).getValue(), is(dateTime("1970-01-01T00:00:00Z")));
    assertThat(((DateTimeFieldValue) DATETIME.getFieldValue(EPOCH, null)).getValue(), is(dateTime("1970-01-01T00:00:00Z")));
    assertThat(((LinkFieldValue) LINK.getFieldValue("", null)).getValue(), is(""));
    assertThat(((SubtableFieldValue) SUBTABLE.getFieldValue("", null)).getRows(), is(rows()));
    assertThat(((SubtableFieldValue) SUBTABLE.getFieldValue(EMPTY, null)).getRows(), is(rows()));
    // spotless:on
  }

  @Test
  public void testUnsupportedTypes() {
    // spotless:off
    assertThrows(UnsupportedOperationException.class, () -> NUMBER.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> CHECK_BOX.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> CHECK_BOX.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> CHECK_BOX.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> CHECK_BOX.getFieldValue(EPOCH, null));
    assertThrows(UnsupportedOperationException.class, () -> CHECK_BOX.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> RADIO_BUTTON.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> RADIO_BUTTON.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> RADIO_BUTTON.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> RADIO_BUTTON.getFieldValue(EPOCH, null));
    assertThrows(UnsupportedOperationException.class, () -> RADIO_BUTTON.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> MULTI_SELECT.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> MULTI_SELECT.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> MULTI_SELECT.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> MULTI_SELECT.getFieldValue(EPOCH, null));
    assertThrows(UnsupportedOperationException.class, () -> MULTI_SELECT.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> DROP_DOWN.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> DROP_DOWN.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> DROP_DOWN.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> DROP_DOWN.getFieldValue(EPOCH, null));
    assertThrows(UnsupportedOperationException.class, () -> DROP_DOWN.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> USER_SELECT.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> USER_SELECT.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> USER_SELECT.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> USER_SELECT.getFieldValue(EPOCH, null));
    assertThrows(UnsupportedOperationException.class, () -> USER_SELECT.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> ORGANIZATION_SELECT.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> ORGANIZATION_SELECT.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> ORGANIZATION_SELECT.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> ORGANIZATION_SELECT.getFieldValue(EPOCH, null));
    assertThrows(UnsupportedOperationException.class, () -> ORGANIZATION_SELECT.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> GROUP_SELECT.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> GROUP_SELECT.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> GROUP_SELECT.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> GROUP_SELECT.getFieldValue(EPOCH, null));
    assertThrows(UnsupportedOperationException.class, () -> GROUP_SELECT.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> DATE.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> DATE.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> TIME.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> TIME.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> DATETIME.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> DATETIME.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> LINK.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> LINK.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> LINK.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> LINK.getFieldValue(EPOCH, null));
    assertThrows(UnsupportedOperationException.class, () -> LINK.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> FILE.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> FILE.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> FILE.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> FILE.getFieldValue("", null));
    assertThrows(UnsupportedOperationException.class, () -> FILE.getFieldValue(EPOCH, null));
    assertThrows(UnsupportedOperationException.class, () -> FILE.getFieldValue(EMPTY, null));
    assertThrows(UnsupportedOperationException.class, () -> SUBTABLE.getFieldValue(false, null));
    assertThrows(UnsupportedOperationException.class, () -> SUBTABLE.getFieldValue(0L, null));
    assertThrows(UnsupportedOperationException.class, () -> SUBTABLE.getFieldValue(0.0d, null));
    assertThrows(UnsupportedOperationException.class, () -> SUBTABLE.getFieldValue(EPOCH, null));
    // spotless:on
  }

  public static BigDecimal number(String value) {
    return new BigDecimal(value);
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  public static <T> List<T> list(T... a) {
    return Arrays.asList(a);
  }

  public static List<User> users(String... codes) {
    return Arrays.stream(codes).map(User::new).collect(Collectors.toList());
  }

  public static List<Organization> organizations(String... codes) {
    return Arrays.stream(codes).map(Organization::new).collect(Collectors.toList());
  }

  public static List<Group> groups(String... codes) {
    return Arrays.stream(codes).map(Group::new).collect(Collectors.toList());
  }

  public static List<TableRow> rows(Long... ids) {
    return Arrays.stream(ids).map(DeserializerTest::tableRow).collect(Collectors.toList());
  }

  public static LocalDate date(CharSequence text) {
    return LocalDate.parse(text);
  }

  public static LocalTime time(CharSequence text) {
    return LocalTime.parse(text);
  }

  public static ZonedDateTime dateTime(CharSequence text) {
    return ZonedDateTime.parse(text);
  }
}
