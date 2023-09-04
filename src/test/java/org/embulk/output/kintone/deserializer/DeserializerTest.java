package org.embulk.output.kintone.deserializer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.kintone.client.model.FileBody;
import com.kintone.client.model.Group;
import com.kintone.client.model.Organization;
import com.kintone.client.model.User;
import com.kintone.client.model.record.CalcFieldValue;
import com.kintone.client.model.record.CheckBoxFieldValue;
import com.kintone.client.model.record.DateFieldValue;
import com.kintone.client.model.record.DateTimeFieldValue;
import com.kintone.client.model.record.DropDownFieldValue;
import com.kintone.client.model.record.FieldValue;
import com.kintone.client.model.record.FileFieldValue;
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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.junit.Test;

public class DeserializerTest {
  // spotless:off
  public static final Function<Long, String> TABLE_ROW = (id) -> String.format("{\"id\":%d,\"value\":{\"リッチエディター\":{\"type\":\"RICH_TEXT\",\"value\":\"\\u003ca href\\u003d\\\"https://www.cybozu.com\\\"\\u003eサイボウズ\\u003c/a\\u003e\"},\"グループ選択\":{\"type\":\"GROUP_SELECT\",\"value\":[{\"name\":\"プロジェクトマネージャー\",\"code\":\"project_manager\"},{\"name\":\"チームリーダー\",\"code\":\"team_leader\"}]},\"文字列（1行）\":{\"type\":\"SINGLE_LINE_TEXT\",\"value\":\"テストです。\"},\"ラジオボタン\":{\"type\":\"RADIO_BUTTON\",\"value\":\"選択肢3\"},\"ドロップダウン\":{\"type\":\"DROP_DOWN\",\"value\":\"選択肢6\"},\"組織選択\":{\"type\":\"ORGANIZATION_SELECT\",\"value\":[{\"name\":\"開発部\",\"code\":\"kaihatsu\"},{\"name\":\"人事部\",\"code\":\"jinji\"}]},\"ユーザー選択\":{\"type\":\"USER_SELECT\",\"value\":[{\"name\":\"Noboru Sato\",\"code\":\"guest/sato@cybozu.com\"},{\"name\":\"Misaki Kato\",\"code\":\"kato\"}]},\"日時\":{\"type\":\"DATETIME\",\"value\":\"2012-01-11T11:30:00Z\"},\"文字列（複数行）\":{\"type\":\"MULTI_LINE_TEXT\",\"value\":\"テスト\\nです。\"},\"時刻\":{\"type\":\"TIME\",\"value\":\"11:30\"},\"チェックボックス\":{\"type\":\"CHECK_BOX\",\"value\":[\"選択肢1\",\"選択肢2\"]},\"複数選択\":{\"type\":\"MULTI_SELECT\",\"value\":[\"選択肢4\",\"選択肢5\"]},\"数値\":{\"type\":\"NUMBER\",\"value\":\"123\"},\"添付ファイル\":{\"type\":\"FILE\",\"value\":[{\"contentType\":\"text/plain\",\"fileKey\":\"201202061155587E339F9067544F1A92C743460E3D12B3297\",\"name\":\"17to20_VerupLog (1).txt\",\"size\":\"23175\"},{\"contentType\":\"application/json\",\"fileKey\":\"201202061155583C763E30196F419E83E91D2E4A03746C273\",\"name\":\"17to20_VerupLog.txt\",\"size\":\"23176\"}]},\"リンク\":{\"type\":\"LINK\",\"value\":\"https://cybozu.co.jp/\"},\"計算\":{\"type\":\"CALC\",\"value\":\"456\"},\"日付\":{\"type\":\"DATE\",\"value\":\"2012-01-11\"}}}", id);
  private static final String NULL_TABLE_ROW = "{\"value\":{\"添付ファイル（null要素）\":{\"type\":\"FILE\",\"value\":[null,null]},\"添付ファイル（null項目）\":{\"type\":\"FILE\",\"value\":[{},{}]},\"複数選択（空）\":{\"type\":\"MULTI_SELECT\",\"value\":[]},\"リッチエディター\":{\"type\":\"RICH_TEXT\"},\"文字列（1行）\":{\"type\":\"SINGLE_LINE_TEXT\"},\"ユーザー選択（null項目）\":{\"type\":\"USER_SELECT\",\"value\":[{},{}]},\"文字列（複数行）\":{\"type\":\"MULTI_LINE_TEXT\"},\"ユーザー選択（空）\":{\"type\":\"USER_SELECT\",\"value\":[]},\"チェックボックス（null要素）\":{\"type\":\"CHECK_BOX\",\"value\":[null,null]},\"組織選択（null項目）\":{\"type\":\"ORGANIZATION_SELECT\",\"value\":[{},{}]},\"計算\":{\"type\":\"CALC\"},\"日付\":{\"type\":\"DATE\"},\"組織選択（空）\":{\"type\":\"ORGANIZATION_SELECT\",\"value\":[]},\"添付ファイル（空）\":{\"type\":\"FILE\",\"value\":[]},\"ラジオボタン\":{\"type\":\"RADIO_BUTTON\"},\"グループ選択（null項目）\":{\"type\":\"GROUP_SELECT\",\"value\":[{},{}]},\"複数選択（null要素）\":{\"type\":\"MULTI_SELECT\",\"value\":[null,null]},\"ドロップダウン\":{\"type\":\"DROP_DOWN\"},\"日時\":{\"type\":\"DATETIME\"},\"組織選択（null要素）\":{\"type\":\"ORGANIZATION_SELECT\",\"value\":[null,null]},\"時刻\":{\"type\":\"TIME\"},\"グループ選択（空）\":{\"type\":\"GROUP_SELECT\",\"value\":[]},\"数値\":{\"type\":\"NUMBER\"},\"ユーザー選択（null要素）\":{\"type\":\"USER_SELECT\",\"value\":[null,null]},\"グループ選択（null要素）\":{\"type\":\"GROUP_SELECT\",\"value\":[null,null]},\"リンク\":{\"type\":\"LINK\"},\"チェックボックス（空）\":{\"type\":\"CHECK_BOX\",\"value\":[]}}}";
  // spotless:on
  @Test
  public void deserialize() {
    // spotless:off
    SubtableFieldValue actual = new Deserializer().deserialize(String.format("[%s,%s]", TABLE_ROW.apply(48290L), TABLE_ROW.apply(48291L)), SubtableFieldValue.class);
    // spotless:on
    SubtableFieldValue expected = new SubtableFieldValue(tableRow(48290L), tableRow(48291L));
    assertTableRows(actual.getRows(), expected.getRows());
  }

  @Test
  public void deserializeNull() {
    // spotless:off
    SubtableFieldValue actual = new Deserializer().deserialize(String.format("[%s,%s]", NULL_TABLE_ROW, NULL_TABLE_ROW), SubtableFieldValue.class);
    // spotless:on
    SubtableFieldValue expected = new SubtableFieldValue(nullTableRow(), nullTableRow());
    assertTableRows(actual.getRows(), expected.getRows());
  }

  public static void assertTableRows(List<TableRow> actual, List<TableRow> expected) {
    assertTableRows("", actual, expected);
  }

  public static void assertTableRows(
      String domain, List<TableRow> actual, List<TableRow> expected) {
    assertThat(domain, actual.size(), is(expected.size()));
    // spotless:off
    IntStream.range(0, actual.size()).forEach(index -> assertTableRow(domain, index, actual.get(index), expected.get(index)));
    // spotless:on
  }

  private static void assertTableRow(String domain, int index, TableRow actual, TableRow expected) {
    String reason = String.format("%s:%d", domain, index);
    assertThat(reason, actual.getId(), is(expected.getId()));
    assertThat(reason, actual.getFieldCodes(), is(expected.getFieldCodes()));
    // spotless:off
    actual.getFieldCodes().forEach(fieldCode -> assertFieldValue(domain, index, fieldCode, actual.getFieldValue(fieldCode), expected.getFieldValue(fieldCode)));
    // spotless:on
  }

  private static void assertFieldValue(
      String domain, int index, String fieldCode, FieldValue actual, FieldValue expected) {
    String reason = String.format("%s:%d:%s", domain, index, fieldCode);
    assertThat(reason, actual.getType(), is(expected.getType()));
    assertThat(reason, actual, is(expected));
  }

  public static TableRow tableRow(Long id) {
    TableRow tableRow = new TableRow(id);
    // spotless:off
    tableRow.putField("文字列（1行）", new SingleLineTextFieldValue("テストです。"));
    tableRow.putField("文字列（複数行）", new MultiLineTextFieldValue("テスト\nです。"));
    tableRow.putField("リッチエディター", new RichTextFieldValue("<a href=\"https://www.cybozu.com\">サイボウズ</a>"));
    tableRow.putField("数値", new NumberFieldValue(new BigDecimal("123")));
    tableRow.putField("計算", new CalcFieldValue(new BigDecimal("456")));
    tableRow.putField("チェックボックス", new CheckBoxFieldValue("選択肢1", "選択肢2"));
    tableRow.putField("ラジオボタン", new RadioButtonFieldValue("選択肢3"));
    tableRow.putField("複数選択", new MultiSelectFieldValue("選択肢4", "選択肢5"));
    tableRow.putField("ドロップダウン", new DropDownFieldValue("選択肢6"));
    tableRow.putField("ユーザー選択", new UserSelectFieldValue(user("guest/sato@cybozu.com", "Noboru Sato"), user("kato", "Misaki Kato")));
    tableRow.putField("組織選択", new OrganizationSelectFieldValue(organization("kaihatsu", "開発部"), organization("jinji", "人事部")));
    tableRow.putField("グループ選択", new GroupSelectFieldValue(group("project_manager", "プロジェクトマネージャー"), group("team_leader", "チームリーダー")));
    tableRow.putField("日付", new DateFieldValue(LocalDate.parse("2012-01-11")));
    tableRow.putField("時刻", new TimeFieldValue(LocalTime.parse("11:30")));
    tableRow.putField("日時", new DateTimeFieldValue(ZonedDateTime.parse("2012-01-11T11:30:00Z")));
    tableRow.putField("リンク", new LinkFieldValue("https://cybozu.co.jp/"));
    tableRow.putField("添付ファイル", new FileFieldValue(fileBody("text/plain", "201202061155587E339F9067544F1A92C743460E3D12B3297", "17to20_VerupLog (1).txt", "23175"), fileBody("application/json", "201202061155583C763E30196F419E83E91D2E4A03746C273", "17to20_VerupLog.txt", "23176")));
    // spotless:on
    return tableRow;
  }

  private static TableRow nullTableRow() {
    TableRow tableRow = new TableRow();
    // spotless:off
    tableRow.putField("文字列（1行）", new SingleLineTextFieldValue(null));
    tableRow.putField("文字列（複数行）", new MultiLineTextFieldValue(null));
    tableRow.putField("リッチエディター", new RichTextFieldValue(null));
    tableRow.putField("数値", new NumberFieldValue(null));
    tableRow.putField("計算", new CalcFieldValue((BigDecimal) null));
    tableRow.putField("チェックボックス（空）", new CheckBoxFieldValue());
    tableRow.putField("チェックボックス（null要素）", new CheckBoxFieldValue(null, null));
    tableRow.putField("ラジオボタン", new RadioButtonFieldValue(null));
    tableRow.putField("複数選択（空）", new MultiSelectFieldValue());
    tableRow.putField("複数選択（null要素）", new MultiSelectFieldValue(null, null));
    tableRow.putField("ドロップダウン", new DropDownFieldValue(null));
    tableRow.putField("ユーザー選択（空）", new UserSelectFieldValue());
    tableRow.putField("ユーザー選択（null要素）", new UserSelectFieldValue(null, null));
    tableRow.putField("ユーザー選択（null項目）", new UserSelectFieldValue(user(null, null), user(null, null)));
    tableRow.putField("組織選択（空）", new OrganizationSelectFieldValue());
    tableRow.putField("組織選択（null要素）", new OrganizationSelectFieldValue(null, null));
    tableRow.putField("組織選択（null項目）", new OrganizationSelectFieldValue(organization(null, null), organization(null, null)));
    tableRow.putField("グループ選択（空）", new GroupSelectFieldValue());
    tableRow.putField("グループ選択（null要素）", new GroupSelectFieldValue(null, null));
    tableRow.putField("グループ選択（null項目）", new GroupSelectFieldValue(group(null, null), group(null, null)));
    tableRow.putField("日付", new DateFieldValue(null));
    tableRow.putField("時刻", new TimeFieldValue(null));
    tableRow.putField("日時", new DateTimeFieldValue(null));
    tableRow.putField("リンク", new LinkFieldValue(null));
    tableRow.putField("添付ファイル（空）", new FileFieldValue());
    tableRow.putField("添付ファイル（null要素）", new FileFieldValue(null, null));
    tableRow.putField("添付ファイル（null項目）", new FileFieldValue(fileBody(null, null, null, null), fileBody(null, null, null, null)));
    // spotless:on
    return tableRow;
  }

  private static User user(String code, String name) {
    return new User(name, code);
  }

  private static Organization organization(String code, String name) {
    return new Organization(name, code);
  }

  private static Group group(String code, String name) {
    return new Group(name, code);
  }

  private static FileBody fileBody(String contentType, String fileKey, String name, String size) {
    FileBody fileBody = new FileBody();
    fileBody.setContentType(contentType);
    fileBody.setFileKey(fileKey);
    fileBody.setName(name);
    fileBody.setSize(size == null ? null : Integer.valueOf(size));
    return fileBody;
  }
}
