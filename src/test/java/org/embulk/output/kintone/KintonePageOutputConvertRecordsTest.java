package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import com.kintone.client.model.record.SingleLineTextFieldValue;
import com.kintone.client.model.record.UpdateKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.embulk.output.kintone.record.Id;
import org.junit.Test;

/**
 * Unit tests for KintonePageOutput#convertRecordsToMaps(List).
 *
 * <p>Verifies that the identifier (update key or $id) is preserved in the map emitted for an error
 * record. The Record wrapped by RecordForUpdate has the update key field removed before the API
 * call (required by the kintone API), so this logic must restore it from the RecordForUpdate
 * itself.
 */
public class KintonePageOutputConvertRecordsTest {

  private static List<Map<String, Object>> invoke(List<?> records) {
    return KintonePageOutput.convertRecordsToMaps(records);
  }

  @Test
  public void recordForUpdateWithUpdateKey_restoresUpdateKeyField() throws Exception {
    Record record = new Record();
    record.putField("memo", new SingleLineTextFieldValue("hello"));
    UpdateKey updateKey = new UpdateKey().setField("external_id").setValue("EXT-001");

    RecordForUpdate recordForUpdate = new RecordForUpdate(updateKey, record);

    List<Map<String, Object>> result = invoke(Collections.singletonList(recordForUpdate));

    assertThat(result.size(), is(1));
    Map<String, Object> map = result.get(0);
    assertThat(map, hasEntry("memo", "hello"));
    // The update key field must be restored even though Record no longer carries it.
    assertThat(map, hasEntry("external_id", (Object) "EXT-001"));
  }

  @Test
  public void recordForUpdateWithId_restoresIdField() throws Exception {
    Record record = new Record();
    record.putField("memo", new SingleLineTextFieldValue("hello"));

    RecordForUpdate recordForUpdate = new RecordForUpdate(123L, record);

    List<Map<String, Object>> result = invoke(Collections.singletonList(recordForUpdate));

    assertThat(result.size(), is(1));
    Map<String, Object> map = result.get(0);
    assertThat(map, hasEntry("memo", "hello"));
    assertThat(map, hasEntry(Id.FIELD, (Object) 123L));
  }

  @Test
  public void plainRecord_isPassedThroughUnchanged() throws Exception {
    Record record = new Record();
    record.putField("memo", new SingleLineTextFieldValue("hello"));

    List<Map<String, Object>> result = invoke(Collections.singletonList(record));

    assertThat(result.size(), is(1));
    Map<String, Object> map = result.get(0);
    assertThat(map, hasEntry("memo", "hello"));
  }

  @Test
  public void unknownRecordType_yieldsEmptyMap() throws Exception {
    List<Map<String, Object>> result = invoke(Collections.singletonList("not a record"));

    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(not(nullValue())));
    assertThat(result.get(0).isEmpty(), is(true));
  }

  @Test
  public void mixedList_preservesIndexAndIdentifiers() throws Exception {
    Record plain = new Record();
    plain.putField("memo", new SingleLineTextFieldValue("plain"));

    Record withKey = new Record();
    withKey.putField("memo", new SingleLineTextFieldValue("keyed"));
    UpdateKey updateKey = new UpdateKey().setField("external_id").setValue("EXT-002");
    RecordForUpdate recordForUpdate = new RecordForUpdate(updateKey, withKey);

    List<Map<String, Object>> result = invoke(Arrays.asList(plain, recordForUpdate));

    assertThat(result.size(), is(2));
    assertThat(result.get(0), hasEntry("memo", "plain"));
    assertThat(result.get(1), hasEntry("memo", "keyed"));
    assertThat(result.get(1), hasEntry("external_id", (Object) "EXT-002"));
  }
}
