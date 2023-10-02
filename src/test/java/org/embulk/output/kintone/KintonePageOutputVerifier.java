package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kintone.client.AppClient;
import com.kintone.client.KintoneClient;
import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.RecordClient;
import com.kintone.client.api.record.GetRecordsByCursorResponseBody;
import com.kintone.client.model.app.field.FieldProperty;
import com.kintone.client.model.app.field.NumberFieldProperty;
import com.kintone.client.model.app.field.SingleLineTextFieldProperty;
import com.kintone.client.model.record.DateTimeFieldValue;
import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.FieldValue;
import com.kintone.client.model.record.NumberFieldValue;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import com.kintone.client.model.record.SingleLineTextFieldValue;
import com.kintone.client.model.record.UpdateKey;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.embulk.config.TaskReport;
import org.embulk.spi.Page;
import org.embulk.spi.TransactionalPageOutput;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class KintonePageOutputVerifier implements TransactionalPageOutput {
  private final TransactionalPageOutput transactionalPageOutput;
  private final String domain;
  private final String field;
  private final List<String> values;
  private final List<Record> addRecords;
  private final List<RecordForUpdate> updateRecords;

  public KintonePageOutputVerifier(
      TransactionalPageOutput transactionalPageOutput,
      String domain,
      String field,
      List<String> values,
      List<Record> addRecords,
      List<RecordForUpdate> updateRecords) {
    this.transactionalPageOutput = transactionalPageOutput;
    this.domain = domain;
    this.field = field;
    this.values = values;
    this.addRecords = addRecords;
    this.updateRecords = updateRecords;
  }

  @Override
  public void add(Page page) {
    runWithMock(() -> transactionalPageOutput.add(page));
  }

  @Override
  public void finish() {
    transactionalPageOutput.finish();
  }

  @Override
  public void close() {
    transactionalPageOutput.close();
  }

  @Override
  public void abort() {
    transactionalPageOutput.abort();
  }

  @Override
  public TaskReport commit() {
    return transactionalPageOutput.commit();
  }

  public void runWithMock(Runnable runnable) {
    try {
      runWithMockClient(runnable);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void runWithMockClient(Runnable runnable) throws Exception {
    @SuppressWarnings("unchecked")
    Map<String, FieldProperty> mockFormFields = mock(Map.class);
    when(mockFormFields.get(matches("^.*_single_line_text$")))
        .thenReturn(new SingleLineTextFieldProperty());
    when(mockFormFields.get(matches("^.*_number$"))).thenReturn(new NumberFieldProperty());
    AppClient mockAppClient = mock(AppClient.class);
    when(mockAppClient.getFormFields(eq(0L))).thenReturn(mockFormFields);
    GetRecordsByCursorResponseBody mockGetRecordsByCursorResponseBody =
        mock(GetRecordsByCursorResponseBody.class);
    when(mockGetRecordsByCursorResponseBody.getRecords())
        .thenReturn(updateRecords.stream().map(this::getRecord).collect(Collectors.toList()));
    when(mockGetRecordsByCursorResponseBody.hasNext()).thenReturn(false);
    RecordClient mockRecordClient = mock(RecordClient.class);
    when(mockRecordClient.createCursor(eq(0L), eq(getFields()), eq(getQuery()))).thenReturn("id");
    when(mockRecordClient.getRecordsByCursor(eq("id")))
        .thenReturn(mockGetRecordsByCursorResponseBody);
    when(mockRecordClient.addRecords(eq(0L), anyList())).thenReturn(Collections.emptyList());
    when(mockRecordClient.updateRecords(eq(0L), anyList())).thenReturn(Collections.emptyList());
    KintoneClient mockKintoneClient = mock(KintoneClient.class);
    when(mockKintoneClient.app()).thenReturn(mockAppClient);
    when(mockKintoneClient.record()).thenReturn(mockRecordClient);
    KintoneClientBuilder mockKintoneClientBuilder = mock(KintoneClientBuilder.class);
    when(mockKintoneClientBuilder.authByApiToken(eq("token"))).thenReturn(mockKintoneClientBuilder);
    when(mockKintoneClientBuilder.build()).thenReturn(mockKintoneClient);
    try (MockedStatic<KintoneClientBuilder> mocked = mockStatic(KintoneClientBuilder.class)) {
      mocked
          .when(() -> KintoneClientBuilder.create(String.format("https://%s", domain)))
          .thenReturn(mockKintoneClientBuilder);
      runnable.run();
    }
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Record>> addRecordsArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockRecordClient, atLeast(0)).addRecords(eq(0L), addRecordsArgumentCaptor.capture());
    assertRecords(
        domain,
        addRecordsArgumentCaptor.getAllValues().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList()),
        addRecords);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<RecordForUpdate>> updateRecordsArgumentCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockRecordClient, atLeast(0))
        .updateRecords(eq(0L), updateRecordsArgumentCaptor.capture());
    assertRecordForUpdates(
        domain,
        updateRecordsArgumentCaptor.getAllValues().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList()),
        updateRecords);
  }

  private Record getRecord(RecordForUpdate updateRecord) {
    UpdateKey key = updateRecord.getUpdateKey();
    String field = key.getField();
    Object value = key.getValue();
    return new Record()
        .putField(
            field,
            field.matches("^.*_number$")
                ? new NumberFieldValue((BigDecimal) value)
                : new SingleLineTextFieldValue((String) value));
  }

  private List<String> getFields() {
    return Collections.singletonList(field);
  }

  private String getQuery() {
    return String.format("%s in (%s)", field, String.join(",", values));
  }

  private static void assertRecords(String domain, List<Record> actual, List<Record> expected) {
    assertThat(domain, actual.size(), is(expected.size()));
    // spotless:off
    IntStream.range(0, actual.size()).forEach(index -> assertRecord(domain, index, actual.get(index), expected.get(index)));
    // spotless:on
  }

  private static void assertRecord(String domain, int index, Record actual, Record expected) {
    String reason = String.format("%s:%d", domain, index);
    assertThat(reason, actual.getId(), is(expected.getId()));
    assertThat(reason, actual.getRevision(), is(expected.getRevision()));
    assertThat(reason, actual.getFieldCodes(true), is(expected.getFieldCodes(true)));
    // spotless:off
    actual.getFieldCodes(true).forEach(fieldCode -> assertFieldValue(domain, index, fieldCode, actual.getFieldValue(fieldCode), expected.getFieldValue(fieldCode)));
    // spotless:on
  }

  private static void assertFieldValue(
      String domain, int index, String fieldCode, FieldValue actual, FieldValue expected) {
    String reason = String.format("%s:%d:%s", domain, index, fieldCode);
    assertThat(reason, actual.getType(), is(expected.getType()));
    // spotless:off
    assertThat(reason, actual, is(expected.getType().equals(FieldType.DATETIME) ? new DateTimeFieldValue(((DateTimeFieldValue) expected).getValue().withZoneSameInstant(ZoneId.of("UTC"))) : expected));
    // spotless:on
  }

  private static void assertRecordForUpdates(
      String domain, List<RecordForUpdate> actual, List<RecordForUpdate> expected) {
    assertThat(domain, actual.size(), is(expected.size()));
    // spotless:off
    IntStream.range(0, actual.size()).forEach(index -> assertRecordForUpdate(domain, index, actual.get(index), expected.get(index)));
    // spotless:on
  }

  private static void assertRecordForUpdate(
      String domain, int index, RecordForUpdate actual, RecordForUpdate expected) {
    String reason = String.format("%s:%d", domain, index);
    assertThat(reason, actual.getId(), is(expected.getId()));
    assertUpdateKey(domain, index, actual.getUpdateKey(), expected.getUpdateKey());
    assertRecord(domain, index, actual.getRecord(), expected.getRecord());
    assertThat(reason, actual.getRevision(), is(expected.getRevision()));
  }

  private static void assertUpdateKey(
      String domain, int index, UpdateKey actual, UpdateKey expected) {
    String reason = String.format("%s:%d", domain, index);
    assertThat(reason, actual.getField(), is(expected.getField()));
    assertThat(reason, actual.getValue(), is(expected.getValue().toString()));
  }

  public interface Runnable {
    void run() throws Exception;
  }
}
