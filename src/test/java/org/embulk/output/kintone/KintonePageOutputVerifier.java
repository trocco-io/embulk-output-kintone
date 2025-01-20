package org.embulk.output.kintone;

import static org.embulk.output.kintone.deserializer.DeserializerTest.assertTableRows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import com.kintone.client.RecordClient;
import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.FieldValue;
import com.kintone.client.model.record.NumberFieldValue;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import com.kintone.client.model.record.SingleLineTextFieldValue;
import com.kintone.client.model.record.SubtableFieldValue;
import com.kintone.client.model.record.UpdateKey;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.embulk.config.TaskReport;
import org.embulk.exec.PooledBufferAllocator;
import org.embulk.output.kintone.record.Id;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.TransactionalPageOutput;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.msgpack.value.StringValue;
import org.msgpack.value.ValueFactory;

public class KintonePageOutputVerifier implements TransactionalPageOutput {
  private final TransactionalPageOutput transactionalPageOutput;
  private final String domain;
  private final String field;
  private final List<String> values;
  private final List<String> addValues;
  private final List<Record> addRecords;
  private final List<RecordForUpdate> updateRecords;

  public KintonePageOutputVerifier(
      String domain,
      String field,
      List<String> values,
      List<String> addValues,
      List<Record> addRecords,
      List<RecordForUpdate> updateRecords) {
    this(null, domain, field, values, addValues, addRecords, updateRecords);
  }

  public KintonePageOutputVerifier(
      TransactionalPageOutput transactionalPageOutput,
      String domain,
      String field,
      List<String> values,
      List<String> addValues,
      List<Record> addRecords,
      List<RecordForUpdate> updateRecords) {
    this.transactionalPageOutput = transactionalPageOutput;
    this.domain = domain;
    this.field = field;
    this.values = values;
    this.addValues = addValues;
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
    if (transactionalPageOutput == null) {
      return;
    }
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

  public void runWithMock(MockClient.Runnable runnable) {
    try {
      runWithMockExec(runnable);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void runWithMockExec(MockClient.Runnable runnable) throws Exception {
    BufferAllocator bufferAllocator = PooledBufferAllocator.create(1024 * 1024);
    try (MockedStatic<Exec> mocked = mockStatic(Exec.class, CALLS_REAL_METHODS)) {
      mocked.when(Exec::getBufferAllocator).thenReturn(bufferAllocator);
      runWithMockClient(runnable);
    }
  }

  private void runWithMockClient(MockClient.Runnable runnable) throws Exception {
    assertValues(domain, getValues(), sorted(values));
    MockClient mockClient = new MockClient(domain, getRecords(), getFields(), getQuery());
    RecordClient mockRecordClient = mockClient.getMockRecordClient();
    mockClient.run(runnable);
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

  private List<String> getValues() {
    if (values.isEmpty() || updateRecords.isEmpty()) {
      return values;
    }
    Function<FieldValue, StringValue> toValue =
        value ->
            field.matches("^.*_number$")
                ? ValueFactory.newString(((NumberFieldValue) value).getValue().toPlainString())
                : ValueFactory.newString(((SingleLineTextFieldValue) value).getValue());
    Function<Record, String> toJson =
        record ->
            Id.FIELD.equals(field)
                ? ValueFactory.newString(record.getId().toString()).toJson()
                : toValue.apply(record.getFieldValue(field)).toJson();
    List<String> values = getRecords().stream().map(toJson).collect(Collectors.toList());
    values.addAll(addValues);
    return sorted(values);
  }

  private List<Record> getRecords() {
    return updateRecords.stream().map(this::getRecord).collect(Collectors.toList());
  }

  private Record getRecord(RecordForUpdate updateRecord) {
    Long id = updateRecord.getId();
    UpdateKey key = updateRecord.getUpdateKey();
    return id != null ? getRecord(id) : getRecord(key);
  }

  private Record getRecord(Long id) {
    return new Record(id, null);
  }

  private Record getRecord(UpdateKey key) {
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

  private static void assertValues(String domain, List<String> actual, List<String> expected) {
    assertThat(domain, actual.size(), is(expected.size()));
    // spotless:off
    IntStream.range(0, actual.size()).forEach(index -> assertValue(domain, index, actual.get(index), expected.get(index)));
    // spotless:on
  }

  private static void assertValue(String domain, int index, String actual, String expected) {
    String reason = String.format("%s:%d", domain, index);
    assertThat(reason, actual, is(expected));
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
    actual.getFieldCodes(true).forEach(fieldCode -> assertSubtableFieldValue(domain, index, fieldCode, actual.getFieldValue(fieldCode), expected.getFieldValue(fieldCode)));
    // spotless:on
  }

  private static void assertFieldValue(
      String domain, int index, String fieldCode, FieldValue actual, FieldValue expected) {
    if (actual.getType() == FieldType.SUBTABLE) {
      return;
    }
    String reason = String.format("%s:%d:%s", domain, index, fieldCode);
    assertThat(reason, actual.getType(), is(expected.getType()));
    assertThat(reason, actual, is(expected));
  }

  private static void assertSubtableFieldValue(
      String domain, int index, String fieldCode, FieldValue actual, FieldValue expected) {
    if (actual.getType() != FieldType.SUBTABLE) {
      return;
    }
    String reason = String.format("%s:%d:%s", domain, index, fieldCode);
    assertThat(reason, actual.getType(), is(expected.getType()));
    // spotless:off
    assertTableRows(reason, ((SubtableFieldValue) actual).getRows(), ((SubtableFieldValue) expected).getRows());
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
    assertId(domain, index, actual, expected);
    assertUpdateKey(domain, index, actual, expected);
    assertRecord(domain, index, actual.getRecord(), expected.getRecord());
    assertThat(reason, actual.getRevision(), is(expected.getRevision()));
  }

  private static void assertId(
      String domain, int index, RecordForUpdate actual, RecordForUpdate expected) {
    if (actual.getUpdateKey() != null && expected.getUpdateKey() != null) {
      return;
    }
    String reason = String.format("%s:%d", domain, index);
    assertThat(reason, actual.getId(), is(expected.getId()));
  }

  private static void assertUpdateKey(
      String domain, int index, RecordForUpdate actual, RecordForUpdate expected) {
    if (actual.getId() != null && expected.getId() != null) {
      return;
    }
    String reason = String.format("%s:%d", domain, index);
    assertThat(reason, actual.getUpdateKey().getField(), is(expected.getUpdateKey().getField()));
    assertThat(reason, actual.getUpdateKey().getValue(), is(expected.getUpdateKey().getValue()));
  }

  private static <T> List<T> sorted(List<T> list) {
    return list.stream().sorted().collect(Collectors.toList());
  }
}
