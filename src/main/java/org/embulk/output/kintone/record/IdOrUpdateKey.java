package org.embulk.output.kintone.record;

import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import com.kintone.client.model.record.UpdateKey;

public class IdOrUpdateKey {
  private final Id id = new Id();
  private final UpdateKey updateKey = new UpdateKey();

  public Id getId() {
    return id;
  }

  public UpdateKey getUpdateKey() {
    return updateKey;
  }

  public String getField() {
    return isIdPresent() ? Id.FIELD : updateKey.getField();
  }

  public Object getValue() {
    return isIdPresent() ? id.getValue() : updateKey.getValue();
  }

  public RecordForUpdate forUpdate(Record record) {
    return isIdPresent()
        ? new RecordForUpdate(id.getValue(), record)
        : new RecordForUpdate(updateKey, record.removeField(updateKey.getField()));
  }

  public boolean isPresent() {
    return isIdPresent() || isUpdateKeyPresent();
  }

  public boolean isIdPresent() {
    return id.isPresent();
  }

  public boolean isUpdateKeyPresent() {
    Object value = updateKey.getValue();
    return value != null && !value.toString().isEmpty();
  }

  @Override
  public String toString() {
    return String.valueOf(getValue());
  }
}
