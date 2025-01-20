package org.embulk.output.kintone;

import com.kintone.client.model.Group;
import com.kintone.client.model.Organization;
import com.kintone.client.model.User;
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
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RichTextFieldValue;
import com.kintone.client.model.record.SingleLineTextFieldValue;
import com.kintone.client.model.record.SubtableFieldValue;
import com.kintone.client.model.record.TimeFieldValue;
import com.kintone.client.model.record.UpdateKey;
import com.kintone.client.model.record.UserSelectFieldValue;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.embulk.output.kintone.deserializer.Deserializer;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public enum KintoneColumnType {
  SINGLE_LINE_TEXT {
    @Override
    public SingleLineTextFieldValue getFieldValue() {
      return new SingleLineTextFieldValue(null);
    }

    @Override
    public SingleLineTextFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new SingleLineTextFieldValue(value);
    }

    @Override
    public String getValue(Record record, String fieldCode) {
      return record.getSingleLineTextFieldValue(fieldCode);
    }

    @Override
    public void setUpdateKey(UpdateKey updateKey, String field) {
      updateKey.setField(field);
    }

    @Override
    public void setUpdateKey(UpdateKey updateKey, String field, FieldValue value) {
      updateKey.setField(field).setValue(((SingleLineTextFieldValue) value).getValue());
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((SingleLineTextFieldValue) value).getValue());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Arrays.asList(Types.BOOLEAN, Types.LONG, Types.DOUBLE, Types.TIMESTAMP, Types.JSON);
    }
  },
  MULTI_LINE_TEXT {
    @Override
    public MultiLineTextFieldValue getFieldValue() {
      return new MultiLineTextFieldValue(null);
    }

    @Override
    public MultiLineTextFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new MultiLineTextFieldValue(value);
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((MultiLineTextFieldValue) value).getValue());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Arrays.asList(Types.BOOLEAN, Types.LONG, Types.DOUBLE, Types.TIMESTAMP, Types.JSON);
    }
  },
  RICH_TEXT {
    @Override
    public RichTextFieldValue getFieldValue() {
      return new RichTextFieldValue(null);
    }

    @Override
    public RichTextFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new RichTextFieldValue(value);
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((RichTextFieldValue) value).getValue());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Arrays.asList(Types.BOOLEAN, Types.LONG, Types.DOUBLE, Types.TIMESTAMP, Types.JSON);
    }
  },
  NUMBER {
    @Override
    public NumberFieldValue getFieldValue() {
      return new NumberFieldValue(null);
    }

    @Override
    public NumberFieldValue getFieldValue(boolean value, KintoneColumnOption option) {
      return getFieldValue(value ? "1" : "0", option);
    }

    @Override
    public NumberFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new NumberFieldValue(new BigDecimal(value.isEmpty() ? "0" : value));
    }

    @Override
    public NumberFieldValue getFieldValue(Instant value, KintoneColumnOption option) {
      return (NumberFieldValue) getFieldValue(value.getEpochSecond(), option);
    }

    @Override
    public BigDecimal getValue(Record record, String fieldCode) {
      return record.getNumberFieldValue(fieldCode);
    }

    @Override
    public void setUpdateKey(UpdateKey updateKey, String field) {
      updateKey.setField(field);
    }

    @Override
    public void setUpdateKey(UpdateKey updateKey, String field, FieldValue value) {
      updateKey.setField(field).setValue(((NumberFieldValue) value).getValue());
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((NumberFieldValue) value).getValue().toPlainString());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Arrays.asList(Types.BOOLEAN, Types.LONG, Types.DOUBLE, Types.TIMESTAMP);
    }
  },
  CHECK_BOX {
    @Override
    public CheckBoxFieldValue getFieldValue() {
      return new CheckBoxFieldValue();
    }

    @Override
    public CheckBoxFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new CheckBoxFieldValue(asList(value, option));
    }

    @Override
    public ArrayValue asValue(FieldValue value) {
      return ValueFactory.newArray(
          ((CheckBoxFieldValue) value)
              .getValues().stream().map(ValueFactory::newString).collect(Collectors.toList()));
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.emptyList();
    }
  },
  RADIO_BUTTON {
    @Override
    public RadioButtonFieldValue getFieldValue() {
      return new RadioButtonFieldValue(null);
    }

    @Override
    public RadioButtonFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new RadioButtonFieldValue(value);
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((RadioButtonFieldValue) value).getValue());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.emptyList();
    }
  },
  MULTI_SELECT {
    @Override
    public MultiSelectFieldValue getFieldValue() {
      return new MultiSelectFieldValue();
    }

    @Override
    public MultiSelectFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new MultiSelectFieldValue(asList(value, option));
    }

    @Override
    public ArrayValue asValue(FieldValue value) {
      return ValueFactory.newArray(
          ((MultiSelectFieldValue) value)
              .getValues().stream().map(ValueFactory::newString).collect(Collectors.toList()));
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.emptyList();
    }
  },
  DROP_DOWN {
    @Override
    public DropDownFieldValue getFieldValue() {
      return new DropDownFieldValue(null);
    }

    @Override
    public DropDownFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new DropDownFieldValue(value);
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((DropDownFieldValue) value).getValue());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.emptyList();
    }
  },
  USER_SELECT {
    @Override
    public UserSelectFieldValue getFieldValue() {
      return new UserSelectFieldValue();
    }

    @Override
    public UserSelectFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new UserSelectFieldValue(
          asList(value, option).stream().map(User::new).collect(Collectors.toList()));
    }

    @Override
    public ArrayValue asValue(FieldValue value) {
      return ValueFactory.newArray(
          ((UserSelectFieldValue) value)
              .getValues().stream()
                  .map(User::getCode)
                  .map(code -> newMap("code", code))
                  .collect(Collectors.toList()));
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.emptyList();
    }
  },
  ORGANIZATION_SELECT {
    @Override
    public OrganizationSelectFieldValue getFieldValue() {
      return new OrganizationSelectFieldValue();
    }

    @Override
    public OrganizationSelectFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new OrganizationSelectFieldValue(
          asList(value, option).stream().map(Organization::new).collect(Collectors.toList()));
    }

    @Override
    public Value asValue(FieldValue value) {
      return ValueFactory.newArray(
          ((OrganizationSelectFieldValue) value)
              .getValues().stream()
                  .map(Organization::getCode)
                  .map(code -> newMap("code", code))
                  .collect(Collectors.toList()));
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.emptyList();
    }
  },
  GROUP_SELECT {
    @Override
    public GroupSelectFieldValue getFieldValue() {
      return new GroupSelectFieldValue();
    }

    @Override
    public GroupSelectFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new GroupSelectFieldValue(
          asList(value, option).stream().map(Group::new).collect(Collectors.toList()));
    }

    @Override
    public Value asValue(FieldValue value) {
      return ValueFactory.newArray(
          ((GroupSelectFieldValue) value)
              .getValues().stream()
                  .map(Group::getCode)
                  .map(code -> newMap("code", code))
                  .collect(Collectors.toList()));
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.emptyList();
    }
  },
  DATE {
    @Override
    public DateFieldValue getFieldValue() {
      return new DateFieldValue(null);
    }

    @Override
    public DateFieldValue getFieldValue(long value, KintoneColumnOption option) {
      return getFieldValue(Instant.ofEpochSecond(value), option);
    }

    @Override
    public DateFieldValue getFieldValue(double value, KintoneColumnOption option) {
      return getFieldValue(Double.valueOf(value).longValue(), option);
    }

    @Override
    public DateFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return value.isEmpty()
          ? getFieldValue(EPOCH, option)
          : new DateFieldValue(
              LocalDate.parse(value)
                  .atStartOfDay(ZoneOffset.UTC)
                  .withZoneSameInstant(getZoneId(option))
                  .toLocalDate());
    }

    @Override
    public DateFieldValue getFieldValue(Instant value, KintoneColumnOption option) {
      return new DateFieldValue(value.atZone(getZoneId(option)).toLocalDate());
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((DateFieldValue) value).getValue().toString());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Arrays.asList(Types.LONG, Types.DOUBLE, Types.TIMESTAMP);
    }
  },
  TIME {
    @Override
    public TimeFieldValue getFieldValue() {
      return new TimeFieldValue(null);
    }

    @Override
    public TimeFieldValue getFieldValue(long value, KintoneColumnOption option) {
      return getFieldValue(Instant.ofEpochSecond(value), option);
    }

    @Override
    public TimeFieldValue getFieldValue(double value, KintoneColumnOption option) {
      return getFieldValue(Double.valueOf(value).longValue(), option);
    }

    @Override
    public TimeFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return value.isEmpty()
          ? getFieldValue(EPOCH, option)
          : new TimeFieldValue(
              LocalTime.parse(value)
                  .atOffset(ZoneOffset.UTC)
                  .withOffsetSameInstant(getZoneOffset(option))
                  .toLocalTime());
    }

    @Override
    public TimeFieldValue getFieldValue(Instant value, KintoneColumnOption option) {
      return new TimeFieldValue(value.atZone(getZoneId(option)).toLocalTime());
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((TimeFieldValue) value).getValue().toString());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Arrays.asList(Types.LONG, Types.DOUBLE, Types.TIMESTAMP);
    }
  },
  DATETIME {
    @Override
    public DateTimeFieldValue getFieldValue() {
      return new DateTimeFieldValue(null);
    }

    @Override
    public DateTimeFieldValue getFieldValue(long value, KintoneColumnOption option) {
      return getFieldValue(Instant.ofEpochSecond(value), option);
    }

    @Override
    public DateTimeFieldValue getFieldValue(double value, KintoneColumnOption option) {
      return getFieldValue(Double.valueOf(value).longValue(), option);
    }

    @Override
    public DateTimeFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return value.isEmpty()
          ? getFieldValue(EPOCH, option)
          : new DateTimeFieldValue(ZonedDateTime.parse(value));
    }

    @Override
    public DateTimeFieldValue getFieldValue(Instant value, KintoneColumnOption option) {
      return new DateTimeFieldValue(value.atZone(ZoneOffset.UTC));
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((DateTimeFieldValue) value).getValue().toString());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Arrays.asList(Types.LONG, Types.DOUBLE, Types.TIMESTAMP);
    }
  },
  LINK {
    @Override
    public LinkFieldValue getFieldValue() {
      return new LinkFieldValue(null);
    }

    @Override
    public LinkFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return new LinkFieldValue(value);
    }

    @Override
    public StringValue asValue(FieldValue value) {
      return ValueFactory.newString(((LinkFieldValue) value).getValue());
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.emptyList();
    }
  },
  FILE {
    @Override
    public FileFieldValue getFieldValue() {
      return new FileFieldValue();
    }

    @Override
    public FileFieldValue getFieldValue(String value, KintoneColumnOption option) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Value asValue(FieldValue value) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.emptyList();
    }
  },
  SUBTABLE {
    @Override
    public SubtableFieldValue getFieldValue() {
      return new SubtableFieldValue();
    }

    @Override
    public SubtableFieldValue getFieldValue(String value, KintoneColumnOption option) {
      return DESERIALIZER.deserialize(value.isEmpty() ? "[]" : value, SubtableFieldValue.class);
    }

    @Override
    public Value asValue(FieldValue value) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected List<Type> getSupportedTypes() {
      return Collections.singletonList(Types.JSON);
    }
  };
  private static final Deserializer DESERIALIZER = new Deserializer();
  private static final Instant EPOCH = Instant.EPOCH;

  public static KintoneColumnType getType(
      KintoneColumnOption option, KintoneColumnType defaultType) {
    return option != null ? valueOf(option.getType()) : defaultType;
  }

  public abstract FieldValue getFieldValue();

  public FieldValue getFieldValue(boolean value, KintoneColumnOption option) {
    if (getSupportedTypes().contains(Types.BOOLEAN)) {
      return getFieldValue(String.valueOf(value), option);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public FieldValue getFieldValue(long value, KintoneColumnOption option) {
    if (getSupportedTypes().contains(Types.LONG)) {
      return getFieldValue(String.valueOf(value), option);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public FieldValue getFieldValue(double value, KintoneColumnOption option) {
    if (getSupportedTypes().contains(Types.DOUBLE)) {
      return getFieldValue(String.valueOf(value), option);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public abstract FieldValue getFieldValue(String value, KintoneColumnOption option);

  public FieldValue getFieldValue(Instant value, KintoneColumnOption option) {
    if (getSupportedTypes().contains(Types.TIMESTAMP)) {
      return getFieldValue(value.toString(), option);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public FieldValue getFieldValue(Value value, KintoneColumnOption option) {
    if (getSupportedTypes().contains(Types.JSON)) {
      return getFieldValue(value.toJson(), option);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public Object getValue(Record Record, String fieldCode) {
    throw new UnsupportedOperationException();
  }

  public void setUpdateKey(UpdateKey updateKey, String field) {
    throw new UnsupportedOperationException();
  }

  public void setUpdateKey(UpdateKey updateKey, String field, FieldValue value) {
    throw new UnsupportedOperationException();
  }

  public abstract Value asValue(FieldValue value);

  protected abstract List<Type> getSupportedTypes();

  private static MapValue newMap(String... kvs) {
    return ValueFactory.newMap(
        Arrays.stream(kvs).map(ValueFactory::newString).toArray(Value[]::new));
  }

  private static List<String> asList(String value, KintoneColumnOption option) {
    return value.isEmpty()
        ? Collections.emptyList()
        : Arrays.asList(value.split(getValueSeparator(option), 0));
  }

  private static ZoneOffset getZoneOffset(KintoneColumnOption option) {
    return getZoneId(option).getRules().getOffset(EPOCH);
  }

  private static ZoneId getZoneId(KintoneColumnOption option) {
    return ZoneId.of(option != null ? option.getTimezone() : "UTC");
  }

  private static String getValueSeparator(KintoneColumnOption option) {
    return option != null ? option.getValueSeparator() : ",";
  }
}
