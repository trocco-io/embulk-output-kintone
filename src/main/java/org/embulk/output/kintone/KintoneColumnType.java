package org.embulk.output.kintone;

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
import org.embulk.output.kintone.deserializer.Deserializer;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

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
    public void setUpdateKey(UpdateKey updateKey, String field) {
      updateKey.setField(field);
    }

    @Override
    public void setUpdateKey(UpdateKey updateKey, String field, FieldValue value) {
      updateKey.setField(field).setValue(((SingleLineTextFieldValue) value).getValue());
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
    public NumberFieldValue getFieldValue(Timestamp value, KintoneColumnOption option) {
      return (NumberFieldValue) getFieldValue(value.getEpochSecond(), option);
    }

    @Override
    public void setUpdateKey(UpdateKey updateKey, String field) {
      updateKey.setField(field);
    }

    @Override
    public void setUpdateKey(UpdateKey updateKey, String field, FieldValue value) {
      updateKey.setField(field).setValue(((NumberFieldValue) value).getValue());
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
  },
  USER_SELECT {
    @Override
    public UserSelectFieldValue getFieldValue() {
      return new UserSelectFieldValue();
    }

    @Override
    public UserSelectFieldValue getFieldValue(String value, KintoneColumnOption option) {
      throw new UnsupportedOperationException();
    }
  },
  ORGANIZATION_SELECT {
    @Override
    public OrganizationSelectFieldValue getFieldValue() {
      return new OrganizationSelectFieldValue();
    }

    @Override
    public OrganizationSelectFieldValue getFieldValue(String value, KintoneColumnOption option) {
      throw new UnsupportedOperationException();
    }
  },
  GROUP_SELECT {
    @Override
    public GroupSelectFieldValue getFieldValue() {
      return new GroupSelectFieldValue();
    }

    @Override
    public GroupSelectFieldValue getFieldValue(String value, KintoneColumnOption option) {
      throw new UnsupportedOperationException();
    }
  },
  DATE {
    @Override
    public DateFieldValue getFieldValue() {
      return new DateFieldValue(null);
    }

    @Override
    public DateFieldValue getFieldValue(long value, KintoneColumnOption option) {
      return getFieldValue(Timestamp.ofEpochSecond(value), option);
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
    public DateFieldValue getFieldValue(Timestamp value, KintoneColumnOption option) {
      return new DateFieldValue(value.getInstant().atZone(getZoneId(option)).toLocalDate());
    }
  },
  TIME {
    @Override
    public TimeFieldValue getFieldValue() {
      return new TimeFieldValue(null);
    }

    @Override
    public TimeFieldValue getFieldValue(long value, KintoneColumnOption option) {
      return getFieldValue(Timestamp.ofEpochSecond(value), option);
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
    public TimeFieldValue getFieldValue(Timestamp value, KintoneColumnOption option) {
      return new TimeFieldValue(value.getInstant().atZone(getZoneId(option)).toLocalTime());
    }
  },
  DATETIME {
    @Override
    public DateTimeFieldValue getFieldValue() {
      return new DateTimeFieldValue(null);
    }

    @Override
    public DateTimeFieldValue getFieldValue(long value, KintoneColumnOption option) {
      return getFieldValue(Timestamp.ofEpochSecond(value), option);
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
    public DateTimeFieldValue getFieldValue(Timestamp value, KintoneColumnOption option) {
      return new DateTimeFieldValue(value.getInstant().atZone(ZoneOffset.UTC));
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
  };
  private static final Deserializer DESERIALIZER = new Deserializer();
  private static final Timestamp EPOCH = Timestamp.ofInstant(Instant.EPOCH);

  public static KintoneColumnType getType(
      KintoneColumnOption option, KintoneColumnType defaultType) {
    return option != null ? valueOf(option.getType()) : defaultType;
  }

  public abstract FieldValue getFieldValue();

  public FieldValue getFieldValue(boolean value, KintoneColumnOption option) {
    return getFieldValue(String.valueOf(value), option);
  }

  public FieldValue getFieldValue(long value, KintoneColumnOption option) {
    return getFieldValue(String.valueOf(value), option);
  }

  public FieldValue getFieldValue(double value, KintoneColumnOption option) {
    return getFieldValue(String.valueOf(value), option);
  }

  public abstract FieldValue getFieldValue(String value, KintoneColumnOption option);

  public FieldValue getFieldValue(Timestamp value, KintoneColumnOption option) {
    return getFieldValue(value.getInstant().toString(), option);
  }

  public FieldValue getFieldValue(Value value, KintoneColumnOption option) {
    return getFieldValue(value.toJson(), option);
  }

  public void setUpdateKey(UpdateKey updateKey, String field) {
    throw new UnsupportedOperationException();
  }

  public void setUpdateKey(UpdateKey updateKey, String field, FieldValue value) {
    throw new UnsupportedOperationException();
  }

  private static List<String> asList(String value, KintoneColumnOption option) {
    return value.isEmpty()
        ? Collections.emptyList()
        : Arrays.asList(value.split(getValueSeparator(option), 0));
  }

  private static ZoneOffset getZoneOffset(KintoneColumnOption option) {
    return getZoneId(option).getRules().getOffset(Instant.EPOCH);
  }

  private static ZoneId getZoneId(KintoneColumnOption option) {
    return ZoneId.of(option != null ? option.getTimezone() : "UTC");
  }

  private static String getValueSeparator(KintoneColumnOption option) {
    return option != null ? option.getValueSeparator() : ",";
  }
}
