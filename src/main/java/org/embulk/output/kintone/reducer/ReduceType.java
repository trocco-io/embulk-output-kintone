package org.embulk.output.kintone.reducer;

import com.kintone.client.model.record.FieldType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.embulk.config.ConfigException;
import org.embulk.output.kintone.KintoneClient;
import org.embulk.output.kintone.KintoneColumnOption;
import org.embulk.output.kintone.KintoneColumnType;
import org.embulk.output.kintone.KintoneSortColumn;
import org.embulk.output.kintone.util.Lazy;
import org.embulk.spi.Column;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public enum ReduceType {
  BOOLEAN {
    @Override
    public MapValue value(
        String value, KintoneColumnOption option, String columnName, Lazy<KintoneClient> client) {
      KintoneColumnType type =
          KintoneColumnType.getType(option, ReduceType.getDefaultType(client, option, columnName));
      Supplier<Value> supplier =
          () -> type.asValue(type.getFieldValue(Boolean.parseBoolean(value), option));
      return value(type, value, supplier);
    }

    @Override
    public Comparator<String> comparator(KintoneSortColumn.Order order) {
      return Comparator.comparing(Boolean::parseBoolean, order(order));
    }
  },
  LONG {
    @Override
    public MapValue value(
        String value, KintoneColumnOption option, String columnName, Lazy<KintoneClient> client) {
      KintoneColumnType type =
          KintoneColumnType.getType(option, ReduceType.getDefaultType(client, option, columnName));
      Supplier<Value> supplier =
          () -> type.asValue(type.getFieldValue(Long.parseLong(value), option));
      return value(type, value, supplier);
    }

    @Override
    public Comparator<String> comparator(KintoneSortColumn.Order order) {
      return Comparator.comparing(Long::parseLong, order(order));
    }
  },
  DOUBLE {
    @Override
    public MapValue value(
        String value, KintoneColumnOption option, String columnName, Lazy<KintoneClient> client) {
      KintoneColumnType type =
          KintoneColumnType.getType(option, ReduceType.getDefaultType(client, option, columnName));
      Supplier<Value> supplier =
          () -> type.asValue(type.getFieldValue(Double.parseDouble(value), option));
      return value(type, value, supplier);
    }

    @Override
    public Comparator<String> comparator(KintoneSortColumn.Order order) {
      return Comparator.comparing(Double::parseDouble, order(order));
    }
  },
  STRING {
    @Override
    public MapValue value(
        String value, KintoneColumnOption option, String columnName, Lazy<KintoneClient> client) {
      KintoneColumnType type =
          KintoneColumnType.getType(option, ReduceType.getDefaultType(client, option, columnName));
      Supplier<Value> supplier = () -> type.asValue(type.getFieldValue(value, option));
      return value(type, value, supplier);
    }

    @Override
    public Comparator<String> comparator(KintoneSortColumn.Order order) {
      return order(order);
    }
  },
  TIMESTAMP {
    @Override
    public MapValue value(
        String value, KintoneColumnOption option, String columnName, Lazy<KintoneClient> client) {
      KintoneColumnType type =
          KintoneColumnType.getType(option, ReduceType.getDefaultType(client, option, columnName));
      Supplier<Value> supplier =
          () -> type.asValue(type.getFieldValue(Timestamp.ofInstant(Instant.parse(value)), option));
      return value(type, value, supplier);
    }

    @Override
    public Comparator<String> comparator(KintoneSortColumn.Order order) {
      return Comparator.comparing(Instant::parse, order(order));
    }
  },
  JSON {
    @Override
    public MapValue value(
        String value, KintoneColumnOption option, String columnName, Lazy<KintoneClient> client) {
      KintoneColumnType type =
          KintoneColumnType.getType(option, ReduceType.getDefaultType(client, option, columnName));
      Supplier<Value> supplier =
          () -> type.asValue(type.getFieldValue(Reducer.PARSER.parse(value), option));
      return value(type, value, supplier);
    }

    @Override
    public Comparator<String> comparator(KintoneSortColumn.Order order) {
      return order(order);
    }
  };
  private static final Value NIL = ValueFactory.newNil();
  private static final Value ID = ValueFactory.newString("id");
  private static final Value TYPE = ValueFactory.newString("type");
  private static final Value VALUE = ValueFactory.newString("value");
  private static final Value KEY_SET = ValueFactory.newString("$$key_set");
  private static final Value SORT_VALUE = ValueFactory.newString("$$sort_value");

  public abstract MapValue value(
      String value, KintoneColumnOption option, String columnName, Lazy<KintoneClient> client);

  public abstract Comparator<String> comparator(KintoneSortColumn.Order order);

  public static Comparator<String> comparator(Column column, KintoneSortColumn.Order order) {
    return valueOf(column).comparator(order);
  }

  public static String asString(Value value, KintoneSortColumn sortColumn) {
    return asString(sortValue(value).map().get(value(sortColumn.getName())));
  }

  public static boolean isEmpty(MapValue value) {
    return value.values().stream()
        .map(Value::asMapValue)
        .map(MapValue::map)
        .map(map -> map.get(VALUE))
        .allMatch(Value::isNilValue);
  }

  public static Value value(String value) {
    return value == null ? NIL : ValueFactory.newString(value);
  }

  public static MapValue value(Value value) {
    ValueFactory.MapBuilder builder = ValueFactory.newMapBuilder();
    Map<Value, Value> map = value.asMapValue().map();
    builder.put(ID, map.get(ID));
    builder.put(VALUE, value(map.get(VALUE).asMapValue().map(), map.get(KEY_SET).asArrayValue()));
    return builder.build();
  }

  public static MapValue value(Long id, MapValue value, MapValue sortValue) {
    ValueFactory.MapBuilder builder = ValueFactory.newMapBuilder();
    builder.put(ID, id == null ? NIL : ValueFactory.newString(id.toString()));
    builder.put(VALUE, value == null ? ValueFactory.emptyMap() : value);
    builder.put(KEY_SET, value == null ? ValueFactory.emptyArray() : keySet(value));
    builder.put(SORT_VALUE, sortValue == null ? ValueFactory.emptyMap() : sortValue);
    return builder.build();
  }

  public static MapValue value(
      Column column, List<String> values, KintoneColumnOption option, Lazy<KintoneClient> client) {
    return valueOf(column)
        .value(values.get(column.getIndex()), option, fieldCode(column.getName()), client);
  }

  private static String fieldCode(String columnName) {
    Matcher m = Pattern.compile("^.*\\.(.*)$").matcher(columnName);
    return m.find() ? m.group(1) : columnName;
  }

  protected static MapValue value(KintoneColumnType type, String value, Supplier<Value> supplier) {
    ValueFactory.MapBuilder builder = ValueFactory.newMapBuilder();
    builder.put(TYPE, value(type.name()));
    builder.put(VALUE, value == null ? NIL : supplier.get());
    return builder.build();
  }

  private static <T extends Comparable<? super T>> Comparator<T> order(
      KintoneSortColumn.Order order) {
    return order == KintoneSortColumn.Order.DESC
        ? Comparator.reverseOrder()
        : Comparator.naturalOrder();
  }

  private static String asString(Value value) {
    return value.isNilValue() ? null : value.asStringValue().asString();
  }

  private static MapValue sortValue(Value value) {
    return value.asMapValue().map().get(SORT_VALUE).asMapValue();
  }

  private static MapValue value(Map<Value, Value> map, ArrayValue keySet) {
    ValueFactory.MapBuilder builder = ValueFactory.newMapBuilder();
    keySet.forEach(key -> builder.put(key, map.get(key)));
    return builder.build();
  }

  private static ArrayValue keySet(MapValue value) {
    return ValueFactory.newArray(new ArrayList<>(value.asMapValue().keySet()));
  }

  private static ReduceType valueOf(Column column) {
    return valueOf(column.getType().getName().toUpperCase());
  }

  private static KintoneColumnType getDefaultType(
      Lazy<KintoneClient> client, KintoneColumnOption option, String fieldCode) {
    if (option != null) {
      return KintoneColumnType.valueOf(option.getType());
    }

    FieldType fieldType = client.get().getFieldType(fieldCode);
    if (fieldType == null) {
      throw new ConfigException("The field '" + fieldCode + "' does not exist.");
    }

    return KintoneColumnType.valueOf(fieldType.toString());
  }
}
