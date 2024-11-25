package org.embulk.output.kintone.reducer;

import com.google.code.externalsorting.csv.CsvExternalSort;
import com.google.code.externalsorting.csv.CsvSortOptions;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.output.kintone.KintoneClient;
import org.embulk.output.kintone.KintoneColumnOption;
import org.embulk.output.kintone.KintoneColumnType;
import org.embulk.output.kintone.KintoneOutputPlugin;
import org.embulk.output.kintone.KintonePageOutput;
import org.embulk.output.kintone.KintoneSortColumn;
import org.embulk.output.kintone.PluginTask;
import org.embulk.output.kintone.util.Lazy;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reducer {
  protected static final CSVFormat FORMAT =
      CSVFormat.DEFAULT.builder().setNullString("").setQuoteMode(QuoteMode.ALL_NON_NULL).build();
  protected static final JsonParser PARSER = new JsonParser();
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final PluginTask task;
  private final List<Integer> indices;
  private final int size;
  private final Schema schema;
  private final Lazy<KintoneClient> client;

  public Reducer(PluginTask task, Schema schema) {
    this.task = task;
    indices =
        schema.getColumns().stream()
            .filter(column -> !column.getName().matches("^.*\\..*$"))
            .map(Column::getIndex)
            .collect(Collectors.toList());
    size = schema.size();
    this.schema = schema(task, schema);
    this.task.setDerivedColumns(
        range().mapToObj(this.schema::getColumn).collect(Collectors.toSet()));
    this.client = KintoneClient.lazy(() -> task, schema);
  }

  public ConfigDiff reduce(List<TaskReport> taskReports, Column column) {
    File merged = file(".merged");
    merge(taskReports, merged);
    File sorted = file(".sorted");
    sort(merged, sorted, sortOptions(task, schema, column));
    AtomicInteger reduced = new AtomicInteger();
    try (CSVParser parser = parser(sorted);
        PageBuilder builder = builder(task, schema)) {
      addRecords(column, reduced, parser, builder);
    } catch (IOException e) {
      throw new ReduceException(e);
    }
    if (reduced.get() % task.getChunkSize() != 0) {
      LOGGER.info(String.format("Number of records reduced: %d", reduced.get()));
    }
    return Exec.newConfigDiff();
  }

  private void addRecords(
      Column column, AtomicInteger reduced, CSVParser parser, PageBuilder builder) {
    List<String> values = null;
    for (CSVRecord record : parser) {
      values = addRecord(column, reduced, builder, values, record);
    }
    if (values != null) {
      addRecord(column, reduced, builder, values, null);
    }
    builder.finish();
  }

  private List<String> addRecord(
      Column column,
      AtomicInteger reduced,
      PageBuilder builder,
      List<String> values,
      CSVRecord record) {
    if (values == null && record == null) {
      return null;
    }
    if (values == null) {
      return values(record);
    }
    int index = column.getIndex();
    if (record != null
        && values.get(index) != null
        && record.get(index) != null
        && values.get(index).equals(record.get(index))) {
      return values(column, values, record);
    }
    schema.visitColumns(new CSVInputColumnVisitor(builder, values(values)));
    builder.addRecord();
    reduced.getAndIncrement();
    if (reduced.get() % task.getChunkSize() == 0) {
      LOGGER.info(String.format("Number of records reduced: %d", reduced.get()));
    }
    return record == null ? null : values(record);
  }

  private List<String> values(CSVRecord record) {
    List<String> values = new ArrayList<>(record.toList());
    range().forEach(index -> values.add(value(record, index).toJson()));
    return values;
  }

  private ArrayValue value(CSVRecord record, int index) {
    ValueFactory.MapBuilder builder = ValueFactory.newMapBuilder();
    String name = schema.getColumnName(index);
    Predicate<Column> isId = column -> column.getName().equals(String.format("%s.$id", name));
    Long id =
        schema.getColumns().stream()
            .filter(isId)
            .findFirst()
            .map(column -> record.get(column.getIndex()))
            .filter(value -> !value.isEmpty())
            .map(Long::parseLong)
            .orElse(null);
    Predicate<Column> predicate =
        column -> column.getName().matches(String.format("^%s\\..*$", name));
    Function<Column, String> function =
        column -> column.getName().replaceFirst(String.format("^%s\\.", name), "");
    schema.getColumns().stream()
        .filter(isId.negate().and(predicate))
        .forEach(column -> builder.put(key(function, column), value(record, column)));
    MapValue value = builder.build();
    return id == null && ReduceType.isEmpty(value)
        ? ValueFactory.emptyArray()
        : ValueFactory.newArray(ReduceType.value(id, value, sortValue(record, index)));
  }

  private Value key(Function<Column, String> function, Column column) {
    KintoneColumnOption option = task.getColumnOptions().get(column.getName());
    return ReduceType.value(option != null ? option.getFieldCode() : function.apply(column));
  }

  private MapValue value(CSVRecord record, Column column) {
    return ReduceType.value(
        column, record.toList(), task.getColumnOptions().get(column.getName()), client);
  }

  private MapValue sortValue(CSVRecord record, int index) {
    ValueFactory.MapBuilder builder = ValueFactory.newMapBuilder();
    String name = schema.getColumnName(index);
    Function<KintoneSortColumn, Column> column = sortColumn -> lookupColumn(name, sortColumn);
    Function<KintoneSortColumn, Value> key = sortColumn -> ReduceType.value(sortColumn.getName());
    Function<KintoneSortColumn, Value> value =
        sortColumn -> ReduceType.value(record.get(column.apply(sortColumn).getIndex()));
    getSortColumns(index)
        .forEach(sortColumn -> builder.put(key.apply(sortColumn), value.apply(sortColumn)));
    return builder.build();
  }

  private List<String> values(Column column, List<String> values, CSVRecord record) {
    if (!indices.stream().allMatch(index -> Objects.equals(values.get(index), record.get(index)))) {
      throw new ReduceException(
          String.format(
              "Couldn't reduce because column %s is not unique to %s\n%s expected %s but actual %s",
              column.getName(),
              range().mapToObj(schema::getColumnName).collect(Collectors.toList()),
              indices.stream().map(schema::getColumnName).collect(Collectors.toList()),
              indices.stream().map(values::get).collect(Collectors.toList()),
              indices.stream().map(record::get).collect(Collectors.toList())));
    }
    range().forEach(index -> values.set(index, value(values, record, index).toJson()));
    return values;
  }

  private ArrayValue value(List<String> values, CSVRecord record, int index) {
    List<Value> list = new ArrayList<>(list(values, index));
    list.addAll(value(record, index).list());
    return list.isEmpty() ? ValueFactory.emptyArray() : ValueFactory.newArray(list);
  }

  private List<String> values(List<String> values) {
    range().forEach(index -> values.set(index, value(values, index).toJson()));
    return values;
  }

  private ArrayValue value(List<String> values, int index) {
    List<Value> list =
        list(values, index).stream()
            .sorted(comparator(index))
            .map(ReduceType::value)
            .collect(Collectors.toList());
    return list.isEmpty() ? ValueFactory.emptyArray() : ValueFactory.newArray(list);
  }

  private Comparator<Value> comparator(int index) {
    String name = schema.getColumnName(index);
    return getSortColumns(index).stream()
        .map(sortColumn -> comparator(name, sortColumn))
        .reduce(Comparator::thenComparing)
        .orElse(Comparator.comparing(value -> 0));
  }

  private Comparator<Value> comparator(String name, KintoneSortColumn sortColumn) {
    Column column = lookupColumn(name, sortColumn);
    return Comparator.comparing(
        value -> ReduceType.asString(value, sortColumn),
        Comparator.nullsLast(ReduceType.comparator(column, sortColumn.getOrder())));
  }

  private List<KintoneSortColumn> getSortColumns(int index) {
    KintoneColumnOption option = task.getColumnOptions().get(schema.getColumnName(index));
    return option != null ? option.getSortColumns() : Collections.emptyList();
  }

  private Column lookupColumn(String name, KintoneSortColumn sortColumn) {
    return schema.lookupColumn(String.format("%s.%s", name, sortColumn.getName()));
  }

  private IntStream range() {
    return IntStream.range(size, schema.size());
  }

  private static Schema schema(PluginTask task, Schema schema) {
    Schema.Builder builder = Schema.builder();
    schema.getColumns().forEach(column -> builder.add(column.getName(), column.getType()));
    schema.getColumns().stream()
        .map(Column::getName)
        .filter(name -> name.matches("^.*\\..*$"))
        .map(name -> name.replaceFirst("\\..*$", ""))
        .distinct()
        .forEach(name -> builder.add(name, type(task, name)));
    return builder.build();
  }

  private static Type type(PluginTask task, String name) {
    return KintoneColumnType.getType(task.getColumnOptions().get(name), KintoneColumnType.SUBTABLE)
            == KintoneColumnType.SUBTABLE
        ? Types.JSON
        : Types.STRING;
  }

  private static File file(String suffix) {
    try {
      return File.createTempFile(String.format("%s.", KintoneOutputPlugin.class.getName()), suffix);
    } catch (IOException e) {
      throw new ReduceException(e);
    }
  }

  private static void merge(List<TaskReport> taskReports, File merged) {
    try (OutputStream out = Files.newOutputStream(merged.toPath())) {
      long bytes =
          taskReports.stream()
              .map(taskReport -> new File(taskReport.get(String.class, "path")).toPath())
              .mapToLong(source -> copy(source, out))
              .sum();
      LOGGER.info(String.format("Number of bytes merged: %d", bytes));
    } catch (IOException e) {
      throw new ReduceException(e);
    }
  }

  private static long copy(Path source, OutputStream out) {
    try {
      long bytes = Files.copy(source, out);
      LOGGER.info(String.format("Number of bytes copied: %d", bytes));
      return bytes;
    } catch (IOException e) {
      throw new ReduceException(e);
    }
  }

  private static void sort(File merged, File sorted, CsvSortOptions sortOptions) {
    try {
      int lines =
          CsvExternalSort.mergeSortedFiles(
              CsvExternalSort.sortInBatch(merged, null, sortOptions, new ArrayList<>()),
              sorted,
              sortOptions,
              false,
              Collections.emptyList());
      LOGGER.info(String.format("Number of lines sorted: %d", lines));
    } catch (IOException | ClassNotFoundException e) {
      throw new ReduceException(e);
    }
  }

  private static CsvSortOptions sortOptions(PluginTask task, Schema schema, Column column) {
    List<KintoneSortColumn> sortColumns = new ArrayList<>();
    sortColumns.add(new KintoneSortColumn(column.getName(), KintoneSortColumn.Order.ASC));
    sortColumns.addAll(task.getSortColumns());
    return new CsvSortOptions.Builder(
            comparator(schema, sortColumns),
            task.getMaxSortTmpFiles().orElse(CsvExternalSort.DEFAULTMAXTEMPFILES),
            task.getMaxSortMemory().orElse(CsvExternalSort.estimateAvailableMemory()))
        .charset(StandardCharsets.UTF_8)
        .format(FORMAT)
        .build();
  }

  private static Comparator<CSVRecord> comparator(
      Schema schema, List<KintoneSortColumn> sortColumns) {
    Function<KintoneSortColumn, Comparator<CSVRecord>> function =
        sortColumn -> comparator(schema, sortColumn);
    return sortColumns.stream()
        .skip(1)
        .map(function)
        .reduce(function.apply(sortColumns.get(0)), Comparator::thenComparing);
  }

  private static Comparator<CSVRecord> comparator(Schema schema, KintoneSortColumn sortColumn) {
    Column column = schema.lookupColumn(sortColumn.getName());
    return Comparator.comparing(
        record -> record.get(column.getIndex()),
        Comparator.nullsLast(ReduceType.comparator(column, sortColumn.getOrder())));
  }

  private static CSVParser parser(File sorted) {
    try {
      return CSVParser.parse(sorted, StandardCharsets.UTF_8, FORMAT);
    } catch (IOException e) {
      throw new ReduceException(e);
    }
  }

  private static PageBuilder builder(PluginTask task, Schema schema) {
    return new PageBuilder(Exec.getBufferAllocator(), schema, new KintonePageOutput(task, schema));
  }

  private static List<Value> list(List<String> values, int index) {
    return PARSER.parse(values.get(index)).asArrayValue().list();
  }
}
