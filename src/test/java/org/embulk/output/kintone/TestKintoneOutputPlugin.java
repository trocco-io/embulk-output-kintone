package org.embulk.output.kintone;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.io.Resources;
import com.kintone.client.Json;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import com.kintone.client.model.record.UpdateKey;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.output.kintone.record.Id;
import org.embulk.output.kintone.record.Skip;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.json.JsonParser;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.mockito.InOrder;
import org.msgpack.value.Value;

public class TestKintoneOutputPlugin extends KintoneOutputPlugin {
  private static final JsonParser PARSER = new JsonParser();

  @Rule
  public final TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "kintone", TestKintoneOutputPlugin.class)
          .build();

  @Override
  public ConfigDiff transaction(
      ConfigSource config, Schema schema, int taskCount, Control control) {
    return config.get(String.class, "reduce_key", null) == null
        ? super.transaction(config, schema, taskCount, control)
        : transactionWithVerifier(config, schema, taskCount, control);
  }

  @Override
  public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
    return taskSource.get(String.class, "ReduceKeyName") == null
        ? openWithVerifier(taskSource, schema, taskIndex)
        : super.open(taskSource, schema, taskIndex);
  }

  protected void runOutput(String configName, String inputName) throws Exception {
    System.gc();
    ConfigSource outConfig = loadConfigYaml(configName);
    Path inputPath = getResourceFile(inputName).toPath();
    embulk.runOutput(outConfig, inputPath);
  }

  protected String getConfigName() {
    return getName("config.yml");
  }

  protected String getInputName() {
    return getName("input.csv");
  }

  protected String getName(String name) {
    return name;
  }

  protected ConfigSource config(String... strings) {
    return Arrays.stream(strings)
        .map(this::fromYamlString)
        .reduce(ConfigSource::merge)
        .orElseGet(() -> fromYamlString("{}"));
  }

  protected ConfigSource loadConfigYaml(String name) {
    ConfigSource config = loadYamlResource("config.yml");
    return config.merge(loadYamlResource(name));
  }

  protected ConfigSource loadYamlResource(String name) {
    return embulk.loadYamlResource(getResourceName(name));
  }

  protected ConfigSource fromYamlString(String string) {
    return embulk.configLoader().fromYamlString(string);
  }

  private ConfigDiff transactionWithVerifier(
      ConfigSource config, Schema schema, int taskCount, Control control) {
    try (KintonePageOutputVerifier verifier = verifier(config)) {
      return runWithMock(verifier, config, schema, taskCount, control);
    }
  }

  private ConfigDiff runWithMock(
      KintonePageOutputVerifier verifier,
      ConfigSource config,
      Schema schema,
      int taskCount,
      OutputPlugin.Control control) {
    String test = config.get(String.class, "domain");
    PluginTask spyTask = spy(config.loadConfig(PluginTask.class));
    ConfigSource spyConfig = spy(config);
    when(spyConfig.loadConfig(PluginTask.class)).thenReturn(spyTask);
    AtomicReference<ConfigDiff> configDiff = new AtomicReference<>();
    verifier.runWithMock(
        () -> configDiff.set(super.transaction(spyConfig, schema, taskCount, control)));
    verify(spyConfig).loadConfig(PluginTask.class);
    InOrder inOrderTask = inOrder(spyTask);
    inOrderTask.verify(spyTask).setDerivedColumns(Collections.emptySet());
    inOrderTask.verify(spyTask).setDerivedColumns(getDerivedColumns(test));
    return configDiff.get();
  }

  private KintonePageOutputVerifier verifier(ConfigSource config) {
    String test = config.get(String.class, "domain");
    String mode = config.get(String.class, "mode");
    String field = config.get(String.class, "update_key", null);
    Skip skip = config.get(Skip.class, "skip_if_non_existing_id_or_update_key");
    boolean preferNulls = config.get(boolean.class, "prefer_nulls", false);
    boolean ignoreNulls = config.get(boolean.class, "ignore_nulls", false);
    return new KintonePageOutputVerifier(
        test,
        field,
        getValues(test, mode, skip, preferNulls, ignoreNulls, field),
        getAddValues(test, mode, skip, preferNulls, ignoreNulls, field),
        getAddRecords(test, mode, skip, preferNulls, ignoreNulls),
        getUpdateRecords(test, mode, skip, preferNulls, ignoreNulls, field));
  }

  private TransactionalPageOutput openWithVerifier(
      TaskSource taskSource, Schema schema, int taskIndex) {
    String test = taskSource.get(String.class, "Domain");
    String mode = taskSource.get(String.class, "Mode");
    String field = taskSource.get(String.class, "UpdateKeyName");
    Skip skip = taskSource.get(Skip.class, "SkipIfNonExistingIdOrUpdateKey");
    boolean preferNulls = taskSource.get(boolean.class, "PreferNulls");
    boolean ignoreNulls = taskSource.get(boolean.class, "IgnoreNulls");
    return new KintonePageOutputVerifier(
        super.open(taskSource, schema, taskIndex),
        test,
        field,
        getValues(test, mode, skip, preferNulls, ignoreNulls, field),
        getAddValues(test, mode, skip, preferNulls, ignoreNulls, field),
        getAddRecords(test, mode, skip, preferNulls, ignoreNulls),
        getUpdateRecords(test, mode, skip, preferNulls, ignoreNulls, field));
  }

  private static Set<Column> getDerivedColumns(String test) {
    String name = String.format("%s/derived_columns.json", test);
    String json = existsResource(name) ? readResource(name) : null;
    return json == null || json.isEmpty()
        ? Collections.emptySet()
        : PARSER.parse(json).asArrayValue().list().stream()
            .map(value -> Exec.getModelManager().readObject(Column.class, value.toJson()))
            .collect(Collectors.toSet());
  }

  private static List<String> getValues(
      String test, String mode, Skip skip, boolean preferNulls, boolean ignoreNulls, String field) {
    String name =
        String.format(
            "%s/%s%s%s%s_values.json",
            test,
            mode,
            format(skip),
            ignoreNulls ? "_ignore_nulls" : preferNulls ? "_prefer_nulls" : "",
            format(field));
    String json = existsResource(name) ? readResource(name) : null;
    return json == null || json.isEmpty()
        ? Collections.emptyList()
        : PARSER.parse(json).asArrayValue().list().stream()
            .map(Value::toJson)
            .collect(Collectors.toList());
  }

  private static List<String> getAddValues(
      String test, String mode, Skip skip, boolean preferNulls, boolean ignoreNulls, String field) {
    String name =
        String.format(
            "%s/%s%s%s%s_add_values.json",
            test,
            mode,
            format(skip),
            ignoreNulls ? "_ignore_nulls" : preferNulls ? "_prefer_nulls" : "",
            format(field));
    String json = existsResource(name) ? readResource(name) : null;
    return json == null || json.isEmpty()
        ? Collections.emptyList()
        : PARSER.parse(json).asArrayValue().list().stream()
            .map(Value::toJson)
            .collect(Collectors.toList());
  }

  private static List<Record> getAddRecords(
      String test, String mode, Skip skip, boolean preferNulls, boolean ignoreNulls) {
    String name =
        String.format(
            "%s/%s%s%s_add_records.jsonl",
            test,
            mode,
            format(skip),
            ignoreNulls ? "_ignore_nulls" : preferNulls ? "_prefer_nulls" : "");
    String jsonl = existsResource(name) ? readResource(name) : null;
    return jsonl == null || jsonl.isEmpty()
        ? Collections.emptyList()
        : Arrays.stream(jsonl.split("\\r?\\n|\\r"))
            .map(s -> Json.parse(s, Record.class))
            .collect(Collectors.toList());
  }

  private static List<RecordForUpdate> getUpdateRecords(
      String test, String mode, Skip skip, boolean preferNulls, boolean ignoreNulls, String field) {
    Function<Record, UpdateKey> key = getKey(field);
    Function<Record, RecordForUpdate> forUpdate =
        record ->
            Id.FIELD.equals(field)
                ? new RecordForUpdate(record.getId(), Record.newFrom(record))
                : new RecordForUpdate(key.apply(record), record.removeField(field));
    String name =
        String.format(
            "%s/%s%s%s_update_records.jsonl",
            test,
            mode,
            format(skip),
            ignoreNulls ? "_ignore_nulls" : preferNulls ? "_prefer_nulls" : "");
    String jsonl = existsResource(name) ? readResource(name) : null;
    return jsonl == null || jsonl.isEmpty()
        ? Collections.emptyList()
        : Arrays.stream(jsonl.split("\\r?\\n|\\r"))
            .map(s -> Json.parse(s, Record.class))
            .map(forUpdate)
            .collect(Collectors.toList());
  }

  private static String format(Skip skip) {
    return skip == Skip.AUTO ? "" : String.format("_%s_skip", skip.name().toLowerCase());
  }

  private static String format(String string) {
    return string == null ? "" : String.format("_%s", string.replace('$', '_'));
  }

  private static Function<Record, UpdateKey> getKey(String field) {
    return field == null
        ? record -> null
        : record ->
            field.matches("^.*_number$")
                ? new UpdateKey(field, record.getNumberFieldValue(field))
                : new UpdateKey(field, record.getSingleLineTextFieldValue(field));
  }

  private static File getResourceFile(String name) {
    return toPath(Objects.requireNonNull(getResource(getResourceName(name)))).toFile();
  }

  private static String readResource(String name) {
    return EmbulkTests.readResource(getResourceName(name));
  }

  private static boolean existsResource(String name) {
    return getResource(getResourceName(name)) != null;
  }

  private static String getResourceName(String name) {
    return String.format("org/embulk/output/kintone/%s", name);
  }

  private static Path toPath(URL url) {
    try {
      return Paths.get(url.toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  private static URL getResource(String resourceName) {
    try {
      return Resources.getResource(resourceName);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
