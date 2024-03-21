package org.embulk.output.kintone;

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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.json.JsonParser;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.msgpack.value.Value;

public class TestKintoneOutputPlugin extends KintoneOutputPlugin {
  private static final JsonParser PARSER = new JsonParser();

  @Rule
  public final TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "kintone", TestKintoneOutputPlugin.class)
          .build();

  @Override
  public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
    String test = taskSource.get(String.class, "Domain");
    String mode = taskSource.get(String.class, "Mode");
    String field = taskSource.get(String.class, "UpdateKeyName");
    boolean preferNulls = taskSource.get(boolean.class, "PreferNulls");
    boolean ignoreNulls = taskSource.get(boolean.class, "IgnoreNulls");
    return new KintonePageOutputVerifier(
        super.open(taskSource, schema, taskIndex),
        test,
        field,
        getValues(test, preferNulls, ignoreNulls),
        getAddRecords(test, mode, preferNulls, ignoreNulls),
        getUpdateRecords(test, mode, preferNulls, ignoreNulls, field));
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

  private static List<String> getValues(String test, boolean preferNulls, boolean ignoreNulls) {
    String name =
        String.format(
            "%s/values%s.json",
            test, ignoreNulls ? "_ignore_nulls" : preferNulls ? "_prefer_nulls" : "");
    String json = existsResource(name) ? readResource(name) : null;
    return json == null || json.isEmpty()
        ? Collections.emptyList()
        : PARSER.parse(json).asArrayValue().list().stream()
            .map(Value::toJson)
            .collect(Collectors.toList());
  }

  private static List<Record> getAddRecords(
      String test, String mode, boolean preferNulls, boolean ignoreNulls) {
    String name =
        String.format(
            "%s/%s_add%s_records.jsonl",
            test, mode, ignoreNulls ? "_ignore_nulls" : preferNulls ? "_prefer_nulls" : "");
    String jsonl = existsResource(name) ? readResource(name) : null;
    return jsonl == null || jsonl.isEmpty()
        ? Collections.emptyList()
        : Arrays.stream(jsonl.split("\\r?\\n|\\r"))
            .map(s -> Json.parse(s, Record.class))
            .collect(Collectors.toList());
  }

  private static List<RecordForUpdate> getUpdateRecords(
      String test, String mode, boolean preferNulls, boolean ignoreNulls, String field) {
    Function<Record, UpdateKey> key = getKey(field);
    String name =
        String.format(
            "%s/%s_update%s_records.jsonl",
            test, mode, ignoreNulls ? "_ignore_nulls" : preferNulls ? "_prefer_nulls" : "");
    String jsonl = existsResource(name) ? readResource(name) : null;
    return jsonl == null || jsonl.isEmpty()
        ? Collections.emptyList()
        : Arrays.stream(jsonl.split("\\r?\\n|\\r"))
            .map(s -> Json.parse(s, Record.class))
            .map(record -> new RecordForUpdate(key.apply(record), record.removeField(field)))
            .collect(Collectors.toList());
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
