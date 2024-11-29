package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.kintone.util.Lazy;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Test;

public class KintoneClientTest extends TestKintoneOutputPlugin {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();
  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  private ConfigSource config;

  @Before
  public void before() {
    config = loadConfigYaml("client/config.yml");
  }

  @Test
  public void testInsert() {
    merge(config("mode: insert"));
    merge(config("update_key: null"));
    runWithMockClient(Lazy::get);
    merge(config("update_key: long_number"));
    assertConfigException("When mode is insert, require no update_key.");
    merge(config("update_key: string_single_line_text"));
    assertConfigException("When mode is insert, require no update_key.");
    merge(config("update_key: $id"));
    assertConfigException("When mode is insert, require no update_key.", id(Types.LONG));
    merge(config("update_key: null"));
    runWithMockClient(Lazy::get, id(Types.STRING));
  }

  @Test
  public void testUpdate() {
    merge(config("mode: update"));
    merge(config("update_key: null"));
    assertConfigException("When mode is update, require update_key or id column.");
    merge(config("update_key: non_existing_column"));
    assertConfigException("The column 'non_existing_column' for update does not exist.");
    merge(config("update_key: non_existing_field"));
    assertConfigException("The field 'non_existing_field' for update does not exist.");
    merge(config("update_key: invalid_type_field_multi_line_text"));
    assertConfigException("The update_key must be 'SINGLE_LINE_TEXT' or 'NUMBER'.");
    merge(config("update_key: long_number"));
    runWithMockClient(Lazy::get);
    merge(config("update_key: string_single_line_text"));
    runWithMockClient(Lazy::get);
    merge(config("update_key: $id"));
    runWithMockClient(Lazy::get, id(Types.LONG));
    merge(config("update_key: null"));
    assertConfigException("The id column must be 'long'.", id(Types.STRING));
  }

  @Test
  public void testUpsert() {
    merge(config("mode: upsert"));
    merge(config("update_key: null"));
    assertConfigException("When mode is upsert, require update_key or id column.");
    merge(config("update_key: non_existing_column"));
    assertConfigException("The column 'non_existing_column' for update does not exist.");
    merge(config("update_key: non_existing_field"));
    assertConfigException("The field 'non_existing_field' for update does not exist.");
    merge(config("update_key: invalid_type_field_multi_line_text"));
    assertConfigException("The update_key must be 'SINGLE_LINE_TEXT' or 'NUMBER'.");
    merge(config("update_key: long_number"));
    runWithMockClient(Lazy::get);
    merge(config("update_key: string_single_line_text"));
    runWithMockClient(Lazy::get);
    merge(config("update_key: $id"));
    runWithMockClient(Lazy::get, id(Types.LONG));
    merge(config("update_key: null"));
    assertConfigException("The id column must be 'long'.", id(Types.STRING));
  }

  private void assertConfigException(String message) {
    assertConfigException(message, builder());
  }

  private void assertConfigException(String message, Schema.Builder builder) {
    runWithMockClient(
        client ->
            assertThat(assertThrows(ConfigException.class, client::get).getMessage(), is(message)),
        builder);
  }

  private void runWithMockClient(Consumer<Lazy<KintoneClient>> consumer) {
    runWithMockClient(consumer, builder());
  }

  private void runWithMockClient(Consumer<Lazy<KintoneClient>> consumer, Schema.Builder builder) {
    MockClient mockClient =
        new MockClient(
            config.get(String.class, "domain"),
            Collections.emptyList(),
            Collections.emptyList(),
            "");
    try (Lazy<KintoneClient> client = KintoneClient.lazy(this::task, schema(builder))) {
      mockClient.run(() -> consumer.accept(client));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void merge(ConfigSource config) {
    this.config.merge(config);
  }

  private PluginTask task() {
    return CONFIG_MAPPER.map(config, PluginTask.class);
  }

  private static Schema schema(Schema.Builder builder) {
    return builder
        .add("non_existing_field", Types.LONG)
        .add("invalid_type_field_multi_line_text", Types.STRING)
        .add("long_number", Types.LONG)
        .add("string_single_line_text", Types.STRING)
        .build();
  }

  private static Schema.Builder id(Type type) {
    return builder().add("$id", type);
  }

  private static Schema.Builder builder() {
    return Schema.builder();
  }

  private interface Consumer<T> {
    void accept(T t) throws Exception;
  }
}
