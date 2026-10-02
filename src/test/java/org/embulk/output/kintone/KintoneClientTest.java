package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class KintoneClientTest extends TestKintoneOutputPlugin {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();
  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
  private static final String CLIENT_CERTIFICATE_PASSWORD = "password";

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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

  @Test
  public void testClientCertificateLackingPassword() {
    config.set("client_certificate_path", clientCertificatePath());
    assertConfigException(
        "Client certificate and client certificate password must be provided together.");
  }

  @Test
  public void testClientCertificateLackingPath() {
    config.set("client_certificate_password", CLIENT_CERTIFICATE_PASSWORD);
    assertConfigException(
        "Client certificate and client certificate password must be provided together.");
  }

  @Test
  public void testClientCertificateInvalidPath() {
    config.set("client_certificate_path", "client\u0000.pfx");
    config.set("client_certificate_password", CLIENT_CERTIFICATE_PASSWORD);
    assertConfigException("Invalid client certificate path: client\u0000.pfx");
  }

  @Test
  public void testClientCertificateFileNotFound() {
    config.set("client_certificate_path", "/nonexistent/client.pfx");
    config.set("client_certificate_password", CLIENT_CERTIFICATE_PASSWORD);
    assertConfigException(
        "Client certificate file not found or not readable: /nonexistent/client.pfx");
  }

  @Test
  public void testClientCertificate() {
    String path = clientCertificatePath();
    config.set("client_certificate_path", path);
    config.set("client_certificate_password", CLIENT_CERTIFICATE_PASSWORD);
    MockClient mockClient = runWithMockClient(Lazy::get, builder());
    verify(mockClient.getMockKintoneClientBuilder())
        .withClientCertificate(eq(Paths.get(path)), eq(CLIENT_CERTIFICATE_PASSWORD));
  }

  @Test
  public void testWithoutClientCertificate() {
    MockClient mockClient = runWithMockClient(Lazy::get, builder());
    verify(mockClient.getMockKintoneClientBuilder(), never())
        .withClientCertificate(any(Path.class), any(String.class));
  }

  @Test
  public void testClientCertificateWrongPassword() {
    String path = clientCertificatePath();
    config.set("client_certificate_path", path);
    config.set("client_certificate_password", "wrong-password");
    // Use the real KintoneClientBuilder: it loads the PKCS#12 before build(), so no network access.
    String message = assertConfigExceptionWithRealBuilder();
    assertTrue(message.startsWith("Failed to load client certificate '" + path + "'."));
    assertFalse(message.contains("wrong-password"));
  }

  @Test
  public void testClientCertificateNotPkcs12() throws IOException {
    File notPkcs12 = temporaryFolder.newFile("not-a-certificate.pfx");
    Files.write(notPkcs12.toPath(), "not a PKCS#12 file".getBytes(StandardCharsets.UTF_8));
    config.set("client_certificate_path", notPkcs12.getAbsolutePath());
    config.set("client_certificate_password", CLIENT_CERTIFICATE_PASSWORD);
    String message = assertConfigExceptionWithRealBuilder();
    assertTrue(
        message.startsWith(
            "Failed to load client certificate '" + notPkcs12.getAbsolutePath() + "'."));
  }

  // Writes a PKCS#12 keystore protected by CLIENT_CERTIFICATE_PASSWORD into a temporary folder.
  // It is generated at test time so that no key material is committed to the repository.
  private String clientCertificatePath() {
    try {
      File file = new File(temporaryFolder.getRoot(), "client.pfx");
      if (!file.exists()) {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        try (OutputStream out = new FileOutputStream(file)) {
          keyStore.store(out, CLIENT_CERTIFICATE_PASSWORD.toCharArray());
        }
      }
      return file.getAbsolutePath();
    } catch (IOException | GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private String assertConfigExceptionWithRealBuilder() {
    try (Lazy<KintoneClient> client = KintoneClient.lazy(this::task, schema(builder()))) {
      return assertThrows(ConfigException.class, client::get).getMessage();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  private MockClient runWithMockClient(
      Consumer<Lazy<KintoneClient>> consumer, Schema.Builder builder) {
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
    return mockClient;
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
