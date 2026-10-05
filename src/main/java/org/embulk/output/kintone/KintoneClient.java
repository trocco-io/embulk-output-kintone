package org.embulk.output.kintone;

import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.RecordClient;
import com.kintone.client.exception.KintoneApiRuntimeException;
import com.kintone.client.exception.KintoneRuntimeException;
import com.kintone.client.model.app.field.FieldProperty;
import com.kintone.client.model.app.field.SubtableFieldProperty;
import com.kintone.client.model.record.FieldType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLHandshakeException;
import org.embulk.config.ConfigException;
import org.embulk.output.kintone.record.Id;
import org.embulk.output.kintone.util.Lazy;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class KintoneClient implements AutoCloseable {
  private static final Pattern HTML_TITLE =
      Pattern.compile("<title>(.*?)</title>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private final PluginTask task;
  private final Schema schema;
  private final com.kintone.client.KintoneClient client;
  private final Map<String, FieldProperty> fields;

  public static Lazy<KintoneClient> lazy(Supplier<PluginTask> task, Schema schema) {
    return new Lazy<KintoneClient>() {
      @Override
      protected KintoneClient initialValue() {
        return new KintoneClient(task.get(), schema);
      }
    };
  }

  private KintoneClient(PluginTask task, Schema schema) {
    this.task = task;
    this.schema = schema;
    KintoneClientBuilder builder = KintoneClientBuilder.create("https://" + task.getDomain());
    if (task.getGuestSpaceId().isPresent()) {
      builder.setGuestSpaceId(task.getGuestSpaceId().get());
    }
    if (task.getBasicAuthUsername().isPresent() && task.getBasicAuthPassword().isPresent()) {
      builder.withBasicAuth(task.getBasicAuthUsername().get(), task.getBasicAuthPassword().get());
    }
    if (task.getUsername().isPresent() && task.getPassword().isPresent()) {
      builder.authByPassword(task.getUsername().get(), task.getPassword().get());
    } else if (task.getToken().isPresent()) {
      builder.authByApiToken(task.getToken().get());
    } else {
      throw new ConfigException("Username and password or token must be configured.");
    }
    configureClientCertificate(builder, task);
    com.kintone.client.KintoneClient client = builder.build();
    try {
      fields = getFormFields(client, task);
      Map<String, FieldProperty> fieldVisitor = new LinkedHashMap<>();
      fields.forEach(
          (field, fieldProperty) -> KintoneClient.addSubTableFields(fieldVisitor, fieldProperty));
      fields.putAll(fieldVisitor);
      KintoneMode.of(task).validate(task, this);
    } catch (RuntimeException e) {
      // The client is already built; do not leak it when the rest of the initialization fails.
      closeQuietly(client, e);
      throw e;
    }
    this.client = client;
  }

  private static Map<String, FieldProperty> getFormFields(
      com.kintone.client.KintoneClient client, PluginTask task) {
    try {
      return client.app().getFormFields(task.getAppId());
    } catch (KintoneRuntimeException e) {
      throw withClientCertificateHint(e, task);
    }
  }

  private static void closeQuietly(com.kintone.client.KintoneClient client, Throwable failure) {
    try {
      client.close();
    } catch (IOException | RuntimeException e) {
      failure.addSuppressed(e);
    }
  }

  private static void configureClientCertificate(KintoneClientBuilder builder, PluginTask task) {
    if (task.getClientCertificatePath().isPresent()
        != task.getClientCertificatePassword().isPresent()) {
      throw new ConfigException(
          "Client certificate and client certificate password must be provided together.");
    }
    if (!task.getClientCertificatePath().isPresent()) {
      return;
    }
    String path = task.getClientCertificatePath().get();
    Path certificate;
    try {
      certificate = Paths.get(path);
    } catch (InvalidPathException e) {
      throw new ConfigException("Invalid client certificate path: " + path, e);
    }
    if (!Files.isRegularFile(certificate) || !Files.isReadable(certificate)) {
      throw new ConfigException("Client certificate file not found or not readable: " + path);
    }
    try {
      builder.withClientCertificate(certificate, task.getClientCertificatePassword().get());
    } catch (KintoneRuntimeException e) {
      // Do not include the password in the message.
      throw new ConfigException(
          "Failed to load client certificate '"
              + path
              + "'. Make sure the file is a valid PKCS#12 (.pfx) and the password is correct.",
          e);
    }
  }

  // Explains failures that involve the client certificate as configuration problems; anything
  // else is returned as is.
  // - kintone Secure Access answers a request without a client certificate with HTTP 400 and an
  //   HTML page titled "No Cert" (the TLS handshake itself succeeds).
  // - kintone-java-client wraps I/O failures (including SSLHandshakeException) as
  //   KintoneRuntimeException("Failed to request", cause). Only a failed handshake is treated as
  //   a certificate problem; other TLS errors (for example a connection reset after the
  //   handshake) can be transient and are returned as is.
  // HTML error pages are also summarized to their <title> so that the page body is kept out of
  // the log.
  static RuntimeException withClientCertificateHint(KintoneRuntimeException e, PluginTask task) {
    if (e instanceof KintoneApiRuntimeException) {
      return describeHtmlErrorResponse((KintoneApiRuntimeException) e, task);
    }
    if (hasSslHandshakeCause(e) && task.getClientCertificatePath().isPresent()) {
      return new ConfigException(
          "TLS handshake with https://"
              + task.getDomain()
              + " failed while using client certificate '"
              + task.getClientCertificatePath().get()
              + "'. Check that the certificate was issued for this domain and is not expired or revoked,"
              + " or whether another TLS problem (for example a proxy or trust store) is the cause.",
          e);
    }
    return e;
  }

  private static RuntimeException describeHtmlErrorResponse(
      KintoneApiRuntimeException e, PluginTask task) {
    String title = htmlTitle(e.getContent());
    if (title == null) {
      return e;
    }
    // Keep the HTML body (which can embed images) out of the message and the stack trace.
    KintoneApiRuntimeException summary =
        new KintoneApiRuntimeException(
            e.getStatusCode(), e.getHeaders(), "HTML page \"" + title + "\"");
    String domain = task.getDomain();
    if (e.getStatusCode() == 400 && "No Cert".equals(title)) {
      if (task.getClientCertificatePath().isPresent()) {
        return new ConfigException(
            "kintone at https://"
                + domain
                + " rejected the request with HTTP 400 \"No Cert\" even though client_certificate_path '"
                + task.getClientCertificatePath().get()
                + "' is set. Check that the certificate was issued for this domain.",
            summary);
      }
      return new ConfigException(
          "kintone at https://"
              + domain
              + " rejected the request with HTTP 400 \"No Cert\". This domain requires client_certificate_path and client_certificate_password.",
          summary);
    }
    return new RuntimeException(
        "HTTP error status " + e.getStatusCode() + " from https://" + domain + ": " + title,
        summary);
  }

  private static String htmlTitle(String content) {
    if (content == null || !content.trim().startsWith("<")) {
      return null;
    }
    Matcher matcher = HTML_TITLE.matcher(content);
    return matcher.find() ? matcher.group(1).trim() : "";
  }

  private static boolean hasSslHandshakeCause(Throwable e) {
    for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
      if (cause instanceof SSLHandshakeException) {
        return true;
      }
    }
    return false;
  }

  private static void addSubTableFields(
      Map<String, FieldProperty> visitor, FieldProperty fieldProperty) {
    if (fieldProperty instanceof SubtableFieldProperty) {
      SubtableFieldProperty subtableFieldProperty = (SubtableFieldProperty) fieldProperty;
      Map<String, FieldProperty> subFields = subtableFieldProperty.getFields();
      visitor.putAll(subFields);
      subFields.forEach(
          (subField, subFieldProperty) ->
              KintoneClient.addSubTableFields(visitor, subFieldProperty));
    }
  }

  public void validateIdOrUpdateKey(String columnName) {
    Column column = getColumn(columnName);
    if (column == null) {
      throw new ConfigException("The column '" + columnName + "' for update does not exist.");
    }
    validateId(column);
    validateUpdateKey(column);
  }

  public Column getColumn(String columnName) {
    return schema.getColumns().stream()
        .filter(column -> column.getName().equals(columnName))
        .findFirst()
        .orElse(null);
  }

  public FieldType getFieldType(String fieldCode) {
    FieldProperty field = fields.get(fieldCode);
    return field == null ? null : field.getType();
  }

  public RecordClient record() {
    return client.record();
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException("kintone throw exception", e);
    }
  }

  private void validateId(Column column) {
    if (!column.getName().equals(Id.FIELD)) {
      return;
    }
    Type type = column.getType();
    if (!type.equals(Types.LONG)) {
      throw new ConfigException("The id column must be 'long'.");
    }
  }

  private void validateUpdateKey(Column column) {
    if (column.getName().equals(Id.FIELD)) {
      return;
    }
    String fieldCode = getFieldCode(column);
    FieldType fieldType = getFieldType(fieldCode);
    if (fieldType == null) {
      throw new ConfigException("The field '" + fieldCode + "' for update does not exist.");
    }
    if (fieldType != FieldType.SINGLE_LINE_TEXT && fieldType != FieldType.NUMBER) {
      throw new ConfigException("The update_key must be 'SINGLE_LINE_TEXT' or 'NUMBER'.");
    }
  }

  private String getFieldCode(Column column) {
    KintoneColumnOption option = task.getColumnOptions().get(column.getName());
    return option != null ? option.getFieldCode() : column.getName();
  }
}
