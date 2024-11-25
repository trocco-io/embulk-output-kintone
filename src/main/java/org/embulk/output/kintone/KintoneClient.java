package org.embulk.output.kintone;

import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.RecordClient;
import com.kintone.client.model.app.field.FieldProperty;
import com.kintone.client.model.app.field.SubtableFieldProperty;
import com.kintone.client.model.record.FieldType;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.embulk.config.ConfigException;
import org.embulk.output.kintone.record.Id;
import org.embulk.output.kintone.util.Lazy;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class KintoneClient implements AutoCloseable {
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
    client = builder.build();
    fields = client.app().getFormFields(task.getAppId());
    Map<String, FieldProperty> fieldVisitor = new LinkedHashMap<>();
    fields.forEach(
        (field, fieldProperty) -> KintoneClient.addSubTableFields(fieldVisitor, fieldProperty));
    fields.putAll(fieldVisitor);
    KintoneMode.of(task).validate(task, this);
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
