package org.embulk.output.kintone.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.TextNode;
import com.kintone.client.model.FileBody;
import com.kintone.client.model.Group;
import com.kintone.client.model.Organization;
import com.kintone.client.model.User;
import com.kintone.client.model.record.CalcFieldValue;
import com.kintone.client.model.record.CheckBoxFieldValue;
import com.kintone.client.model.record.DateFieldValue;
import com.kintone.client.model.record.DateTimeFieldValue;
import com.kintone.client.model.record.DropDownFieldValue;
import com.kintone.client.model.record.FieldType;
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
import com.kintone.client.model.record.TableRow;
import com.kintone.client.model.record.TimeFieldValue;
import com.kintone.client.model.record.UserSelectFieldValue;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Deserializer {
  private static final JsonNode NULL = new TextNode(null);
  private final ObjectMapper mapper = new ObjectMapper();

  public Deserializer() {
    SimpleModule module = new SimpleModule();
    // spotless:off
    addDeserializer(module, SubtableFieldValue.class, this::deserializeSubtable);
    addDeserializer(module, TableRow.class, this::deserializeTableRow);
    addDeserializer(module, SingleLineTextFieldValue.class, this::deserializeSingleLineText);
    addDeserializer(module, MultiLineTextFieldValue.class, this::deserializeMultiLineText);
    addDeserializer(module, RichTextFieldValue.class, this::deserializeRichText);
    addDeserializer(module, NumberFieldValue.class, this::deserializeNumber);
    addDeserializer(module, CalcFieldValue.class, this::deserializeCalc);
    addDeserializer(module, CheckBoxFieldValue.class, this::deserializeCheckBox);
    addDeserializer(module, RadioButtonFieldValue.class, this::deserializeRadioButton);
    addDeserializer(module, MultiSelectFieldValue.class, this::deserializeMultiSelect);
    addDeserializer(module, DropDownFieldValue.class, this::deserializeDropDown);
    addDeserializer(module, UserSelectFieldValue.class, this::deserializeUserSelect);
    addDeserializer(module, User.class, this::deserializeUser);
    addDeserializer(module, OrganizationSelectFieldValue.class, this::deserializeOrganizationSelect);
    addDeserializer(module, Organization.class, this::deserializeOrganization);
    addDeserializer(module, GroupSelectFieldValue.class, this::deserializeGroupSelect);
    addDeserializer(module, Group.class, this::deserializeGroup);
    addDeserializer(module, DateFieldValue.class, this::deserializeDate);
    addDeserializer(module, TimeFieldValue.class, this::deserializeTime);
    addDeserializer(module, DateTimeFieldValue.class, this::deserializeDateTime);
    addDeserializer(module, LinkFieldValue.class, this::deserializeLink);
    addDeserializer(module, FileFieldValue.class, this::deserializeFile);
    addDeserializer(module, FileBody.class, this::deserializeFileBody);
    // spotless:on
    mapper.registerModule(module);
  }

  public <T> T deserialize(String content, Class<T> type) {
    try {
      return mapper.readValue(content, type);
    } catch (IOException e) {
      throw new DeserializeException(e);
    }
  }

  private <T> void addDeserializer(
      SimpleModule module,
      Class<T> type,
      BiFunction<JsonParser, DeserializationContext, T> deserializer) {
    module.addDeserializer(type, new DeserializeApplier<>(deserializer));
  }

  private SubtableFieldValue deserializeSubtable(
      JsonParser parser, DeserializationContext context) {
    return new SubtableFieldValue(readList(parser, TableRow.class));
  }

  private TableRow deserializeTableRow(JsonParser parser, DeserializationContext context) {
    ObjectCodec codec = parser.getCodec();
    JsonNode node = readTree(codec, parser);
    TableRow row = new TableRow(get(node, "id", JsonNode::asLong));
    stream(node.get("value").fields())
        .forEach(entry -> row.putField(entry.getKey(), readValue(entry.getValue(), codec)));
    return row;
  }

  private SingleLineTextFieldValue deserializeSingleLineText(
      JsonParser parser, DeserializationContext context) {
    return new SingleLineTextFieldValue(readText(parser));
  }

  private MultiLineTextFieldValue deserializeMultiLineText(
      JsonParser parser, DeserializationContext context) {
    return new MultiLineTextFieldValue(readText(parser));
  }

  private RichTextFieldValue deserializeRichText(
      JsonParser parser, DeserializationContext context) {
    return new RichTextFieldValue(readText(parser));
  }

  private NumberFieldValue deserializeNumber(JsonParser parser, DeserializationContext context) {
    return new NumberFieldValue(read(parser, BigDecimal::new));
  }

  private CalcFieldValue deserializeCalc(JsonParser parser, DeserializationContext context) {
    return new CalcFieldValue((BigDecimal) read(parser, BigDecimal::new));
  }

  private CheckBoxFieldValue deserializeCheckBox(
      JsonParser parser, DeserializationContext context) {
    return new CheckBoxFieldValue(readList(parser, String.class));
  }

  private RadioButtonFieldValue deserializeRadioButton(
      JsonParser parser, DeserializationContext context) {
    return new RadioButtonFieldValue(readText(parser));
  }

  private MultiSelectFieldValue deserializeMultiSelect(
      JsonParser parser, DeserializationContext context) {
    return new MultiSelectFieldValue(readList(parser, String.class));
  }

  private DropDownFieldValue deserializeDropDown(
      JsonParser parser, DeserializationContext context) {
    return new DropDownFieldValue(readText(parser));
  }

  private UserSelectFieldValue deserializeUserSelect(
      JsonParser parser, DeserializationContext context) {
    return new UserSelectFieldValue(readList(parser, User.class));
  }

  private User deserializeUser(JsonParser parser, DeserializationContext context) {
    JsonNode node = readTree(parser);
    return new User(get(node, "name", JsonNode::asText), get(node, "code", JsonNode::asText));
  }

  private OrganizationSelectFieldValue deserializeOrganizationSelect(
      JsonParser parser, DeserializationContext context) {
    return new OrganizationSelectFieldValue(readList(parser, Organization.class));
  }

  private Organization deserializeOrganization(JsonParser parser, DeserializationContext context) {
    JsonNode node = readTree(parser);
    return new Organization(
        get(node, "name", JsonNode::asText), get(node, "code", JsonNode::asText));
  }

  private GroupSelectFieldValue deserializeGroupSelect(
      JsonParser parser, DeserializationContext context) {
    return new GroupSelectFieldValue(readList(parser, Group.class));
  }

  private Group deserializeGroup(JsonParser parser, DeserializationContext context) {
    JsonNode node = readTree(parser);
    return new Group(get(node, "name", JsonNode::asText), get(node, "code", JsonNode::asText));
  }

  private DateFieldValue deserializeDate(JsonParser parser, DeserializationContext context) {
    return new DateFieldValue(read(parser, LocalDate::parse));
  }

  private TimeFieldValue deserializeTime(JsonParser parser, DeserializationContext context) {
    return new TimeFieldValue(read(parser, LocalTime::parse));
  }

  private DateTimeFieldValue deserializeDateTime(
      JsonParser parser, DeserializationContext context) {
    return new DateTimeFieldValue(read(parser, ZonedDateTime::parse));
  }

  private LinkFieldValue deserializeLink(JsonParser parser, DeserializationContext context) {
    return new LinkFieldValue(readText(parser));
  }

  private FileFieldValue deserializeFile(JsonParser parser, DeserializationContext context) {
    return new FileFieldValue(readList(parser, FileBody.class));
  }

  private FileBody deserializeFileBody(JsonParser parser, DeserializationContext context) {
    JsonNode node = readTree(parser);
    return new FileBody()
        .setContentType(get(node, "contentType", JsonNode::asText))
        .setFileKey(get(node, "fileKey", JsonNode::asText))
        .setName(get(node, "name", JsonNode::asText))
        .setSize(get(node, "size", JsonNode::asInt));
  }

  private static <T> T get(JsonNode node, String name, Function<JsonNode, T> as) {
    return node.has(name) ? as.apply(node.get(name)) : null;
  }

  private static FieldValue readValue(JsonNode node, ObjectCodec codec) {
    return node == null
        ? null
        : readValueAs(
            node.has("value") ? node.get("value") : NULL,
            codec,
            FieldType.valueOf(node.get("type").asText()).getFieldValueClass());
  }

  private static <T> List<T> readList(JsonParser parser, Class<T> type) {
    ObjectCodec codec = parser.getCodec();
    return stream(readTree(codec, parser).elements())
        .map(node -> readValueAs(node, codec, type))
        .collect(Collectors.toList());
  }

  private static <T> Stream<T> stream(Iterator<T> iterator) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
  }

  private static <T> T read(JsonParser parser, Function<String, T> as) {
    String text = readText(parser);
    return text == null || text.isEmpty() ? null : as.apply(text);
  }

  private static String readText(JsonParser parser) {
    JsonNode node = readTree(parser);
    return node == null || node.isNull() ? null : node.asText();
  }

  private static JsonNode readTree(JsonParser parser) {
    return readTree(parser.getCodec(), parser);
  }

  private static JsonNode readTree(ObjectCodec codec, JsonParser parser) {
    try {
      return codec.readTree(parser);
    } catch (IOException e) {
      throw new DeserializeException(e);
    }
  }

  private static <T> T readValueAs(JsonNode node, ObjectCodec codec, Class<T> type) {
    try (JsonParser parser = node.traverse(codec)) {
      return parser.readValueAs(type);
    } catch (IOException e) {
      throw new DeserializeException(e);
    }
  }
}
