package org.embulk.output.kintone.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.util.function.BiFunction;

public class DeserializeApplier<T> extends JsonDeserializer<T> {
  private final BiFunction<JsonParser, DeserializationContext, T> deserializer;

  public DeserializeApplier(BiFunction<JsonParser, DeserializationContext, T> deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public T deserialize(JsonParser parser, DeserializationContext context) {
    return deserializer.apply(parser, context);
  }
}
