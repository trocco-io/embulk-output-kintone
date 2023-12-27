package com.kintone.client;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class Json {
  private static final JsonMapper MAPPER = new JsonMapper();

  public static String format(Object o) {
    return new String(MAPPER.format(o), StandardCharsets.UTF_8);
  }

  public static <T> T parse(String s, Class<T> clazz) {
    return MAPPER.parse(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)), clazz);
  }
}
