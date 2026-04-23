package org.embulk.output.kintone.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class Reflect {
  public static void setField(Class<?> clazz, String name, Object value) {
    try {
      Field field = clazz.getDeclaredField(name);
      field.setAccessible(true);
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      field.set(null, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
