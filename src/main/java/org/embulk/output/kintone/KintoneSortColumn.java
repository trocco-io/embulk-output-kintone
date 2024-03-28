package org.embulk.output.kintone;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KintoneSortColumn {
  private final String name;
  private final Order order;

  @JsonCreator
  public KintoneSortColumn(@JsonProperty("name") String name, @JsonProperty("order") Order order) {
    this.name = name;
    this.order = order;
  }

  public String getName() {
    return name;
  }

  public Order getOrder() {
    return order;
  }

  public enum Order {
    ASC,
    DESC;

    @JsonCreator
    public static Order of(String name) {
      return valueOf(name.toUpperCase());
    }
  }
}
