package org.embulk.output.kintone;

import org.embulk.config.ConfigSource;
import org.junit.Before;

public class TestTask extends TestKintoneOutputPlugin {
  private ConfigSource config;

  @Before
  public void before() {
    config = fromYamlString("{}");
  }

  @Override
  protected ConfigSource loadConfigYaml(String name) {
    ConfigSource config = super.loadConfigYaml("task/config.yml");
    return config.merge(loadYamlResource(name)).merge(this.config);
  }

  protected void merge(ConfigSource config) {
    this.config.merge(config);
  }

  protected void runOutput() throws Exception {
    String test = config.get(String.class, "domain");
    runOutput(getConfigName(test), getInputName(test));
  }

  private String getConfigName(String test) {
    return getName(test, getConfigName());
  }

  private String getInputName(String test) {
    return getName(test, getInputName());
  }

  private static String getName(String test, String name) {
    return String.format("%s/%s", test, name);
  }
}
