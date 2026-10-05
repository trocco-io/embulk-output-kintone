package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Test;

// transaction() validates the configuration once per job before any task runs, so a job whose
// input has no records still fails on a misconfiguration (a task creates its client only when it
// receives records).
public class KintoneOutputPluginTransactionTest extends TestKintoneOutputPlugin {
  private ConfigSource config;
  private final AtomicBoolean tasksRan = new AtomicBoolean();
  private final OutputPlugin.Control control =
      taskSource -> {
        tasksRan.set(true);
        return Collections.emptyList();
      };

  @Before
  public void before() {
    config = loadConfigYaml("client/config.yml");
  }

  @Test
  public void testValidConfigurationRunsTasks() {
    config.merge(config("mode: insert"));
    transaction(config, schema(), 1, control);
    assertTrue(tasksRan.get());
  }

  @Test
  public void testInvalidUpdateKeyFailsBeforeTasksRun() {
    config.merge(config("mode: insert", "update_key: long_number"));
    ConfigException e =
        assertThrows(ConfigException.class, () -> transaction(config, schema(), 1, control));
    assertThat(e.getMessage(), is("When mode is insert, require no update_key."));
    assertFalse(tasksRan.get());
  }

  @Test
  public void testNoCertResponseFailsBeforeTasksRun() {
    config.merge(config("mode: insert", "domain: example.s.cybozu.com"));
    failTransactionFormFieldsWith(
        KintoneClientTest.htmlErrorResponse(400, KintoneClientTest.NO_CERT_HTML));
    ConfigException e =
        assertThrows(ConfigException.class, () -> transaction(config, schema(), 1, control));
    assertThat(
        e.getMessage(),
        is(
            "kintone at https://example.s.cybozu.com rejected the request with HTTP 400 \"No Cert\". This domain requires client_certificate_path and client_certificate_password."));
    assertFalse(tasksRan.get());
  }

  private static Schema schema() {
    return Schema.builder().add("long_number", Types.LONG).build();
  }
}
