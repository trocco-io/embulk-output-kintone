package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
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
  // Reducer#reduce needs an Exec session, which these tests do not have, so in reduce mode the
  // control records that the tasks were reached and stops the transaction before the reducer runs.
  private final OutputPlugin.Control reduceControl =
      taskSource -> {
        tasksRan.set(true);
        throw new TasksRan();
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

  // In reduce mode the reducer collapses "foo.bar" columns into a derived column "foo" and writes
  // with that derived schema, so an update_key naming a derived column must pass the validation
  // even though the column is absent from the schema passed to transaction(). The test harness
  // runs reduce-mode transactions through a verifier, which wraps every exception in a
  // RuntimeException, so the assertions look at the cause.
  @Test
  public void testReduceModeAcceptsDerivedUpdateKey() {
    config.merge(reduceConfig("update_key: foo_single_line_text"));
    RuntimeException e =
        assertThrows(
            RuntimeException.class, () -> transaction(config, reduceSchema(), 1, reduceControl));
    assertThat(e.getCause(), is(instanceOf(TasksRan.class)));
    assertTrue(tasksRan.get());
  }

  @Test
  public void testReduceModeStillRejectsUnknownUpdateKeyBeforeTasksRun() {
    config.merge(reduceConfig("update_key: unknown"));
    RuntimeException e =
        assertThrows(
            RuntimeException.class, () -> transaction(config, reduceSchema(), 1, reduceControl));
    assertThat(e.getCause(), is(instanceOf(ConfigException.class)));
    assertThat(e.getCause().getMessage(), is("The column 'unknown' for update does not exist."));
    assertFalse(tasksRan.get());
  }

  // The verifier used by the harness in reduce mode reads skip_if_non_existing_id_or_update_key
  // from the raw config, so it has to be present.
  private ConfigSource reduceConfig(String updateKey) {
    return config(
        "mode: upsert",
        "reduce_key: long_number",
        "skip_if_non_existing_id_or_update_key: never",
        updateKey);
  }

  private static Schema schema() {
    return Schema.builder().add("long_number", Types.LONG).build();
  }

  private static Schema reduceSchema() {
    return Schema.builder()
        .add("long_number", Types.LONG)
        .add("foo_single_line_text.bar", Types.STRING)
        .build();
  }

  private static final class TasksRan extends RuntimeException {}
}
