package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import net.jcip.annotations.NotThreadSafe;
import org.embulk.exec.PartialExecutionException;
import org.junit.Test;

@NotThreadSafe
public class TestTaskSkip extends TestTask {
  @Override
  public void before() {
    super.before();
    merge(config("domain: task/skip"));
  }

  @Test
  public void testInsert() throws Exception {
    merge(config("mode: insert", "update_key: null"));
    merge(config("skip_if_non_existing_id_or_update_key: auto"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: never"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: always"));
    runOutput();
    merge(config("prefer_nulls: true"));
    merge(config("skip_if_non_existing_id_or_update_key: auto"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: never"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: always"));
    runOutput();
  }

  @Test
  public void testUpdate() throws Exception {
    merge(config("mode: update", "update_key: long_number"));
    merge(config("skip_if_non_existing_id_or_update_key: auto"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: never"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: always"));
    runOutput();
    merge(config("prefer_nulls: true"));
    merge(config("skip_if_non_existing_id_or_update_key: auto"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: never"));
    assertNoIdOrUpdateKeyValueWasSpecified();
    merge(config("skip_if_non_existing_id_or_update_key: always"));
    runOutput();
  }

  @Test
  public void testUpsert() throws Exception {
    merge(config("mode: upsert", "update_key: string_single_line_text"));
    merge(config("skip_if_non_existing_id_or_update_key: auto"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: never"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: always"));
    runOutput();
    merge(config("prefer_nulls: true"));
    merge(config("skip_if_non_existing_id_or_update_key: auto"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: never"));
    runOutput();
    merge(config("skip_if_non_existing_id_or_update_key: always"));
    runOutput();
  }

  private void assertNoIdOrUpdateKeyValueWasSpecified() {
    Exception e = assertThrows(PartialExecutionException.class, this::runOutput);
    assertThat(e.getCause(), is(instanceOf(RuntimeException.class)));
    assertThat(e.getCause().getCause(), is(instanceOf(RuntimeException.class)));
    assertThat(e.getCause().getCause().getMessage(), is("No id or update key value was specified"));
  }
}
