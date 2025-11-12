package org.embulk.output.kintone;

import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;

@NotThreadSafe
public class TestTaskMode extends TestTask {
  @Override
  public void before() {
    super.before();
    merge(config("domain: task/mode"));
  }

  @Test
  public void testInsert() throws Exception {
    merge(config("mode: insert"));
    runOutput();
    merge(config("prefer_nulls: true"));
    runOutput();
    merge(config("ignore_nulls: true"));
    runOutput();
  }

  @Test
  public void testUpdate() throws Exception {
    merge(config("mode: update", "update_key: string_number"));
    runOutput();
    merge(config("prefer_nulls: true"));
    runOutput();
    merge(config("ignore_nulls: true"));
    runOutput();
  }

  @Test
  public void testInsertOrUpdate() throws Exception {
    merge(config("mode: insert_or_update", "update_key: double_single_line_text"));
    merge(config("skip_if_non_existing_id_or_update_key: never"));
    runOutput();
    merge(config("prefer_nulls: true"));
    runOutput();
    merge(config("ignore_nulls: true"));
    runOutput();
  }
}
