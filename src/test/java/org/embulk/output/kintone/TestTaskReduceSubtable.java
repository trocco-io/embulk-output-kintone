package org.embulk.output.kintone;

import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;

@NotThreadSafe
public class TestTaskReduceSubtable extends TestTask {
  @Override
  public void before() {
    super.before();
    merge(config("domain: task/reduce_subtable"));
  }

  @Test
  public void testInsert() throws Exception {
    merge(config("mode: insert"));
    merge(config("reduce_key: timestamp"));
    runOutput();
    merge(config("prefer_nulls: true"));
    runOutput();
    merge(config("ignore_nulls: true"));
    runOutput();
  }

  @Test
  public void testUpdate() throws Exception {
    merge(config("mode: update", "update_key: string_number"));
    merge(config("reduce_key: string_number"));
    runOutput();
    merge(config("prefer_nulls: true"));
    runOutput();
    merge(config("ignore_nulls: true"));
    runOutput();
  }

  @Test
  public void testUpsert() throws Exception {
    merge(config("mode: upsert", "update_key: double_single_line_text"));
    merge(config("reduce_key: double_single_line_text"));
    runOutput();
    merge(config("prefer_nulls: true"));
    runOutput();
    merge(config("ignore_nulls: true"));
    runOutput();
  }
}
