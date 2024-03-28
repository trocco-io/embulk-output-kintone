package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import net.jcip.annotations.NotThreadSafe;
import org.embulk.exec.PartialExecutionException;
import org.embulk.output.kintone.reducer.ReduceException;
import org.junit.Test;

@NotThreadSafe
public class TestTaskReduceException extends TestTask {
  @Override
  public void before() {
    super.before();
    merge(config("domain: task/reduce_exception"));
  }

  @Test
  public void test() throws Exception {
    merge(config("mode: insert"));
    merge(config("reduce_key: double_single_line_text"));
    runOutput();
    merge(config("mode: update", "update_key: double_single_line_text"));
    merge(config("reduce_key: string_number"));
    runOutput();
    merge(config("mode: upsert", "update_key: string_number"));
    merge(config("reduce_key: json.double_single_line_text"));
    assertReduceException(
        "Couldn't reduce because column json.double_single_line_text is not unique to [json, json_subtable]\n[double_single_line_text, string_number] expected [123.0, 456] but actual [456.0, 123]");
    merge(config("reduce_key: json.string_number"));
    assertReduceException(
        "Couldn't reduce because column json.string_number is not unique to [json, json_subtable]\n[double_single_line_text, string_number] expected [123.0, 456] but actual [456.0, 123]");
    merge(config("reduce_key: json_subtable.double_single_line_text"));
    assertReduceException(
        "Couldn't reduce because column json_subtable.double_single_line_text is not unique to [json, json_subtable]\n[double_single_line_text, string_number] expected [123.0, 456] but actual [456.0, 123]");
    merge(config("reduce_key: json_subtable.string_number"));
    assertReduceException(
        "Couldn't reduce because column json_subtable.string_number is not unique to [json, json_subtable]\n[double_single_line_text, string_number] expected [123.0, 456] but actual [456.0, 123]");
  }

  private void assertReduceException(String message) {
    Exception e = assertThrows(PartialExecutionException.class, this::runOutput);
    assertThat(e.getCause(), is(instanceOf(RuntimeException.class)));
    assertThat(e.getCause().getCause(), is(instanceOf(ReduceException.class)));
    assertThat(e.getCause().getCause().getMessage(), is(message));
  }
}
