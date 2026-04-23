package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KintoneOutputPluginErrorFileConcatenationTest {
  private static final String TEST_OUTPUT_DIR = "build/test-output";
  private static final String TEST_OUTPUT_FILE = TEST_OUTPUT_DIR + "/concatenated_errors.jsonl";
  private Path testDir;
  private TestableKintoneOutputPlugin plugin;

  // Extend KintoneOutputPlugin to expose concatenateErrorFiles for testing
  private static class TestableKintoneOutputPlugin extends KintoneOutputPlugin {
    public void concatenateErrorFiles(String outputFile) {
      // Use reflection to call private method
      try {
        java.lang.reflect.Method method =
            KintoneOutputPlugin.class.getDeclaredMethod("concatenateErrorFiles", String.class);
        method.setAccessible(true);
        method.invoke(this, outputFile);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Before
  public void setUp() throws IOException {
    testDir = Paths.get(TEST_OUTPUT_DIR);
    if (!Files.exists(testDir)) {
      Files.createDirectories(testDir);
    }
    cleanupTestFiles();
    plugin = new TestableKintoneOutputPlugin();
  }

  @After
  public void tearDown() throws IOException {
    cleanupTestFiles();
  }

  private void cleanupTestFiles() throws IOException {
    if (Files.exists(testDir)) {
      Files.list(testDir)
          .filter(
              path -> {
                String fileName = path.getFileName().toString();
                return fileName.contains("_task") || fileName.equals("concatenated_errors.jsonl");
              })
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  // Ignore
                }
              });
    }
  }

  @Test
  public void testConcatenateErrorFiles() throws IOException {
    // Create multiple task error files
    createTaskErrorFile(
        TEST_OUTPUT_FILE + "_task000.jsonl",
        "{\"record_data\":{\"id\":\"001\"},\"error_code\":\"ERR1\",\"error_message\":\"Error 1\"}");
    createTaskErrorFile(
        TEST_OUTPUT_FILE + "_task001.jsonl",
        "{\"record_data\":{\"id\":\"002\"},\"error_code\":\"ERR2\",\"error_message\":\"Error 2\"}",
        "{\"record_data\":{\"id\":\"003\"},\"error_code\":\"ERR3\",\"error_message\":\"Error 3\"}");
    createTaskErrorFile(
        TEST_OUTPUT_FILE + "_task002.jsonl",
        "{\"record_data\":{\"id\":\"004\"},\"error_code\":\"ERR4\",\"error_message\":\"Error 4\"}");

    // Call the concatenate method
    plugin.concatenateErrorFiles(TEST_OUTPUT_FILE);

    // Verify the output file exists
    Path outputFile = Paths.get(TEST_OUTPUT_FILE);
    assertTrue("Concatenated file should exist", Files.exists(outputFile));

    // Verify the content
    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have 4 lines", lines.size(), is(4));

    // Parse and verify each line
    Gson gson = new Gson();
    List<String> ids = new ArrayList<>();
    List<String> errorCodes = new ArrayList<>();

    for (String line : lines) {
      JsonObject json = gson.fromJson(line, JsonObject.class);
      JsonObject recordData = json.getAsJsonObject("record_data");
      ids.add(recordData.get("id").getAsString());
      errorCodes.add(json.get("error_code").getAsString());
    }

    assertThat(ids, containsInAnyOrder("001", "002", "003", "004"));
    assertThat(errorCodes, containsInAnyOrder("ERR1", "ERR2", "ERR3", "ERR4"));

    // Verify task files are deleted
    assertFalse(
        "Task file 0 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task000.jsonl")));
    assertFalse(
        "Task file 1 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task001.jsonl")));
    assertFalse(
        "Task file 2 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task002.jsonl")));
  }

  @Test
  public void testConcatenateEmptyFiles() throws IOException {
    // Create empty task files
    createTaskErrorFile(TEST_OUTPUT_FILE + "_task000.jsonl");
    createTaskErrorFile(TEST_OUTPUT_FILE + "_task001.jsonl");
    createTaskErrorFile(TEST_OUTPUT_FILE + "_task002.jsonl");

    // Call the concatenate method
    plugin.concatenateErrorFiles(TEST_OUTPUT_FILE);

    // Verify the output file does not exist (deleted because it's empty)
    Path outputFile = Paths.get(TEST_OUTPUT_FILE);
    assertFalse("Empty concatenated file should be deleted", Files.exists(outputFile));

    // Verify task files are deleted
    assertFalse(
        "Task file 0 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task000.jsonl")));
    assertFalse(
        "Task file 1 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task001.jsonl")));
    assertFalse(
        "Task file 2 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task002.jsonl")));
  }

  @Test
  public void testConcatenateMixedFiles() throws IOException {
    // Create mix of empty and non-empty task files
    createTaskErrorFile(TEST_OUTPUT_FILE + "_task000.jsonl"); // Empty
    createTaskErrorFile(
        TEST_OUTPUT_FILE + "_task001.jsonl",
        "{\"record_data\":{\"id\":\"100\"},\"error_code\":\"ERR\",\"error_message\":\"Error\"}");
    createTaskErrorFile(TEST_OUTPUT_FILE + "_task002.jsonl"); // Empty

    // Call the concatenate method
    plugin.concatenateErrorFiles(TEST_OUTPUT_FILE);

    // Verify the output file exists
    Path outputFile = Paths.get(TEST_OUTPUT_FILE);
    assertTrue("Concatenated file should exist", Files.exists(outputFile));

    // Verify the content
    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have 1 line", lines.size(), is(1));

    Gson gson = new Gson();
    JsonObject json = gson.fromJson(lines.get(0), JsonObject.class);
    JsonObject recordData = json.getAsJsonObject("record_data");
    assertThat(recordData.get("id").getAsString(), is("100"));
    assertThat(json.get("error_code").getAsString(), is("ERR"));

    // Verify task files are deleted
    assertFalse(
        "Task file 0 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task000.jsonl")));
    assertFalse(
        "Task file 1 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task001.jsonl")));
    assertFalse(
        "Task file 2 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task002.jsonl")));
  }

  @Test
  public void testConcatenateNoTaskFiles() throws IOException {
    // Call the concatenate method without creating any task files
    plugin.concatenateErrorFiles(TEST_OUTPUT_FILE);

    // Verify no output file is created
    Path outputFile = Paths.get(TEST_OUTPUT_FILE);
    assertFalse(
        "No output file should be created when no task files exist", Files.exists(outputFile));
  }

  @Test
  public void testConcatenateMissingTaskFiles() throws IOException {
    // Create only some task files (not all)
    createTaskErrorFile(
        TEST_OUTPUT_FILE + "_task000.jsonl",
        "{\"record_data\":{\"id\":\"001\"},\"error_code\":\"ERR1\",\"error_message\":\"Error 1\"}");
    createTaskErrorFile(
        TEST_OUTPUT_FILE + "_task002.jsonl",
        "{\"record_data\":{\"id\":\"002\"},\"error_code\":\"ERR2\",\"error_message\":\"Error 2\"}");
    // _task001.jsonl is missing

    // Call the concatenate method
    plugin.concatenateErrorFiles(TEST_OUTPUT_FILE);

    // Verify the output file exists
    Path outputFile = Paths.get(TEST_OUTPUT_FILE);
    assertTrue("Concatenated file should exist", Files.exists(outputFile));

    // Verify the content (should contain data from existing files)
    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have 2 lines", lines.size(), is(2));

    // Verify existing task files are deleted
    assertFalse(
        "Task file 0 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task000.jsonl")));
    assertFalse(
        "Task file 2 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task002.jsonl")));
  }

  @Test
  public void testConcatenateWithLargeContent() throws IOException {
    // Create task files with multiple lines
    List<String> largeContent = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      largeContent.add(
          String.format(
              "{\"record_data\":{\"id\":\"%03d\"},\"error_code\":\"ERR\",\"error_message\":\"Error %d\"}",
              i, i));
    }

    createTaskErrorFile(TEST_OUTPUT_FILE + "_task000.jsonl", largeContent.toArray(new String[0]));
    createTaskErrorFile(TEST_OUTPUT_FILE + "_task001.jsonl", largeContent.toArray(new String[0]));

    // Call the concatenate method
    plugin.concatenateErrorFiles(TEST_OUTPUT_FILE);

    // Verify the output file exists
    Path outputFile = Paths.get(TEST_OUTPUT_FILE);
    assertTrue("Concatenated file should exist", Files.exists(outputFile));

    // Verify the content
    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have 200 lines", lines.size(), is(200));

    // Verify task files are deleted
    assertFalse(
        "Task file 0 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task000.jsonl")));
    assertFalse(
        "Task file 1 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task001.jsonl")));
  }

  @Test
  public void testConcatenateWithSpecialCharacters() throws IOException {
    // Create task files with special characters
    createTaskErrorFile(
        TEST_OUTPUT_FILE + "_task000.jsonl",
        "{\"record_data\":{\"text\":\"Line with\\nnewline\"},\"error_code\":\"ERR\",\"error_message\":\"Error with \\\"quotes\\\"\"}");
    createTaskErrorFile(
        TEST_OUTPUT_FILE + "_task001.jsonl",
        "{\"record_data\":{\"text\":\"Tab\\there\"},\"error_code\":\"ERR\",\"error_message\":\"Backslash \\\\ test\"}");

    // Call the concatenate method
    plugin.concatenateErrorFiles(TEST_OUTPUT_FILE);

    // Verify the output file exists
    Path outputFile = Paths.get(TEST_OUTPUT_FILE);
    assertTrue("Concatenated file should exist", Files.exists(outputFile));

    // Verify the content
    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have 2 lines", lines.size(), is(2));

    // Parse and verify special characters are preserved
    Gson gson = new Gson();
    JsonObject json1 = gson.fromJson(lines.get(0), JsonObject.class);
    JsonObject recordData1 = json1.getAsJsonObject("record_data");
    assertThat(recordData1.get("text").getAsString(), is("Line with\nnewline"));
    assertThat(json1.get("error_message").getAsString(), is("Error with \"quotes\""));

    JsonObject json2 = gson.fromJson(lines.get(1), JsonObject.class);
    JsonObject recordData2 = json2.getAsJsonObject("record_data");
    assertThat(recordData2.get("text").getAsString(), is("Tab\there"));
    assertThat(json2.get("error_message").getAsString(), is("Backslash \\ test"));

    // Verify task files are deleted
    assertFalse(
        "Task file 0 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task000.jsonl")));
    assertFalse(
        "Task file 1 should be deleted",
        Files.exists(Paths.get(TEST_OUTPUT_FILE + "_task001.jsonl")));
  }

  private void createTaskErrorFile(String filename, String... lines) throws IOException {
    Path filePath = Paths.get(filename);
    try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8)) {
      for (String line : lines) {
        writer.write(line);
        writer.newLine();
      }
    }
  }
}
