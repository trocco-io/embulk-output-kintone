package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ErrorFileLoggerTest {
  private static final String TEST_OUTPUT_DIR = "build/test-output";
  private static final String TEST_OUTPUT_FILE = TEST_OUTPUT_DIR + "/error_test";
  private Path testDir;
  private ErrorFileLogger logger;

  @Before
  public void setUp() throws IOException {
    testDir = Paths.get(TEST_OUTPUT_DIR);
    if (!Files.exists(testDir)) {
      Files.createDirectories(testDir);
    }
    cleanupTestFiles();
  }

  @After
  public void tearDown() throws IOException {
    if (logger != null) {
      logger.close();
    }
    cleanupTestFiles();
  }

  private void cleanupTestFiles() throws IOException {
    if (Files.exists(testDir)) {
      Files.list(testDir)
          .filter(path -> path.getFileName().toString().startsWith("error_test"))
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
  public void testLogError() throws IOException {
    logger = new ErrorFileLogger(TEST_OUTPUT_FILE, 1);

    Map<String, Object> recordData = new HashMap<>();
    recordData.put("id", "123");
    recordData.put("name", "Test User");
    recordData.put("email", "test@example.com");

    logger.logError(recordData, "GAIA_RE01", "Field validation error: email: Invalid format");
    logger.close();

    Path outputFile = Paths.get(TEST_OUTPUT_FILE + "_task001.jsonl");
    assertTrue("Output file should exist", Files.exists(outputFile));

    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have one line", lines.size(), is(1));

    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(lines.get(0), JsonObject.class);

    assertThat(
        "Should have error_code", jsonObject.get("error_code").getAsString(), is("GAIA_RE01"));
    assertThat(
        "Should have error_message",
        jsonObject.get("error_message").getAsString(),
        is("Field validation error: email: Invalid format"));

    JsonObject recordDataJson = jsonObject.getAsJsonObject("record_data");
    assertThat("Should have id in record_data", recordDataJson.get("id").getAsString(), is("123"));
    assertThat(
        "Should have name in record_data",
        recordDataJson.get("name").getAsString(),
        is("Test User"));
    assertThat(
        "Should have email in record_data",
        recordDataJson.get("email").getAsString(),
        is("test@example.com"));
  }

  @Test
  public void testMultipleErrors() throws IOException {
    logger = new ErrorFileLogger(TEST_OUTPUT_FILE, 2);

    Map<String, Object> recordData1 = new HashMap<>();
    recordData1.put("id", "001");
    recordData1.put("field1", "value1");

    Map<String, Object> recordData2 = new HashMap<>();
    recordData2.put("id", "002");
    recordData2.put("field2", "value2");

    logger.logError(recordData1, "ERR_001", "First error message");
    logger.logError(recordData2, "ERR_002", "Second error message");
    logger.close();

    Path outputFile = Paths.get(TEST_OUTPUT_FILE + "_task002.jsonl");
    assertTrue("Output file should exist", Files.exists(outputFile));

    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have two lines", lines.size(), is(2));

    Gson gson = new Gson();
    JsonObject firstError = gson.fromJson(lines.get(0), JsonObject.class);
    JsonObject secondError = gson.fromJson(lines.get(1), JsonObject.class);

    assertThat(
        "First error should have correct code",
        firstError.get("error_code").getAsString(),
        is("ERR_001"));
    assertThat(
        "Second error should have correct code",
        secondError.get("error_code").getAsString(),
        is("ERR_002"));
  }

  @Test
  public void testNullValues() throws IOException {
    logger = new ErrorFileLogger(TEST_OUTPUT_FILE, 3);

    Map<String, Object> recordData = new HashMap<>();
    recordData.put("id", "123");
    recordData.put("nullField", null);

    logger.logError(recordData, null, null);
    logger.close();

    Path outputFile = Paths.get(TEST_OUTPUT_FILE + "_task003.jsonl");
    assertTrue("Output file should exist", Files.exists(outputFile));

    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have one line", lines.size(), is(1));

    // Check that null values are handled properly
    assertThat(
        "Should contain nullField with null value",
        lines.get(0),
        containsString("\"nullField\":null"));
    assertThat("Should have empty error_code", lines.get(0), containsString("\"error_code\":\"\""));
    assertThat(
        "Should have empty error_message", lines.get(0), containsString("\"error_message\":\"\""));
  }

  @Test
  public void testEmptyFileIsDeleted() throws IOException {
    logger = new ErrorFileLogger(TEST_OUTPUT_FILE, 4);
    logger.close();

    Path outputFile = Paths.get(TEST_OUTPUT_FILE + "_task004.jsonl");
    assertFalse("Empty file should be deleted", Files.exists(outputFile));
  }

  @Test
  public void testDisabledLogger() throws IOException {
    // Create logger with null output path
    logger = new ErrorFileLogger(null, 5);

    Map<String, Object> recordData = new HashMap<>();
    recordData.put("id", "123");

    // This should not throw any exception and should not create any file
    logger.logError(recordData, "ERR", "Error message");
    logger.close();

    Path outputFile = Paths.get(TEST_OUTPUT_FILE + "_task005.jsonl");
    assertFalse("No file should be created when logger is disabled", Files.exists(outputFile));
  }

  @Test
  public void testEmptyOutputPath() throws IOException {
    // Create logger with empty output path
    logger = new ErrorFileLogger("", 6);

    Map<String, Object> recordData = new HashMap<>();
    recordData.put("id", "123");

    // This should not throw any exception and should not create any file
    logger.logError(recordData, "ERR", "Error message");
    logger.close();

    // No assertion for file existence as no file should be created
  }

  @Test
  public void testDirectoryCreation() throws IOException {
    String nestedPath = TEST_OUTPUT_DIR + "/nested/dir/error_test";
    logger = new ErrorFileLogger(nestedPath, 7);

    Map<String, Object> recordData = new HashMap<>();
    recordData.put("id", "123");

    logger.logError(recordData, "ERR", "Error message");
    logger.close();

    Path outputFile = Paths.get(nestedPath + "_task007.jsonl");
    assertTrue("Output file should exist in nested directory", Files.exists(outputFile));

    // Cleanup nested directories
    Files.deleteIfExists(outputFile);
    Files.deleteIfExists(Paths.get(TEST_OUTPUT_DIR + "/nested/dir"));
    Files.deleteIfExists(Paths.get(TEST_OUTPUT_DIR + "/nested"));
  }

  @Test
  public void testLargeRecordData() throws IOException {
    logger = new ErrorFileLogger(TEST_OUTPUT_FILE, 8);

    Map<String, Object> recordData = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      recordData.put("field_" + i, "value_" + i);
    }

    logger.logError(recordData, "ERR", "Error with large record");
    logger.close();

    Path outputFile = Paths.get(TEST_OUTPUT_FILE + "_task008.jsonl");
    assertTrue("Output file should exist", Files.exists(outputFile));

    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have one line", lines.size(), is(1));

    // Verify all fields are present
    for (int i = 0; i < 100; i++) {
      assertThat(
          "Should contain field_" + i,
          lines.get(0),
          containsString("\"field_" + i + "\":\"value_" + i + "\""));
    }
  }

  @Test
  public void testSpecialCharactersInData() throws IOException {
    logger = new ErrorFileLogger(TEST_OUTPUT_FILE, 9);

    Map<String, Object> recordData = new HashMap<>();
    recordData.put("field1", "Value with \"quotes\"");
    recordData.put("field2", "Value with\nnewline");
    recordData.put("field3", "Value with\ttab");
    recordData.put("field4", "Value with \\backslash");

    logger.logError(
        recordData, "ERR", "Error message with special chars: \"quotes\" and\nnewlines");
    logger.close();

    Path outputFile = Paths.get(TEST_OUTPUT_FILE + "_task009.jsonl");
    assertTrue("Output file should exist", Files.exists(outputFile));

    List<String> lines = Files.readAllLines(outputFile, StandardCharsets.UTF_8);
    assertThat("Should have one line", lines.size(), is(1));

    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(lines.get(0), JsonObject.class);
    JsonObject recordDataJson = jsonObject.getAsJsonObject("record_data");

    assertThat(
        "Should handle quotes properly",
        recordDataJson.get("field1").getAsString(),
        is("Value with \"quotes\""));
    assertThat(
        "Should handle newline properly",
        recordDataJson.get("field2").getAsString(),
        is("Value with\nnewline"));
    assertThat(
        "Should handle tab properly",
        recordDataJson.get("field3").getAsString(),
        is("Value with\ttab"));
    assertThat(
        "Should handle backslash properly",
        recordDataJson.get("field4").getAsString(),
        is("Value with \\backslash"));
  }
}
