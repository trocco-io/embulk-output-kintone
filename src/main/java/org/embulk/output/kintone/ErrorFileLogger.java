package org.embulk.output.kintone;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for collecting Kintone errors, structuring them, and outputting to a file */
public class ErrorFileLogger implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ErrorFileLogger.class);
  private static final int FLUSH_INTERVAL = 100;

  private final String outputPath;
  private final int taskIndex;
  private final Gson gson;
  private boolean enabled;

  private BufferedWriter writer;
  private Path filePath;
  private int recordCount = 0;
  private int errorCount = 0;

  /** Error record class for JSON serialization */
  private static class ErrorRecord {
    @SerializedName("record_data")
    private final Map<String, Object> recordData;

    @SerializedName("error_code")
    private final String errorCode;

    @SerializedName("error_message")
    private final String errorMessage;

    ErrorRecord(Map<String, Object> recordData, String errorCode, String errorMessage) {
      this.recordData = recordData;
      this.errorCode = errorCode != null ? errorCode : "";
      this.errorMessage = errorMessage != null ? errorMessage : "";
    }
  }

  public ErrorFileLogger(String outputPath, int taskIndex) {
    this.outputPath = outputPath;
    this.taskIndex = taskIndex;
    this.gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
    this.enabled = outputPath != null && !outputPath.trim().isEmpty();

    if (enabled) {
      try {
        open();
      } catch (IOException e) {
        logger.error("Failed to open error file for writing", e);
        this.enabled = false;
      }
    }
  }

  private void open() throws IOException {
    if (!enabled || writer != null) {
      return;
    }

    this.filePath = Paths.get(generateFilePath());
    ensureDirectoryExists(filePath);

    this.writer =
        Files.newBufferedWriter(
            filePath, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  /**
   * Logs Kintone errors
   *
   * @param recordData Original record data (Map format)
   * @param errorCode Error code
   * @param errorMessage Error message (including field errors)
   */
  public void logError(Map<String, Object> recordData, String errorCode, String errorMessage) {
    if (!enabled) {
      return;
    }

    ErrorRecord errorRecord = new ErrorRecord(recordData, errorCode, errorMessage);
    writeRecord(errorRecord);
    errorCount++;
  }

  private void writeRecord(ErrorRecord record) {
    if (writer == null) {
      return;
    }

    try {
      writer.write(gson.toJson(record));
      writer.newLine();
      recordCount++;

      if (recordCount % FLUSH_INTERVAL == 0) {
        writer.flush();
      }
    } catch (IOException e) {
      logger.error("Failed to write error record", e);
    }
  }

  private String generateFilePath() {
    return String.format("%s_task%03d.jsonl", outputPath, taskIndex);
  }

  private void ensureDirectoryExists(Path path) throws IOException {
    Path parent = path.getParent();
    if (parent != null && !Files.exists(parent)) {
      Files.createDirectories(parent);
    }
  }

  @Override
  public void close() {
    if (writer != null) {
      try {
        writer.flush();
        writer.close();
        writer = null; // Prevent multiple close calls

        if (errorCount == 0 && filePath != null) {
          Files.deleteIfExists(filePath);
        }
      } catch (IOException e) {
        logger.error("Failed to close error file", e);
      }
    }
  }
}
