package org.embulk.output.kintone;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.kintone.client.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kintoneエラーを収集し、構造化してファイルに出力するクラス
 */
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
    
    /**
     * エラーレコードの内部クラス
     */
    private static class ErrorRecord {
        @SerializedName("record_data")
        private final java.util.Map<String, Object> recordData;
        
        @SerializedName("error_code")
        private final String errorCode;
        
        @SerializedName("error_message")
        private final String errorMessage;
        
        @SerializedName("affected_fields")
        private final String[] affectedFields;
        
        @SerializedName("timestamp")
        private final String timestamp;
        
        @SerializedName("task_index")
        private final Integer taskIndex;
        
        @SerializedName("record_index")
        private final Integer recordIndex;
        
        ErrorRecord(java.util.Map<String, Object> recordData,
                   String errorCode, String errorMessage,
                   String[] affectedFields, Integer taskIndex, Integer recordIndex) {
            this.recordData = recordData;
            this.errorCode = errorCode != null ? errorCode : "";
            this.errorMessage = errorMessage != null ? errorMessage : "";
            this.affectedFields = affectedFields;
            this.timestamp = Instant.now().toString();
            this.taskIndex = taskIndex;
            this.recordIndex = recordIndex;
        }
    }
    
    public ErrorFileLogger(String outputPath, int taskIndex) {
        this.outputPath = outputPath;
        this.taskIndex = taskIndex;
        this.gson = new GsonBuilder().disableHtmlEscaping().create();
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
        
        this.writer = Files.newBufferedWriter(
            filePath,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        );
        
    }
    
    /**
     * Kintoneエラーを記録
     * @param record 元のレコードデータ
     * @param errorCode エラーコード
     * @param errorMessage エラーメッセージ（フィールドエラーも含む）
     * @param affectedFields 影響を受けたフィールドのリスト
     * @param recordIndex レコードインデックス
     */
    public void logError(Record record, String errorCode, String errorMessage, 
                         String[] affectedFields, int recordIndex) {
        if (!enabled) {
            return;
        }
        
        java.util.Map<String, Object> recordData = extractAllFields(record);
        
        ErrorRecord errorRecord = new ErrorRecord(
            recordData,
            errorCode,
            errorMessage,
            affectedFields,
            taskIndex,
            recordIndex
        );
        
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
    
    /**
     * Recordから全フィールドを抽出
     */
    private java.util.Map<String, Object> extractAllFields(Record record) {
        java.util.Map<String, Object> fields = new java.util.HashMap<>();
        
        if (record == null) {
            return fields;
        }
        
        // Recordオブジェクトから全フィールドを取得
        record.getFieldCodes(true).forEach(fieldCode -> {
            Object value = record.getFieldValue(fieldCode);
            if (value != null) {
                // 実際の値を取得
                Object actualValue = extractActualValue(value);
                fields.put(fieldCode, actualValue);
            } else {
                fields.put(fieldCode, null);
            }
        });
        
        return fields;
    }
    
    /**
     * FieldValueオブジェクトから実際の値を抽出
     */
    private Object extractActualValue(Object value) {
        if (value == null) {
            return null;
        }
        
        // SingleLineTextFieldValue, MultiLineTextFieldValue などの場合
        if (value instanceof com.kintone.client.model.record.SingleLineTextFieldValue) {
            return ((com.kintone.client.model.record.SingleLineTextFieldValue) value).getValue();
        } else if (value instanceof com.kintone.client.model.record.MultiLineTextFieldValue) {
            return ((com.kintone.client.model.record.MultiLineTextFieldValue) value).getValue();
        } else if (value instanceof com.kintone.client.model.record.NumberFieldValue) {
            com.kintone.client.model.record.NumberFieldValue numberValue = 
                (com.kintone.client.model.record.NumberFieldValue) value;
            return numberValue.getValue() != null ? numberValue.getValue().toString() : null;
        } else if (value instanceof com.kintone.client.model.record.CheckBoxFieldValue) {
            com.kintone.client.model.record.CheckBoxFieldValue checkBoxValue = 
                (com.kintone.client.model.record.CheckBoxFieldValue) value;
            return checkBoxValue.getValues();
        } else if (value instanceof com.kintone.client.model.record.RadioButtonFieldValue) {
            return ((com.kintone.client.model.record.RadioButtonFieldValue) value).getValue();
        } else if (value instanceof com.kintone.client.model.record.DropDownFieldValue) {
            return ((com.kintone.client.model.record.DropDownFieldValue) value).getValue();
        } else if (value instanceof com.kintone.client.model.record.DateFieldValue) {
            com.kintone.client.model.record.DateFieldValue dateValue = 
                (com.kintone.client.model.record.DateFieldValue) value;
            return dateValue.getValue() != null ? dateValue.getValue().toString() : null;
        } else if (value instanceof com.kintone.client.model.record.DateTimeFieldValue) {
            com.kintone.client.model.record.DateTimeFieldValue dateTimeValue = 
                (com.kintone.client.model.record.DateTimeFieldValue) value;
            return dateTimeValue.getValue() != null ? dateTimeValue.getValue().toString() : null;
        } else if (value instanceof com.kintone.client.model.record.TimeFieldValue) {
            com.kintone.client.model.record.TimeFieldValue timeValue = 
                (com.kintone.client.model.record.TimeFieldValue) value;
            return timeValue.getValue() != null ? timeValue.getValue().toString() : null;
        } else if (value instanceof com.kintone.client.model.record.LinkFieldValue) {
            return ((com.kintone.client.model.record.LinkFieldValue) value).getValue();
        } else if (value instanceof com.kintone.client.model.record.RichTextFieldValue) {
            return ((com.kintone.client.model.record.RichTextFieldValue) value).getValue();
        } else if (value instanceof com.kintone.client.model.record.MultiSelectFieldValue) {
            com.kintone.client.model.record.MultiSelectFieldValue multiSelectValue = 
                (com.kintone.client.model.record.MultiSelectFieldValue) value;
            return multiSelectValue.getValues();
        } else if (value instanceof com.kintone.client.model.record.UserSelectFieldValue) {
            com.kintone.client.model.record.UserSelectFieldValue userSelectValue = 
                (com.kintone.client.model.record.UserSelectFieldValue) value;
            java.util.List<String> users = new java.util.ArrayList<>();
            if (userSelectValue.getValues() != null) {
                userSelectValue.getValues().forEach(user -> users.add(user.getCode()));
            }
            return users;
        } else if (value instanceof com.kintone.client.model.record.GroupSelectFieldValue) {
            com.kintone.client.model.record.GroupSelectFieldValue groupSelectValue = 
                (com.kintone.client.model.record.GroupSelectFieldValue) value;
            java.util.List<String> groups = new java.util.ArrayList<>();
            if (groupSelectValue.getValues() != null) {
                groupSelectValue.getValues().forEach(group -> groups.add(group.getCode()));
            }
            return groups;
        } else if (value instanceof com.kintone.client.model.record.OrganizationSelectFieldValue) {
            com.kintone.client.model.record.OrganizationSelectFieldValue orgSelectValue = 
                (com.kintone.client.model.record.OrganizationSelectFieldValue) value;
            java.util.List<String> orgs = new java.util.ArrayList<>();
            if (orgSelectValue.getValues() != null) {
                orgSelectValue.getValues().forEach(org -> orgs.add(org.getCode()));
            }
            return orgs;
        } else if (value instanceof com.kintone.client.model.record.FileFieldValue) {
            com.kintone.client.model.record.FileFieldValue fileValue = 
                (com.kintone.client.model.record.FileFieldValue) value;
            java.util.List<String> fileKeys = new java.util.ArrayList<>();
            if (fileValue.getValues() != null) {
                fileValue.getValues().forEach(file -> fileKeys.add(file.getFileKey()));
            }
            return fileKeys;
        } else {
            // その他のFieldValueタイプや通常のオブジェクトの場合
            return value.toString();
        }
    }
    
    private String generateFilePath() {
        String basePath = outputPath;
        
        // ディレクトリパスの場合の処理
        if (basePath.endsWith("/") || basePath.endsWith("\\")) {
            // ディレクトリの場合、デフォルトファイル名を追加
            basePath = basePath + "kintone_errors.jsonl";
        } else if (!basePath.contains(".")) {
            // 拡張子がない場合、ディレクトリとみなして処理
            if (!basePath.endsWith(java.io.File.separator)) {
                basePath = basePath + java.io.File.separator;
            }
            basePath = basePath + "kintone_errors.jsonl";
        } else if (!basePath.endsWith(".jsonl")) {
            // 別の拡張子がある場合、.jsonlに変更
            basePath = basePath.substring(0, basePath.lastIndexOf(".")) + ".jsonl";
        }
        
        // タスクインデックスを含むファイル名を生成
        String base = basePath.substring(0, basePath.lastIndexOf(".jsonl"));
        return String.format("%s_task%03d.jsonl", base, taskIndex);
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
                writer = null;  // 複数回のclose呼び出しを防ぐ
                
                if (errorCount == 0 && filePath != null) {
                    Files.deleteIfExists(filePath);
                }
            } catch (IOException e) {
                logger.error("Failed to close error file", e);
            }
        }
    }
}