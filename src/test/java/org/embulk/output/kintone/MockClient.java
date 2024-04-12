package org.embulk.output.kintone;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.kintone.client.AppClient;
import com.kintone.client.KintoneClient;
import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.RecordClient;
import com.kintone.client.api.record.GetRecordsByCursorResponseBody;
import com.kintone.client.model.app.field.CheckBoxFieldProperty;
import com.kintone.client.model.app.field.DateFieldProperty;
import com.kintone.client.model.app.field.DateTimeFieldProperty;
import com.kintone.client.model.app.field.DropDownFieldProperty;
import com.kintone.client.model.app.field.FieldProperty;
import com.kintone.client.model.app.field.FileFieldProperty;
import com.kintone.client.model.app.field.GroupSelectFieldProperty;
import com.kintone.client.model.app.field.LinkFieldProperty;
import com.kintone.client.model.app.field.MultiLineTextFieldProperty;
import com.kintone.client.model.app.field.MultiSelectFieldProperty;
import com.kintone.client.model.app.field.NumberFieldProperty;
import com.kintone.client.model.app.field.OrganizationSelectFieldProperty;
import com.kintone.client.model.app.field.RadioButtonFieldProperty;
import com.kintone.client.model.app.field.RichTextFieldProperty;
import com.kintone.client.model.app.field.SingleLineTextFieldProperty;
import com.kintone.client.model.app.field.SubtableFieldProperty;
import com.kintone.client.model.app.field.TimeFieldProperty;
import com.kintone.client.model.app.field.UserSelectFieldProperty;
import com.kintone.client.model.record.Record;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.MockedStatic;

public class MockClient {
  private final String domain;
  private final List<Record> records;
  private final List<String> fields;
  private final String query;
  private final RecordClient mockRecordClient;

  public MockClient(String domain, List<Record> records, List<String> fields, String query) {
    this.domain = domain;
    this.records = records;
    this.fields = fields;
    this.query = query;
    mockRecordClient = mock(RecordClient.class);
  }

  public RecordClient getMockRecordClient() {
    return mockRecordClient;
  }

  public void run(Runnable runnable) throws Exception {
    @SuppressWarnings("unchecked")
    Map<String, FieldProperty> mockFormFields = mock(Map.class);
    when(mockFormFields.get(matches("^.*_single_line_text$")))
        .thenReturn(new SingleLineTextFieldProperty());
    when(mockFormFields.get(matches("^.*_multi_line_text$")))
        .thenReturn(new MultiLineTextFieldProperty());
    when(mockFormFields.get(matches("^.*_rich_text$"))).thenReturn(new RichTextFieldProperty());
    when(mockFormFields.get(matches("^.*_number$"))).thenReturn(new NumberFieldProperty());
    when(mockFormFields.get(matches("^.*_check_box$"))).thenReturn(new CheckBoxFieldProperty());
    when(mockFormFields.get(matches("^.*_radio_button$")))
        .thenReturn(new RadioButtonFieldProperty());
    when(mockFormFields.get(matches("^.*_multi_select$")))
        .thenReturn(new MultiSelectFieldProperty());
    when(mockFormFields.get(matches("^.*_drop_down$"))).thenReturn(new DropDownFieldProperty());
    when(mockFormFields.get(matches("^.*_user_select$"))).thenReturn(new UserSelectFieldProperty());
    when(mockFormFields.get(matches("^.*_organization_select$")))
        .thenReturn(new OrganizationSelectFieldProperty());
    when(mockFormFields.get(matches("^.*_group_select$")))
        .thenReturn(new GroupSelectFieldProperty());
    when(mockFormFields.get(matches("^.*_date$"))).thenReturn(new DateFieldProperty());
    when(mockFormFields.get(matches("^.*_time$"))).thenReturn(new TimeFieldProperty());
    when(mockFormFields.get(matches("^.*_datetime$"))).thenReturn(new DateTimeFieldProperty());
    when(mockFormFields.get(matches("^.*_link$"))).thenReturn(new LinkFieldProperty());
    when(mockFormFields.get(matches("^.*_file$"))).thenReturn(new FileFieldProperty());
    when(mockFormFields.get(matches("^.*_subtable$"))).thenReturn(new SubtableFieldProperty());
    AppClient mockAppClient = mock(AppClient.class);
    when(mockAppClient.getFormFields(eq(0L))).thenReturn(mockFormFields);
    GetRecordsByCursorResponseBody mockGetRecordsByCursorResponseBody =
        mock(GetRecordsByCursorResponseBody.class);
    when(mockGetRecordsByCursorResponseBody.getRecords()).thenReturn(records);
    when(mockGetRecordsByCursorResponseBody.hasNext()).thenReturn(false);
    when(mockRecordClient.createCursor(eq(0L), eq(fields), eq(query))).thenReturn("id");
    when(mockRecordClient.getRecordsByCursor(eq("id")))
        .thenReturn(mockGetRecordsByCursorResponseBody);
    when(mockRecordClient.addRecords(eq(0L), anyList())).thenReturn(Collections.emptyList());
    when(mockRecordClient.updateRecords(eq(0L), anyList())).thenReturn(Collections.emptyList());
    com.kintone.client.KintoneClient mockKintoneClient = mock(KintoneClient.class);
    when(mockKintoneClient.app()).thenReturn(mockAppClient);
    when(mockKintoneClient.record()).thenReturn(mockRecordClient);
    KintoneClientBuilder mockKintoneClientBuilder = mock(KintoneClientBuilder.class);
    when(mockKintoneClientBuilder.authByApiToken(eq("token"))).thenReturn(mockKintoneClientBuilder);
    when(mockKintoneClientBuilder.build()).thenReturn(mockKintoneClient);
    try (MockedStatic<KintoneClientBuilder> mocked = mockStatic(KintoneClientBuilder.class)) {
      mocked
          .when(() -> KintoneClientBuilder.create(String.format("https://%s", domain)))
          .thenReturn(mockKintoneClientBuilder);
      runnable.run();
    }
  }

  public interface Runnable {
    void run() throws Exception;
  }
}
