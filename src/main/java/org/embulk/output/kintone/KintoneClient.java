package org.embulk.output.kintone;

import com.cybozu.kintone.client.authentication.Auth;
import com.cybozu.kintone.client.connection.Connection;
import com.cybozu.kintone.client.exception.KintoneAPIException;
import com.cybozu.kintone.client.model.cursor.CreateRecordCursorResponse;
import com.cybozu.kintone.client.model.record.GetRecordsResponse;
import com.cybozu.kintone.client.module.recordCursor.RecordCursor;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Page;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static org.embulk.output.kintone.KintoneMode.INSERT;

public class KintoneClient
        implements TransactionalPageOutput
{
    private final Logger logger = LoggerFactory.getLogger(KintoneClient.class);
    private static final int FETCH_SIZE = 500;
    private Auth kintoneAuth;
    private RecordCursor kintoneRecordManager;
//    private Connection con;
    public Connection con;
    private CreateRecordCursorResponse cursor;

    public KintoneClient(){
        this.kintoneAuth = new Auth();
    }

    public void validateAuth(final PluginTask task) throws ConfigException{
        if (task.getUsername().isPresent() && task.getPassword().isPresent()) {
            return;
        } else if (task.getToken().isPresent()) {
            return;
        } else {
            throw new ConfigException("Username and password or token must be provided");
        }
    }

    public void connect(final PluginTask task) {
        if (task.getUsername().isPresent() && task.getPassword().isPresent()) {
            this.kintoneAuth.setPasswordAuth(task.getUsername().get(), task.getPassword().get());
        } else if (task.getToken().isPresent()) {
            this.kintoneAuth.setApiToken(task.getToken().get());
        }

        if (task.getBasicAuthUsername().isPresent() && task.getBasicAuthPassword().isPresent()) {
            this.kintoneAuth.setBasicAuth(task.getBasicAuthUsername().get(),
                    task.getBasicAuthPassword().get());
        }

        if (task.getGuestSpaceId().isPresent()) {
            this.con = new Connection(task.getDomain(), this.kintoneAuth, task.getGuestSpaceId().or(-1));
        } else {
            this.con = new Connection(task.getDomain(), this.kintoneAuth);
        }
        this.kintoneRecordManager = new RecordCursor(con);
    }

    @Override
    public void add(Page page) {

    }

    @Override
    public void finish() {

    }

    @Override
    public void close() {

    }

    @Override
    public void abort() {

    }

    @Override
    public TaskReport commit() {
        return null;
    }
}
