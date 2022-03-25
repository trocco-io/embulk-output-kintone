package org.embulk.output.kintone;

import static org.junit.Assert.assertEquals;

import org.embulk.config.ConfigSource;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.standards.CsvParserPlugin;
import org.embulk.standards.LocalFileInputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;



public class TestKintoneOutputPlugin
{

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .registerPlugin(OutputPlugin.class, "kintone", KintoneOutputPlugin.class)
            .build();


    @Test
    public void testConfigDefault() {
        final ConfigSource config = embulk.configLoader()
                                          .newConfigSource()
                                          .set("type", "kintone");
        final PluginTask task = config.loadConfig(PluginTask.class);

        assertEquals("insert", task.getMode());
    }
}
