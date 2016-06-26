package org.embulk.output.analytics_cloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.StringWriter;
import java.util.*;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.google.common.base.Optional;
import java.io.IOException;

import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

public class AnalyticsCloudOutputPlugin
        implements OutputPlugin
{
    protected static Logger logger;
    private static PartnerConnection client = null;
    private static Integer batchSize;
    private static String insightsExternalDataId;
    public static Integer partNumber = 1;

    public static HashMap<String, String> DATATYPE_MAP = new HashMap<String, String>(){
        {
            put("string", "Text");
            put("long", "Numeric");
            put("boolean", "Text");
            put("timestamp", "Date");
            put("double", "Numeric");
            put("json", "Text");
        }
    };

    public interface AutoMetadataTask extends Task
    {
        @Config("connector")
        @ConfigDefault("\"EmbulkOutputPluginConnector\"")
        public Optional<String> getConnectorName();

        @Config("description")
        @ConfigDefault("\"\"")
        public Optional<String> getDescription();

        @Config("scale")
        @ConfigDefault("4")
        public Optional<Integer> getScale();

        @Config("precision")
        @ConfigDefault("18")
        public Optional<Integer> getPrecision();

        @Config("defaultValue")
        @ConfigDefault("0")
        public Optional<Double> getDefaultValue();

        @Config("format")
        @ConfigDefault("\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"")
        public Optional<String> getFormat();

    }

    public interface PluginTask
            extends Task
    {
        @Config("username")
        public String getUsername();

        @Config("password")
        public String getPassword();

        @Config("login_endpoint")
        @ConfigDefault("\"https://login.salesforce.com\"")
        public Optional<String> getLoginEndpoint();
        
        @Config("edgemart_alias")
        public String getEdgemartAlias();
        
        @Config("operation")
        @ConfigDefault("\"Append\"")
        public Optional<String> getOperation();
        
        @Config("metadata_json")
        @ConfigDefault("null")
        public Optional<String> getMetadataJson();

        @Config("auto_metadata_settings")
        @ConfigDefault("null")
        public Optional<AutoMetadataTask> getAutoMetadataTask();

        @Config("batch_size")
        @ConfigDefault("3000")
        public Integer getBatchSize();

        @Config("version")
        @ConfigDefault("34.0")
        public Optional<String> getVersion();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        logger = Exec.getLogger(getClass());

        batchSize = task.getBatchSize();
        final String username = task.getUsername();
        final String password = task.getPassword();
        final String loginEndpoint = task.getLoginEndpoint().get();
        try {
            if (client == null) {
                ConnectorConfig connectorConfig = new ConnectorConfig();
                connectorConfig.setUsername(username);
                connectorConfig.setPassword(password);
                connectorConfig.setAuthEndpoint(loginEndpoint + "/services/Soap/u/" +task.getVersion().get() + "/");

                client = Connector.newConnection(connectorConfig);
                GetUserInfoResult userInfo = client.getUserInfo();
                logger.info("login successful with {}", userInfo.getUserName());
                insightsExternalDataId = this.createInsightsExternalData(task, schema);
            }
        } catch(ConnectionException ex) {
            logger.error("Login error. Please check your credentials.");
            throw new RuntimeException(ex);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
            
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("analytics_cloud output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
        if (insightsExternalDataId != null) {
            final SObject insightsExdata = new SObject();
            insightsExdata.setType("InsightsExternalData");
            insightsExdata.setId(insightsExternalDataId);
            insightsExdata.addField("Action", "Process");
            try {
                SaveResult[] srs = client.update(new SObject[]{insightsExdata});
                if (srs[0].getSuccess()) {
                    logger.info("Import is processing.");
                } else {
                    logger.error(srs[0].getErrors()[0].getMessage());
                }
            } catch (ConnectionException ex) {
                logger.error(ex.getMessage());
            }
        }
        logger.info("logout");
        try {
            if (client != null) {
                client.logout();
            }
        } catch (ConnectionException ex) {}
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PageReader reader = new PageReader(schema);
        return new AnalyticsCloudPageOutput(reader, client);
    }
    
    public class AnalyticsCloudPageOutput
            implements TransactionalPageOutput
    {
        private final PageReader pageReader;
        private final PartnerConnection client;
        private ArrayList<ArrayList<String>> records;

        public AnalyticsCloudPageOutput(final PageReader pageReader,
                PartnerConnection client)
        {
            this.pageReader = pageReader;
            this.client = client;
            this.records = new ArrayList<>();
        }

        @Override
        public void add(Page page)
        {
            try {
                synchronized (AnalyticsCloudOutputPlugin.partNumber) {
                    if (AnalyticsCloudOutputPlugin.partNumber == 0) {
                        AnalyticsCloudOutputPlugin.partNumber++;
                        ArrayList<String> header = new ArrayList<>();
                        for (Column column : this.pageReader.getSchema().getColumns()) {
                            header.add(column.getName());
                        }
                        this.records.add(header);
                    }
                }

                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    ArrayList<String> record = new ArrayList<>();

                    pageReader.getSchema().visitColumns(new ColumnVisitor() {
                        @Override
                        public void doubleColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                record.add("");
                            } else {
                                record.add(String.valueOf(pageReader.getDouble(column)));
                            }
                        }
                        @Override
                        public void timestampColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                record.add("");
                            } else {
                                DateTime dt = new DateTime(pageReader.getTimestamp(column).getEpochSecond() * 1000);
                                record.add(dt.toString("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
                            }
                        }
                        @Override
                        public void stringColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                record.add("");
                            } else {
                                record.add(pageReader.getString(column));
                            }
                        }
                        @Override
                        public void longColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                record.add("");
                            } else {
                                record.add(String.valueOf(pageReader.getLong(column)));
                            }
                        }
                        @Override
                        public void booleanColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                record.add("");
                            } else {
                                record.add(String.valueOf(pageReader.getBoolean(column)));
                            }
                        }

                    });
                    this.records.add(record);

                    if (this.records.size() == batchSize) {
                        this.action();
                    }
                }

            } catch (ConfigException ex) {
                logger.error("Configuration Error: {}", ex.getMessage());
            } catch (ApiFault ex) {
                logger.error("API Error: {}", ex.getExceptionMessage());
            } catch (ConnectionException ex) {
                logger.error("Connection Error: {}", ex.getMessage());
            }
        }

        @Override
        public void finish()
        {
            try {
                if (!this.records.isEmpty()) {
                    this.action();
                }
            } catch (ConnectionException e) {
                e.printStackTrace();
            }

        }

        @Override
        public void close()
        {
            
        }

        @Override
        public void abort()
        {
        }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        private void action() throws ConnectionException{
            final SObject insightsExdataPart = new SObject();
            insightsExdataPart.setType("InsightsExternalDataPart");
            insightsExdataPart.addField("InsightsExternalDataId", AnalyticsCloudOutputPlugin.insightsExternalDataId);
            insightsExdataPart.addField("DataFile", this.createCsvBody(this.records));
            synchronized (AnalyticsCloudOutputPlugin.partNumber) {
                insightsExdataPart.addField("PartNumber", AnalyticsCloudOutputPlugin.partNumber);
                AnalyticsCloudOutputPlugin.partNumber++;
            }

            SaveResult[] srs = this.client.create(new SObject[]{insightsExdataPart});
            if (srs[0].getSuccess()) {
                logger.info("InsightsExternalDataPart whose part number is {} is created.", insightsExdataPart.getField("PartNumber"));
            } else {
                String errorMessage = srs[0].getErrors()[0].getMessage();
                logger.error(errorMessage);
                throw new RuntimeException(errorMessage);
            }
            this.records = new ArrayList<>();
        }



        private byte[] createCsvBody(ArrayList<ArrayList<String>> records) {
            StringWriter stringWriter = new StringWriter();
            ICsvListWriter writer = new CsvListWriter(stringWriter, CsvPreference.EXCEL_PREFERENCE);
            try {
                for (ArrayList<String> record : records) {
                    writer.write(record);
                }
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return stringWriter.toString().getBytes();
        }
        

    }

    private String createInsightsExternalData(PluginTask task, Schema schema) {
        final SObject insightsExdata = new SObject();
        insightsExdata.setType("InsightsExternalData");
        insightsExdata.addField("EdgemartAlias", task.getEdgemartAlias());
        insightsExdata.addField("Action", "None");
        insightsExdata.addField("Operation", task.getOperation().get());
        if (task.getMetadataJson().isPresent()) {
            insightsExdata.addField("MetadataJson", task.getMetadataJson().get().getBytes());
        } else if (task.getAutoMetadataTask().isPresent()){
            insightsExdata.addField("MetadataJson", this.createMetadataJSON(task, schema).getBytes());
        }

        try {
            SaveResult[] srs = this.client.create(new SObject[]{ insightsExdata });
            if (srs[0].getSuccess()) {
                return srs[0].getId();
            }
            String errorMessage = srs[0].getErrors()[0].getMessage();
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        } catch (ConnectionException ex) {
            logger.debug(ex.getMessage() + ":" + ex.getStackTrace());
            throw new RuntimeException(ex);
        }
    }

    private String createMetadataJSON(PluginTask task, Schema schema) {
        HashMap<String, Object> metadata = new HashMap<>();
        metadata.put("fileFormat", new HashMap<String, Object>(){
            {
                put("charsetName", "UTF-8");
                put("fieldsEnclosedBy", "\"");
                put("fieldsDelimitedBy", ",");
                put("numberOfLinesToIgnore", 1);
            }
        });

        ArrayList<HashMap<String, Object>> fields = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            fields.add(new HashMap<String, Object>(){
                {
                    put("name", column.getName());
                    put("label", column.getName());
                    put("fullyQualifiedName", column.getName());
                    put("description", "");
                    put("isSystemField", false);
                    put("type", DATATYPE_MAP.get(column.getType().toString()));

                    if (column.getType().getJavaType().equals(Timestamp.class)) {
                        put("format", task.getAutoMetadataTask().get().getFormat().get());
                    }
                    if (column.getType().getJavaType().equals(long.class)) {
                        put("scale", task.getAutoMetadataTask().get().getScale().get());
                        put("precision", task.getAutoMetadataTask().get().getPrecision().get());
                        put("defaultValue", task.getAutoMetadataTask().get().getDefaultValue().get());
                    }
                }
            });
        }

        ArrayList<HashMap<String, Object>> objects = new ArrayList<>();
        objects.add(new HashMap<String, Object>() {
            {
                put("connector", task.getAutoMetadataTask().get().getConnectorName().get());
                put("description", task.getAutoMetadataTask().get().getDescription().get());
                put("fullyQualifiedName", task.getEdgemartAlias());
                put("label", task.getEdgemartAlias());
                put("name", task.getEdgemartAlias());
                put("fields", fields);
            }
        });
        metadata.put("objects", objects);

        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(metadata);
        } catch (JsonProcessingException ex) {
            logger.error(ex.getMessage());
        }
        return null;
    }
}
