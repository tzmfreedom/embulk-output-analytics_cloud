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
    public static HashMap<String, String> DATATYPE_MAP;
    static {
        HashMap<String, String> dataTypeMap = new HashMap<>();
        dataTypeMap.put("string", "Text");
        dataTypeMap.put("long", "Numeric");
        dataTypeMap.put("boolean", "Text");
        dataTypeMap.put("timestamp", "Date");
        dataTypeMap.put("double", "Numeric");
        dataTypeMap.put("json", "Text");
        DATATYPE_MAP = dataTypeMap;
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

        @Config("auto_metadata")
        @ConfigDefault("true")
        public Optional<String> getAutoMetadata();

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
        PluginTask task = taskSource.loadTask(PluginTask.class);
        
        PageReader reader = new PageReader(schema);
        return new AnalyticsCloudPageOutput(reader, client, task);
    }
    
    public class AnalyticsCloudPageOutput
            implements TransactionalPageOutput
    {
        private final PageReader pageReader;
        private final PartnerConnection client;
        private ArrayList<ArrayList<String>> records;
        private Integer partNumber;
        private PluginTask task;
        private String insightsExternalDataId;

        public AnalyticsCloudPageOutput(final PageReader pageReader,
                PartnerConnection client, PluginTask task)
        {
            this.pageReader = pageReader;
            this.client = client;
            this.partNumber = 1;
            this.task = task;
            this.records = new ArrayList<>();
        }

        @Override
        public void add(Page page)
        {
            try {
                if (this.records.isEmpty() && this.partNumber == 1) {
                    SaveResult[] srs = this.createInsightsExternalData();
                    if (srs != null) {
                        this.insightsExternalDataId = srs[0].getId();
                    } else {
                        this.insightsExternalDataId = null;
                    }

                    ArrayList<String> header = new ArrayList<>();
                    for (Column column : this.pageReader.getSchema().getColumns()) {
                        header.add(column.getName());
                    }
                    this.records.add(header);
                }

                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    ArrayList<String> record = new ArrayList<>();

                    pageReader.getSchema().visitColumns(new ColumnVisitor() {
                        @Override
                        public void doubleColumn(Column column) {
                            record.add(String.valueOf(pageReader.getDouble(column)));
                        }
                        @Override
                        public void timestampColumn(Column column) {
                            DateTime dt = new DateTime(pageReader.getTimestamp(column).getEpochSecond()*1000);
                            record.add(dt.toString("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
                        }
                        @Override
                        public void stringColumn(Column column) {
                            record.add(pageReader.getString(column));
                        }
                        @Override
                        public void longColumn(Column column) {
                            record.add(String.valueOf(pageReader.getLong(column)));
                        }
                        @Override
                        public void booleanColumn(Column column) {
                            record.add(String.valueOf(pageReader.getBoolean(column)));
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
                    SaveResult[] srs = this.action();
                }
            } catch (ConnectionException e) {
                e.printStackTrace();
            }
            if (this.insightsExternalDataId != null) {
                final SObject insightsExdata = new SObject();
                insightsExdata.setType("InsightsExternalData");
                insightsExdata.setId(this.insightsExternalDataId);
                insightsExdata.addField("Action", "Process");
                try {
                    SaveResult[] srs = this.client.update(new SObject[]{insightsExdata});
                } catch (ConnectionException ex) {
                    logger.error(ex.getMessage());
                }
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

        private SaveResult[] action() throws ConnectionException{
            final SObject insightsExdataPart = new SObject();
            insightsExdataPart.setType("InsightsExternalDataPart");
            insightsExdataPart.addField("InsightsExternalDataId", this.insightsExternalDataId);
            insightsExdataPart.addField("DataFile", this.createCsvBody(this.records));
            insightsExdataPart.addField("PartNumber", this.partNumber);

            SaveResult[] srs = this.client.create(new SObject[]{insightsExdataPart});
            this.records = new ArrayList<>();
            this.partNumber++;
            return srs;
        }

        private SaveResult[] createInsightsExternalData() {
            final SObject insightsExdata = new SObject();
            insightsExdata.setType("InsightsExternalData");
            insightsExdata.addField("EdgemartAlias", task.getEdgemartAlias());
            insightsExdata.addField("Action", "None");
            insightsExdata.addField("Operation", task.getOperation().get());
            if (task.getMetadataJson().isPresent()) {
                insightsExdata.addField("MetadataJson", task.getMetadataJson().get().getBytes());
            } else if (task.getAutoMetadata().get().toLowerCase().equals("true")){
                insightsExdata.addField("MetadataJson", this.createMetadataJSON().getBytes());
            }

            try {
                return this.client.create(new SObject[]{ insightsExdata });
            } catch (ConnectionException ex) {
                logger.debug(ex.getMessage() + ":" + ex.getStackTrace());
            }

            return null;
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
        
        public String createMetadataJSON() {
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
            for (Column column : pageReader.getSchema().getColumns()) {
                fields.add(new HashMap<String, Object>(){
                    {
                        put("name", column.getName());
                        put("label", column.getName());
                        put("fullyQualifiedName", column.getName());
                        put("description", "");
                        put("isSystemField", false);
                        put("type", DATATYPE_MAP.get(column.getType().toString()));

                        if (column.getType().getJavaType().equals(Timestamp.class)) {
                            put("format", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                        }
                        if (column.getType().getJavaType().equals(long.class)) {
                            put("scale", 12);
                            put("precision", 18);
                            put("defaultValue", 0);
                        }
                    }
                });
            }
            
            ArrayList<HashMap<String, Object>> objects = new ArrayList<>();
            objects.add(new HashMap<String, Object>() {
                {
                    put("connector", "EmbulkOutputPluginConnector");
                    put("description", "");
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
}
