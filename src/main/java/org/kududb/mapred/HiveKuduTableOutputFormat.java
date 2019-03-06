package org.kududb.mapred;

/**
 * Created by bimal on 4/13/16.
 */
import org.apache.hadoop.hive.kududb.KuduHandler.HiveKuduConstants;
import org.apache.hadoop.hive.kududb.KuduHandler.HiveKuduWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({ "deprecation", "rawtypes" })
public class HiveKuduTableOutputFormat implements OutputFormat, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(HiveKuduTableOutputFormat.class);

    /** Job parameter that specifies how long we wait for operations to complete */
    static final String OPERATION_TIMEOUT_MS_KEY = "kudu.mapreduce.operation.timeout.ms";

    static final String OPERATION_IGNORE_ROW_ERROR = "kudu.mapreduce.operation.ignore.row.error";

    /** Number of rows that are buffered before flushing to the tablet server */
    static final String BUFFER_ROW_COUNT_KEY = "kudu.mapreduce.buffer.row.count";

    /**
     * Job parameter that specifies which key is to be used to reach the HiveKuduTableOutputFormat
     * belonging to the caller
     */
    static final String MULTITON_KEY = "kudu.mapreduce.multitonkey";

    /**
     * This multiton is used so that the tasks using this output format/record writer can find
     * their KuduTable without having a direct dependency on this class,
     * with the additional complexity that the output format cannot be shared between threads.
     */
    private static final ConcurrentHashMap<String, HiveKuduTableOutputFormat> MULTITON = new
            ConcurrentHashMap<String, HiveKuduTableOutputFormat>();

    /**
     * This counter helps indicate which task log to look at since rows that weren't applied will
     * increment this counter.
     */
    public enum Counters { ROWS_WITH_ERRORS }

    private static final int ROW_COUNT_FLAG_TIME = 60000;

    private Configuration conf = null;

    private KuduClient client;
    private KuduTable table;
    private KuduSession session;
    private long operationTimeoutMs;
    private boolean operationIgnoreRowError;
    private List<ColumnSchema> columns;
    private int columnSize;

    @Override
    public void setConf(Configuration entries) {
        LOG.warn("I was called : setConf");
        this.conf = new Configuration(entries);

        String masterAddress = this.conf.get(HiveKuduConstants.MASTER_ADDRESS_NAME);
        String tableName = this.conf.get(HiveKuduConstants.TABLE_NAME);
        this.operationTimeoutMs = this.conf.getLong(OPERATION_TIMEOUT_MS_KEY,
                AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS);
        this.operationIgnoreRowError = this.conf.getBoolean(OPERATION_IGNORE_ROW_ERROR, false);
        int bufferSpace = this.conf.getInt(BUFFER_ROW_COUNT_KEY, 1000);

        LOG.warn(" the master address here is " + masterAddress);
        LOG.warn(" the output table here is " + tableName);

        this.client = new KuduClient.KuduClientBuilder(masterAddress)
                .defaultOperationTimeoutMs(operationTimeoutMs)
                .build();
        try {
            this.table = client.openTable(tableName);
        } catch (Exception ex) {
            throw new RuntimeException("Could not obtain the table from the master, " +
                    "is the master running and is this table created? tablename=" + tableName + " and " +
                    "master address= " + masterAddress, ex);
        }
        this.session = client.newSession();
        this.session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
        this.session.setMutationBufferSpace(bufferSpace);
        this.session.setIgnoreAllDuplicateRows(true);
        String multitonKey = String.valueOf(Thread.currentThread().getId());
        assert(MULTITON.get(multitonKey) == null);
        MULTITON.put(multitonKey, this);
        entries.set(MULTITON_KEY, multitonKey);

        Schema schema = table.getSchema();
        this.columns = schema.getColumns();
        this.columnSize = columns.size();
    }

    private void shutdownClient() throws IOException {
        LOG.warn("I was called : shutdownClient");
        try {
            client.shutdown();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static KuduTable getKuduTable(String multitonKey) {
        return MULTITON.get(multitonKey).getKuduTable();
    }

    private KuduTable getKuduTable() {
        return this.table;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }


    @Override
    public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable)
            throws IOException {
        return new TableRecordWriter(this.session);
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        shutdownClient();
    }

    /*
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws
            IOException, InterruptedException {
        return new KuduTableOutputCommitter();
    }
    */

    protected class TableRecordWriter implements RecordWriter<NullWritable, HiveKuduWritable> {

        private final AtomicLong rowsWithErrors = new AtomicLong();
        private final KuduSession session;
        private long rowCount = 0;

        public TableRecordWriter(KuduSession session) {
            this.session = session;
        }

        private Operation getOperation(HiveKuduWritable hiveKuduWritable)
            throws IOException{
            int recCount = hiveKuduWritable.getColCount();

            if (recCount != columnSize) {
                throw new IOException("Kudu table column count of " + columnSize + " does not match "
                        + "with Serialized object record count of " + recCount);
            }
            //TODO: Find out if the record needs to be updated or deleted.
            //Assume only insert

            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();

            for (int i = 0; i < columnSize; i++) {
                Object obj = hiveKuduWritable.get(i);
                if (obj == null) {
                    row.setNull(i);
                    continue;
                }

                switch(hiveKuduWritable.getType(i)) {
                    case STRING: {
                        row.addString(i, obj.toString());
                        break;
                    }
                    case FLOAT: {
                        row.addFloat(i, (Float) obj);
                        break;
                    }
                    case DOUBLE: {
                        row.addDouble(i, (Double) obj);
                        break;
                    }
                    case BOOL: {
                        row.addBoolean(i, (Boolean) obj);
                        break;
                    }
                    case INT8: {
                        row.addByte(i, (Byte) obj);
                        break;
                    }
                    case INT16: {
                        row.addShort(i, (Short) obj);
                        break;
                    }
                    case INT32: {
                        row.addInt(i, (Integer) obj);
                        break;
                    }
                    case INT64: {
                        row.addLong(i, (Long) obj);
                        break;
                    }
                    case UNIXTIME_MICROS: {
                        row.addLong(i, (Long) obj);
                        break;
                    }
                    case BINARY: {
                        row.addBinary(i, (byte[]) obj);
                        break;
                    }
                    default:
                        throw new IOException("Cannot write Object '"
                                + obj.getClass().getSimpleName() + "' as type: " + hiveKuduWritable.getType(i).name());
                }
            }

            return insert;
        }
        @Override
        public void write(NullWritable key, HiveKuduWritable kw)
                throws IOException {
            try {
                if (System.currentTimeMillis()%ROW_COUNT_FLAG_TIME == 0) {
                    LOG.info("current line size : " + rowCount);
                }

                Operation operation = getOperation(kw);
                session.apply(operation);

                rowCount++;
            } catch (Exception e) {
                throw new IOException("Encountered an error while writing", e);
            }
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            try {
                LOG.info("all line size : " + rowCount);
                processRowErrors(session.close());
            } catch (Exception e) {
                throw new IOException("Encountered an error while closing this task", e);
            } finally {
                shutdownClient();
                if (reporter != null) {
                    // This is the only place where we have access to the context in the record writer,
                    // so set the counter here.
                    reporter.getCounter(Counters.ROWS_WITH_ERRORS).setValue(rowsWithErrors.get());
                }
            }
        }

        private void processRowErrors(List<OperationResponse> responses) throws Exception {
            List<RowError> errors = OperationResponse.collectErrors(responses);
            if (!errors.isEmpty()) {
                int rowErrorsCount = errors.size();
                rowsWithErrors.addAndGet(rowErrorsCount);
                StringBuilder sb = new StringBuilder("Got per errors for ").append(rowErrorsCount)
                        .append(" rows, ").append("the errors(").append(errors.size()).append(") list: \n");
                for (int i = 0; i < errors.size() && i < 10; i++) {
                    sb.append(errors.get(i).getErrorStatus().toString()).append("\n");
                }
                if (errors.size() > 10) {
                    sb.append("...... \n");
                }
                LOG.error(sb.toString());
                if (!operationIgnoreRowError) {
                    throw new IOException(sb.toString());
                }
            }
        }
    }
}