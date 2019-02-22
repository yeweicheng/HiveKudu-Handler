package org.kududb.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.kududb.KuduHandler.HiveKuduWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.io.IOException;
import java.sql.Timestamp;

public class KuduTableRecordReader implements RecordReader<NullWritable, HiveKuduWritable> {

    private static final Log LOG = LogFactory.getLog(KuduTableRecordReader.class);

    private final NullWritable currentKey = NullWritable.get();
    private RowResult currentValue;
    private RowResultIterator iterator;
    private KuduScanner scanner;
    private KuduTableSplit tableSplit;
    private Type[] types;
    private KuduClient client;
    private boolean first = true;
    private long rowCount;
    private static final int ROW_COUNT_FLAG_TIME = 60000;

    public KuduTableRecordReader(KuduTableSplit tableSplit, KuduClient client, KuduTable table) throws IOException, InterruptedException {
        LOG.warn("I was called : TableRecordReader");

        this.tableSplit = tableSplit;
        this.client = client;

        scanner = KuduScanToken.deserializeIntoScanner(tableSplit.getScanTokenSerialized(), client);

        Schema schema = table.getSchema();
        types = new Type[schema.getColumnCount()];
        for (int i = 0; i < types.length; i++) {
            types[i] = schema.getColumnByIndex(i).getType();
            LOG.warn("Setting types array " + i + " to " + types[i].name());
        }
        // Calling this now to set iterator.
        tryRefreshIterator();
        rowCount = 0L;
    }

    @Override
    public boolean next(NullWritable o, HiveKuduWritable o2) throws IOException {
        if (System.currentTimeMillis()%ROW_COUNT_FLAG_TIME == 0) {
            LOG.info("current line size : " + rowCount);
        }

        if (!iterator.hasNext()) {
            tryRefreshIterator();
            if (!iterator.hasNext()) {
                // Means we still have the same iterator, we're done
                LOG.info("all line size : " + rowCount);
                return false;
            }
        }

        currentValue = iterator.next();
        o = currentKey;
        o2.clear();
        for (int i = 0; i < types.length; i++) {
            if (currentValue.isNull(i))
                o2.set(i, null);
            else {
                switch (types[i]) {
                    case STRING: {
                        o2.set(i, currentValue.getString(i));
                        break;
                    }
                    case FLOAT: {
                        o2.set(i, currentValue.getFloat(i));
                        break;
                    }
                    case DOUBLE: {
                        o2.set(i, currentValue.getDouble(i));
                        break;
                    }
                    case BOOL: {
                        o2.set(i, currentValue.getBoolean(i));
                        break;
                    }
                    case INT8: {
                        o2.set(i, currentValue.getByte(i));
                        break;
                    }
                    case INT16: {
                        o2.set(i, currentValue.getShort(i));
                        break;
                    }
                    case INT32: {
                        o2.set(i, currentValue.getInt(i));
                        break;
                    }
                    case INT64: {
                        o2.set(i, currentValue.getLong(i));
                        break;
                    }
                    case UNIXTIME_MICROS: {
                        long epoch_micros_seconds = currentValue.getLong(i);
//						long epoch_seconds = epoch_micros_seconds / 1000000L;
//						long nano_seconds_adjustment = (epoch_micros_seconds % 1000000L) * 1000L;
                        // mark jdk 1.8
//						Instant instant = Instant.ofEpochSecond(epoch_seconds, nano_seconds_adjustment);
//						o2.set(i, Timestamp.from(instant));
                        if (String.valueOf(epoch_micros_seconds).length() == 10) {
                            epoch_micros_seconds = epoch_micros_seconds*1000;
                        }
                        o2.set(i, new Timestamp(epoch_micros_seconds));
                        break;
                    }
                    case BINARY: {
                        o2.set(i, currentValue.getBinaryCopy(i));
                        break;
                    }
                    default:
                        throw new IOException("Cannot write Object '" + currentValue.getColumnType(i).name()
                                + "' as type: " + types[i].name());
                }
            }
//            LOG.warn("Value returned " + o2.get(i));
        }
        this.rowCount += 1L;
        return true;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public HiveKuduWritable createValue() {
        return new HiveKuduWritable(types);
    }

    @Override
    public long getPos() throws IOException {
        return this.rowCount;
        //TODO: Get progress
    }

    /**
     * If the scanner has more rows, get a new iterator else don't do anything.
     * @throws IOException
     */
    private void tryRefreshIterator() throws IOException {
        if (!scanner.hasMoreRows()) {
            this.close();
            return;
        }
        try {
            iterator = scanner.nextRows();
        } catch (Exception e) {
            throw new IOException("Couldn't get scan data", e);
        }
    }

    /*
     * Mapreduce code for reference
     *
     * @Override public NullWritable getCurrentKey() throws IOException,
     * InterruptedException { return currentKey; }
     *
     * @Override public RowResult getCurrentValue() throws IOException,
     * InterruptedException { return currentValue; }
     */

    @Override
    public float getProgress() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        LOG.warn("I was called : close");
        try {
            if (!scanner.isClosed()) {
                scanner.close();
            }
        } catch (NullPointerException npe) {
            LOG.warn("The scanner is supposed to be open but its not. TODO: Fix me.");
        } catch (Exception e) {
            throw new IOException(e);
        }
//            this.client.close();
    }

}