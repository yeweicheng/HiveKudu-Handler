/**
 * Copyright 2016 Bimal Tandel

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.hadoop.hive.kududb.KuduHandler;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.kudu.Type;
import org.kududb.mapred.HiveKuduTableInputFormat;

import java.util.*;


/**
 * Created by bimal on 4/12/16.
 */

public class HiveKuduSerDe extends AbstractSerDe {

    private static final Log LOG = LogFactory.getLog(HiveKuduSerDe.class);

    private HiveKuduWritable cachedWritable; //Currently Update/Delete not supported from Hive.
    private StructObjectInspector objectInspector;
    private List<Object> deserializeCache;
    private int fieldCount;

    public HiveKuduSerDe() {
    }

    @Override
    public void initialize(Configuration sysConf, Properties tblProps)
            throws SerDeException {
        LOG.debug("tblProps: " + tblProps);

        String tableName = sysConf.get(HiveKuduConstants.TABLE_NAME);
        String columnNameProperty = tblProps
                .getProperty(HiveKuduConstants.LIST_COLUMNS);
        String columnTypeProperty = tblProps
                .getProperty(HiveKuduConstants.LIST_COLUMN_TYPES);

        LOG.warn(HiveKuduConstants.LIST_COLUMNS + ": " + columnNameProperty);
        LOG.warn(HiveKuduConstants.LIST_COLUMN_TYPES + ": " + columnTypeProperty);


        if (columnNameProperty.length() == 0
                && columnTypeProperty.length() == 0) {
            //This is where we will implement option to connect to Kudu and get the column list using Serde.
        }

        List<String> columnNamesTmp = Arrays.asList(columnNameProperty.split(","));
        String[] columnTypesTmp = columnTypeProperty.split(":");
        if (columnNamesTmp.size() != columnTypesTmp.length) {
            throw new SerDeException("Splitting column and types failed." + "columnNames: "
                    + columnNamesTmp + ", columnTypes: "
                    + Arrays.toString(columnTypesTmp));
        }

        // DEBUG
//        Iterator<Map.Entry<String, String>> iterator = sysConf.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, String> e = iterator.next();
//            LOG.warn(e.getKey() + " -> " + e.getValue());
//        }

        boolean ignoreFilter = sysConf.getBoolean(HiveKuduConstants.IGNORE_PUSH_FILTER, false);
        if (ignoreFilter) {
            LOG.warn("ignore push filter enabled");
        }

        String columns = sysConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
        String columnsInside = sysConf.get(HiveKuduConstants.INSIDE_PUSH_FILTER);
        String columnsInsideTable = sysConf.get(HiveKuduConstants.INSIDE_PUSH_FILTER + "." + tableName);
        String onlyKuduTable = sysConf.get(HiveKuduConstants.ONLY_KUDU_TABLE);
        String isKuduTable = sysConf.get(HiveKuduConstants.IS_KUDU_TABLE);
//        LOG.warn(HiveKuduConstants.ONLY_KUDU_TABLE + ": " + onlyKuduTable);
//        LOG.warn(HiveKuduConstants.IS_KUDU_TABLE + ": " + isKuduTable);
        LOG.warn(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR + ": " + columns);
//        LOG.warn(HiveKuduConstants.INSIDE_PUSH_FILTER + ": " + columnsInside);
//        LOG.warn(HiveKuduConstants.INSIDE_PUSH_FILTER + "." + tableName + ": " + columnsInsideTable);
//
//        if (StringUtils.isNotBlank(columnsInside) && !"*".equals(columnsInside) && !"true".equals(onlyKuduTable)) {
//            columns = columnsInside;
//        } else if (StringUtils.isNotBlank(columnsInsideTable) && !"*".equals(columnsInsideTable) && !"true".equals(onlyKuduTable)) {
//            columns = columnsInsideTable;
//        } else if (!"true".equals(onlyKuduTable)) {
//            columns = null;
//        }

        if (!"true".equals(onlyKuduTable)) {
            columns = null;
        }

        if (StringUtils.isNotBlank(columns) && (columns.startsWith("_col") || columns.startsWith(",_col"))) {
            columns = null;
        }

        List<String> columnNames;
        String[] columnTypes;
        if (!ignoreFilter && StringUtils.isNotBlank(columns)) {
            Map<String, Integer> allColMap = new HashMap<>();
            for (int i = 0; i < columnNamesTmp.size(); i++) {
                allColMap.put(columnNamesTmp.get(i), i);
            }

            LOG.warn("filter columns: " + columns);
            List<String> filterColumn = Arrays.asList(columns.split(","));

            columnNames = new ArrayList<>();
            for (int i = 0; i < filterColumn.size(); i++) {
                if (allColMap.containsKey(filterColumn.get(i))) {
                    columnNames.add(filterColumn.get(i));
                }
            }

            columnTypes = new String[columnNames.size()];
            for (int i = 0; i < columnNames.size(); i++) {
                columnTypes[i] = columnTypesTmp[allColMap.get(columnNames.get(i))];
            }
        } else {
            LOG.warn("use all columns ");
            columnNames = columnNamesTmp;
            columnTypes = columnTypesTmp;
        }

        final Type[] types = new Type[columnTypes.length];

        for (int i = 0; i < types.length; i++) {
            types[i] = HiveKuduBridgeUtils.hiveTypeToKuduType(columnTypes[i]);
        }

        this.cachedWritable = new HiveKuduWritable(types);

        this.fieldCount = types.length;

        final List<ObjectInspector> fieldOIs = new ArrayList<>(columnTypes.length);

        for (int i = 0; i < types.length; i++) {
            ObjectInspector oi = HiveKuduBridgeUtils.getObjectInspector(types[i], columnTypes[i]);
            fieldOIs.add(oi);
        }

        this.objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, fieldOIs);

        this.deserializeCache = new ArrayList<>(columnTypes.length);

    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return HiveKuduWritable.class;
    }

    @Override
    public HiveKuduWritable serialize(Object row, ObjectInspector inspector)
            throws SerDeException {

        final StructObjectInspector structInspector = (StructObjectInspector) inspector;
        final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();

        if (fields.size() != fieldCount) {
            throw new SerDeException(String.format(
                    "Required %d columns, received %d.", fieldCount,
                    fields.size()));
        }

        cachedWritable.clear();
        StructField structField;
//        Map<String, StructField> fieldMap = new HashMap<>(fields.size());
//        for (int i = 0; i < fields.size(); i++) {
//            structField = fields.get(i);
//            fieldMap.put(structField.getFieldName(), structField);
//        }

        for (int i = 0; i < fieldCount; i++) {
            structField = fields.get(i);
//            structField = fieldMap.get(columnNames.get(i));
            if (structField != null) {
                Object field = structInspector.getStructFieldData(row,
                        structField);
                ObjectInspector fieldOI = structField.getFieldObjectInspector();

                Object javaObject = HiveKuduBridgeUtils.deparseObject(field,
                        fieldOI);
                cachedWritable.set(i, javaObject);
            }
        }
        return cachedWritable;
    }

    @Override
    public Object deserialize(Writable record) throws SerDeException {
        if (!(record instanceof HiveKuduWritable)) {
            throw new SerDeException("Expected HiveKuduWritable, received "
                    + record.getClass().getName());
        }
        HiveKuduWritable tuple = (HiveKuduWritable) record;
        deserializeCache.clear();
        for (int i = 0; i < tuple.getColCount(); i++) {
            Object o = tuple.get(i);
            deserializeCache.add(o);
        }
        return deserializeCache;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // TODO How to implement this?
        return null;
    }
}


