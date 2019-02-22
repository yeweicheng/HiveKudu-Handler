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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.CreateTableOptions;
import org.kududb.mapred.HiveKuduTableInputFormat;
import org.kududb.mapred.HiveKuduTableOutputFormat;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;

/**
 * Created by bimal on 4/11/16.
 */

@SuppressWarnings({ "deprecation", "rawtypes" })
public class KuduStorageHandler extends DefaultStorageHandler
        implements HiveMetaHook, HiveStoragePredicateHandler {

    private static final Log LOG = LogFactory.getLog(KuduStorageHandler.class);

    private Configuration conf;

    private String kuduMaster;
    private String kuduTableName;

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return HiveKuduTableInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HiveKuduTableOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return HiveKuduSerDe.class;
    }

    public KuduStorageHandler() {
        // TODO: Empty initializer??
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return this;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc,
                                             Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    private void configureJobProperties(TableDesc tableDesc,
                                        Map<String, String> jobProperties) {

        //This will always have the DB Name qualifier of Hive. Dont use this to set Kudu Tablename.
        String tblName = tableDesc.getTableName();
        LOG.debug("Hive Table Name:" + tblName);
        Properties tblProps = tableDesc.getProperties();
        String columnNames = tblProps.getProperty(HiveKuduConstants.LIST_COLUMNS);
        String columnTypes = tblProps.getProperty(HiveKuduConstants.LIST_COLUMN_TYPES);
        LOG.debug("Columns names:" + columnNames);
        LOG.debug("Column types:" + columnTypes);

        if (columnNames.length() == 0) {
            //TODO: Place keeper to insert SerDeHelper code to connect to Kudu to extract column names.
            LOG.warn("SerDe currently doesn't support column names and types. Please provide it explicitly");
        }

        //set map reduce properties.
        jobProperties.put(HiveKuduConstants.MR_INPUT_TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        jobProperties.put(HiveKuduConstants.MR_OUTPUT_TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        jobProperties.put(HiveKuduConstants.MR_MASTER_ADDRESS_NAME,
                tblProps.getProperty(HiveKuduConstants.MASTER_ADDRESS_NAME));

        LOG.debug("Kudu Table Name: " + tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        LOG.debug("Kudu Master Addresses: " + tblProps.getProperty(HiveKuduConstants.MASTER_ADDRESS_NAME));


        //set configuration property
        conf.set(HiveKuduConstants.MR_INPUT_TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        conf.set(HiveKuduConstants.MR_OUTPUT_TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        conf.set(HiveKuduConstants.MR_MASTER_ADDRESS_NAME,
                tblProps.getProperty(HiveKuduConstants.MASTER_ADDRESS_NAME));

        conf.set(HiveKuduConstants.TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        conf.set(HiveKuduConstants.KEY_COLUMNS,
                tblProps.getProperty(HiveKuduConstants.KEY_COLUMNS));
        conf.set(HiveKuduConstants.MASTER_ADDRESS_NAME,
                tblProps.getProperty(HiveKuduConstants.MASTER_ADDRESS_NAME));

        //set class variables
        kuduMaster = conf.get(HiveKuduConstants.MASTER_ADDRESS_NAME);
        kuduTableName = conf.get(HiveKuduConstants.TABLE_NAME);

        for (String key : tblProps.stringPropertyNames()) {
            if (key.startsWith(HiveKuduConstants.MR_PROPERTY_PREFIX)) {
                String value = tblProps.getProperty(key);
                jobProperties.put(key, value);
                //Also set configuration for Non Map Reduce Hive calls to the Handler
                conf.set(key, value);
            }
        }
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        try {
            Class<?>[] classes = {
            };

            FileSystem localFs = FileSystem.getLocal(jobConf);
            Set<String> jars = new HashSet<>(jobConf.getStringCollection("tmpjars"));
            LOG.warn("tmpjars are " + jobConf.get("tmpjars"));
            for (Class<?> clazz : classes) {
                if (clazz == null) {
                    continue;
                }
                final String path = jarFinderGetJar(clazz);
                LOG.warn("class path " + path);
                if (path == null) {
                    throw new RuntimeException("Could not find jar for class " + clazz + " in order to ship it to the cluster.");
                }
                if (!localFs.exists(new Path(path))) {
                    throw new RuntimeException("Could not validate jar file " + path + " for class " + clazz);
                }
                jars.add(path);
            }
            LOG.warn("jars size " + jars.size());
            if (jars.isEmpty()) {
                return;
            }
            //noinspection ToArrayCallWithZeroLengthArrayArgument
            jobConf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    public static String jarFinderGetJar(Class klass) {
        Preconditions.checkNotNull(klass, "klass");
        ClassLoader loader = klass.getClassLoader();
        if (loader != null) {
            String class_file = klass.getName().replaceAll("\\.", "/") + ".class";
            try {
                for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
                    URL url = (URL) itr.nextElement();
                    String path = url.getPath();
                    if (path.startsWith("file:")) {
                        path = path.substring("file:".length());
                    }
                    path = URLDecoder.decode(path, "UTF-8");
                    if ("jar".equals(url.getProtocol())) {
                        path = URLDecoder.decode(path, "UTF-8");
                        return path.replaceAll("!.*$", "");
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException {
        return new DefaultHiveAuthorizationProvider();
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf,
                                                  Deserializer deserializer, ExprNodeDesc predicate) {
        // TODO: Implement push down to Kudu here.
        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        IndexPredicateAnalyzer analyzer =
                HiveKuduTableInputFormat.newIndexPredicateAnalyzer(jobConf.get(HiveKuduConstants.KEY_COLUMNS).split(","));
        List<IndexSearchCondition> conditions = new ArrayList<IndexSearchCondition>();
        ExprNodeGenericFuncDesc residualPredicate =
                (ExprNodeGenericFuncDesc)analyzer.analyzePredicate(predicate, conditions);
        decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(conditions);
        decomposedPredicate.residualPredicate = residualPredicate;


        LOG.warn("handler push filter: " + decomposedPredicate.pushedPredicate.getExprString());
//        decomposedPredicate.pushedPredicate = (ExprNodeGenericFuncDesc) predicate;
//        decomposedPredicate.residualPredicate = null;

        return decomposedPredicate;
    }

    @Override
    public void preCreateTable(Table tbl)
            throws MetaException {
        // Nothing to do
    }

    @Override
    public void commitCreateTable(Table tbl) throws MetaException {
        // Nothing to do
    }

    @Override
    public void preDropTable(Table tbl) throws MetaException {
        // Nothing to do

    }

    @Override
    public void commitDropTable(Table tbl, boolean deleteData)
            throws MetaException {
        // Nothing to do
    }

    @Override
    public void rollbackCreateTable(Table tbl) throws MetaException {
        // Nothing to do
    }

    @Override
    public void rollbackDropTable(Table tbl) throws MetaException {
        // Nothing to do
    }

}
