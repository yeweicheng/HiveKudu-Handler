package org.kududb.mapred;

import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.*;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduPredicateBuilder {

    public List<KuduScanToken> toPredicateScan(KuduScanToken.KuduScanTokenBuilder builder,
                                               KuduTable table,
                                               List<IndexSearchCondition> conditions) throws IOException {

        if (conditions == null || conditions.isEmpty()) {
            throw new IOException("unsupported conditions, only can use =,>=,>,<=,<");
        }

        Map<String, ColumnSchema> columnMap = new HashMap<>();
        List<ColumnSchema> columnSchemas = table.getSchema().getColumns();
        for (int i = 0; i < columnSchemas.size(); i++) {
            columnMap.put(columnSchemas.get(i).getName(), columnSchemas.get(i));
        }

        String columnName;
        KuduPredicate predicate;
        IndexSearchCondition sc;
        for (int i = 0; i < conditions.size(); i++) {
            sc = conditions.get(i);
            columnName = sc.getColumnDesc().getColumn();
            if (!columnMap.containsKey(columnName)) {
                continue;
            }

            ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());
            PrimitiveObjectInspector objInspector;
            Object writable;

            try {
                objInspector = (PrimitiveObjectInspector)eval.initialize(null);
                writable = eval.evaluate(null);
            } catch (ClassCastException cce) {
                throw new IOException("Currently only primitve types are supported. Found: " +
                        sc.getConstantDesc().getTypeString());
            } catch (HiveException e) {
                throw new IOException(e);
            }

            predicate = toComparison(columnMap.get(columnName), getComparisonOp(sc.getComparisonOp()),
                    writable, objInspector);
            builder.addPredicate(predicate);
        }

        return builder.build();
    }

    private KuduPredicate.ComparisonOp getComparisonOp(String comparisonOp) throws IOException {

        if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(comparisonOp)){
            return KuduPredicate.ComparisonOp.EQUAL;
        } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan".equals(comparisonOp)){
            return KuduPredicate.ComparisonOp.LESS;
        } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan"
                .equals(comparisonOp)) {
            return KuduPredicate.ComparisonOp.GREATER_EQUAL;
        } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan"
                .equals(comparisonOp)){
            return KuduPredicate.ComparisonOp.GREATER;
        } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan"
                .equals(comparisonOp)){
            return KuduPredicate.ComparisonOp.LESS_EQUAL;
        } else {
            throw new IOException(comparisonOp + " is not a supported comparison operator");
        }
    }

    private KuduPredicate toComparison(ColumnSchema columnSchema, KuduPredicate.ComparisonOp op, Object writable,
                                  PrimitiveObjectInspector poi) throws IOException {

        PrimitiveObjectInspector.PrimitiveCategory pc = poi.getPrimitiveCategory();
        switch (poi.getPrimitiveCategory()) {
            case INT:
                return KuduPredicate.newComparisonPredicate(columnSchema,
                        op, ((IntWritable)writable).get());
            case BOOLEAN:
                return KuduPredicate.newComparisonPredicate(columnSchema,
                        op, ((BooleanWritable)writable).get());
            case LONG:
                return KuduPredicate.newComparisonPredicate(columnSchema,
                        op, ((LongWritable)writable).get());
            case FLOAT:
                return KuduPredicate.newComparisonPredicate(columnSchema,
                        op, ((FloatWritable)writable).get());
            case DOUBLE:
                return KuduPredicate.newComparisonPredicate(columnSchema,
                        op, ((DoubleWritable)writable).get());
            case SHORT:
                return KuduPredicate.newComparisonPredicate(columnSchema,
                        op, ((ShortWritable)writable).get());
            case STRING:
                return KuduPredicate.newComparisonPredicate(columnSchema,
                        op, ((Text)writable).toString());
            case BYTE:
                return KuduPredicate.newComparisonPredicate(columnSchema,
                        op, ((ByteWritable)writable).get());

            default:
                throw new IOException("Type not supported " + pc);
        }
    }
}
