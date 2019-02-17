package org.test;

import org.apache.kudu.client.*;

public class KuduSimple {

    public static void main(String[] args) throws Exception {
        KuduClient client = new KuduClient.KuduClientBuilder("local-01:7051").build();

        long startTime = System.currentTimeMillis();
        KuduTable users = client.openTable("users");
        KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(users);
        KuduPredicate predicate = KuduPredicate.newComparisonPredicate(users.getSchema().getColumn("user_id"),
                KuduPredicate.ComparisonOp.EQUAL, 82216821);
//        KuduPredicate predicate = KuduPredicate.newComparisonPredicate(users.getSchema().getColumn("first_name"),
//                KuduPredicate.ComparisonOp.EQUAL, "彩云Dye我心上人");
        builder.addPredicate(predicate);

        KuduScanner scanner = builder.build();
        while (scanner.hasMoreRows()) {
            RowResultIterator iterator = scanner.nextRows();
            while (iterator.hasNext()) {
                RowResult result = iterator.next();
                /**
                 * 输出行
                 */
                System.out.println("user_id:" + result.getInt("user_id"));
                System.out.println("first_name:" + result.getString("first_name"));
                System.out.println("last_name:" + result.getString("last_name"));
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("duration time: " + (endTime - startTime));

        scanner.close();
        client.shutdown();
    }

}
