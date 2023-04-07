package com.geektime.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * https://xyueji.gitee.io/Flink%E5%AD%A6%E4%B9%A0--%E5%A6%82%E4%BD%95%E8%AE%A1%E7%AE%97%E5%AE%9E%E6%97%B6%E7%83%AD%E9%97%A8%E5%95%86%E5%93%81.html
 *
 * 数据集
 * https://tianchi.aliyun.com/dataset/649?t=1680785499836
 */
public class TopNBySQL {

    public static void main(String[] args) {
        EnvironmentSettings environmentSettings= EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);

        tEnv.executeSql("create table sourceTable (userId bigint,itemId bigint,categoryId bigint,behavior string,ts timestamp(3),watermark for ts as ts - interval '10' seconds) with ('connector'='filesystem','path'='datas/UserBehavior.csv','format'='csv')");

        String topNItemsSql =
                "SELECT itemId, COUNT(itemId) AS cnt, HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as winstart, HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as winend " +
                        "FROM sourceTable " +
                        "WHERE behavior = 'pv' and "+
                        "GROUP BY itemId, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) " +
                        "ORDER BY cnt DESC " +
                        "LIMIT 5";

//        Table resultTable = tEnv.sqlQuery("select * from sourceTable");
        Table resultTable = tEnv.sqlQuery(topNItemsSql);
        resultTable.printSchema();
        //+----+----------------------+----------------------+-------------------------+
        //| op |               itemId |                  cnt |                winstart |
        //+----+----------------------+----------------------+-------------------------+
        //| +I |                    3 |                    2 | 2023-04-06 14:01:05.000 |
        //| +I |                    3 |                    3 | 2023-04-06 14:01:10.000 |
        //| +I |                    3 |                    1 | 2023-04-06 14:01:15.000 |
        //| +I |                    2 |                    2 | 2023-04-06 14:00:55.000 |
        //| +I |                    1 |                    1 | 2023-04-06 14:00:55.000 |
        //| -D |                    1 |                    1 | 2023-04-06 14:00:55.000 |
        //| +I |                    2 |                    2 | 2023-04-06 14:01:00.000 |
        //+----+----------------------+----------------------+-------------------------+
        //7 rows in set
        resultTable.execute().print();

    }
}
