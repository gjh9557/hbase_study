package DMLhbase;


import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import java.io.IOException;


public class Demogettable {
    public static Table getTable(String tablename){
        Configuration configuration = new Configuration();
        //2. 设置连接hbase的参数
        configuration.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //3. 获取Admin对象
        Connection connection = null;
        Table table=null;
        if(StringUtils.isNotEmpty(tablename)){

                try {
                    table=connection.getTable(TableName.valueOf(tablename));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        return  table;
    }
    public static void close(Table table){
        if (table!=null){
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
