import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 连接到HBase的服务
 */
public class Demo1_Conn {
    public static void main(String[] args) throws IOException {
        //1. 获取连接配置对象
        Configuration configuration = new Configuration();
        //2. 设置连接hbase的参数
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //3. 获取Admin对象
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        //4. 检验指定表是否存在，来判断是否连接到hbase
        boolean flag = hBaseAdmin.tableExists("ns1:aaa");
        //5. 打印
        System.out.println(flag);
    }

}