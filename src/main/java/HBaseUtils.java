import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;


/**
 * HBase Client工具类
 */
public class HBaseUtils {

    private static final Logger logger = Logger.getLogger(HBaseUtils.class);

    private final static String CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String CONNECT_VALUE = "hadoop01:2181,hadoop02:2181,hadoop03:2181";

    /**
     * 获取Admin对象
     */
    public static Admin getAdmin() {
        //1. 获取连接配置对象
        Configuration configuration = new Configuration();
        //2. 设置连接hbase的参数
        configuration.set(CONNECT_KEY, CONNECT_VALUE);
        //3. 获取Admin对象
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            logger.warn("连接HBase的时候异常！", e);
        }
        return admin;
    }

    public static void close(Admin admin) {
        if(null != admin) {
            try {
                admin.close();
                admin.getConnection().close();
            } catch (IOException e) {
                logger.warn("关闭admin的时候异常!", e);
            }
        }
    }

    public static Table getTable(String user_info) {
        return null;
    }

    /**
     * 列举namespace
     */

    public void listNamesapce() throws IOException {
        //1. 获取admin对象
        Admin admin = HBaseUtils.getAdmin();
        //2. 获取namespace的所有描述器
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        //3. 遍历
        for (NamespaceDescriptor descriptor : namespaceDescriptors) {
            System.out.println(descriptor);
        }
        //4. 关闭
        HBaseUtils.close(admin);
    }

    public void listNamespaceTables() throws IOException {
        //1. 获取admin对象
        Admin admin = HBaseUtils.getAdmin();
        //2. 获取name所有的表名
        TableName[] tableNames = admin.listTableNamesByNamespace("ns1");
        //3. 遍历
        for(TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
        }
        //4. 关闭
        HBaseUtils.close(admin);
    }

    public void listAllNamespaceTables() throws IOException {
        //1. 获取admin对象
        Admin admin = HBaseUtils.getAdmin();
        //2. 获取name所有的表名
        TableName[] tableNames = admin.listTableNames();
        //3. 遍历
        for(TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
        }
        //4. 关闭
        HBaseUtils.close(admin);
    }
    @Test
    public void listAllNamespaceTableDescriptor() throws IOException {
        //1. 获取admin对象
        Admin admin = HBaseUtils.getAdmin();
        //2. 获取name所有的表名
        HTableDescriptor[] tableDescriptors = admin.listTableDescriptorsByNamespace("ns1");
        //3. 遍历
        for(HTableDescriptor descriptor : tableDescriptors) {
            System.out.println(descriptor.getTableName());
        }
        //4. 关闭
        HBaseUtils.close(admin);
    }
    /**
     * 删除namespace
     * 只能删除空的namespace
     */
    @Test
    public void dropNamespace() throws IOException {
        //1. 获取admin对象
        Admin admin = HBaseUtils.getAdmin();
        //2. 删除
        admin.deleteNamespace("lee");
        //3. 关闭
        HBaseUtils.close(admin);
    }
}