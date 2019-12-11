import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 操作namespace
 */
public class Demo2_Namespace {

    private HBaseAdmin hBaseAdmin;

    @Before
    public void before() throws IOException {
        //1. 获取连接配置对象
        Configuration configuration = new Configuration();
        //2. 设置连接hbase的参数
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //3. 获取Admin对象
        hBaseAdmin = new HBaseAdmin(configuration);
    }

    @After
    public void close() throws IOException {
        hBaseAdmin.close();
    }
    /**
     * 创建namespace
     */
    @Test
    public void createNamespace() throws IOException {
        //1. 创建namespace对象
        NamespaceDescriptor descriptor = NamespaceDescriptor.create("lixi").build();
        //2. 提交hbase中创建对象
        hBaseAdmin.createNamespace(descriptor);
    }
    @Before
    public void before2() throws IOException {
        //1. 获取连接配置对象
        Configuration configuration = new Configuration();
        //2. 设置连接hbase的参数
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //3. 获取Admin对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        hBaseAdmin = (HBaseAdmin) connection.getAdmin();
    }
}