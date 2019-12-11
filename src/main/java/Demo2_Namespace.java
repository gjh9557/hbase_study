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
 * ����namespace
 */
public class Demo2_Namespace {

    private HBaseAdmin hBaseAdmin;

    @Before
    public void before() throws IOException {
        //1. ��ȡ�������ö���
        Configuration configuration = new Configuration();
        //2. ��������hbase�Ĳ���
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //3. ��ȡAdmin����
        hBaseAdmin = new HBaseAdmin(configuration);
    }

    @After
    public void close() throws IOException {
        hBaseAdmin.close();
    }
    /**
     * ����namespace
     */
    @Test
    public void createNamespace() throws IOException {
        //1. ����namespace����
        NamespaceDescriptor descriptor = NamespaceDescriptor.create("lixi").build();
        //2. �ύhbase�д�������
        hBaseAdmin.createNamespace(descriptor);
    }
    @Before
    public void before2() throws IOException {
        //1. ��ȡ�������ö���
        Configuration configuration = new Configuration();
        //2. ��������hbase�Ĳ���
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //3. ��ȡAdmin����
        Connection connection = ConnectionFactory.createConnection(configuration);
        hBaseAdmin = (HBaseAdmin) connection.getAdmin();
    }
}