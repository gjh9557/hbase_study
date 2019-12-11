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
 * HBase Client������
 */
public class HBaseUtils {

    private static final Logger logger = Logger.getLogger(HBaseUtils.class);

    private final static String CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String CONNECT_VALUE = "hadoop01:2181,hadoop02:2181,hadoop03:2181";

    /**
     * ��ȡAdmin����
     */
    public static Admin getAdmin() {
        //1. ��ȡ�������ö���
        Configuration configuration = new Configuration();
        //2. ��������hbase�Ĳ���
        configuration.set(CONNECT_KEY, CONNECT_VALUE);
        //3. ��ȡAdmin����
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            logger.warn("����HBase��ʱ���쳣��", e);
        }
        return admin;
    }

    public static void close(Admin admin) {
        if(null != admin) {
            try {
                admin.close();
                admin.getConnection().close();
            } catch (IOException e) {
                logger.warn("�ر�admin��ʱ���쳣!", e);
            }
        }
    }

    public static Table getTable(String user_info) {
        return null;
    }

    /**
     * �о�namespace
     */

    public void listNamesapce() throws IOException {
        //1. ��ȡadmin����
        Admin admin = HBaseUtils.getAdmin();
        //2. ��ȡnamespace������������
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        //3. ����
        for (NamespaceDescriptor descriptor : namespaceDescriptors) {
            System.out.println(descriptor);
        }
        //4. �ر�
        HBaseUtils.close(admin);
    }

    public void listNamespaceTables() throws IOException {
        //1. ��ȡadmin����
        Admin admin = HBaseUtils.getAdmin();
        //2. ��ȡname���еı���
        TableName[] tableNames = admin.listTableNamesByNamespace("ns1");
        //3. ����
        for(TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
        }
        //4. �ر�
        HBaseUtils.close(admin);
    }

    public void listAllNamespaceTables() throws IOException {
        //1. ��ȡadmin����
        Admin admin = HBaseUtils.getAdmin();
        //2. ��ȡname���еı���
        TableName[] tableNames = admin.listTableNames();
        //3. ����
        for(TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
        }
        //4. �ر�
        HBaseUtils.close(admin);
    }
    @Test
    public void listAllNamespaceTableDescriptor() throws IOException {
        //1. ��ȡadmin����
        Admin admin = HBaseUtils.getAdmin();
        //2. ��ȡname���еı���
        HTableDescriptor[] tableDescriptors = admin.listTableDescriptorsByNamespace("ns1");
        //3. ����
        for(HTableDescriptor descriptor : tableDescriptors) {
            System.out.println(descriptor.getTableName());
        }
        //4. �ر�
        HBaseUtils.close(admin);
    }
    /**
     * ɾ��namespace
     * ֻ��ɾ���յ�namespace
     */
    @Test
    public void dropNamespace() throws IOException {
        //1. ��ȡadmin����
        Admin admin = HBaseUtils.getAdmin();
        //2. ɾ��
        admin.deleteNamespace("lee");
        //3. �ر�
        HBaseUtils.close(admin);
    }
}