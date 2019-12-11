import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * ���ӵ�HBase�ķ���
 */
public class Demo1_Conn {
    public static void main(String[] args) throws IOException {
        //1. ��ȡ�������ö���
        Configuration configuration = new Configuration();
        //2. ��������hbase�Ĳ���
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //3. ��ȡAdmin����
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        //4. ����ָ�����Ƿ���ڣ����ж��Ƿ����ӵ�hbase
        boolean flag = hBaseAdmin.tableExists("ns1:aaa");
        //5. ��ӡ
        System.out.println(flag);
    }

}