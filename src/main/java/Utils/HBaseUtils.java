package Utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;


/**
 * HBase Client������
 */
public class HBaseUtils {

    private static final Logger logger = Logger.getLogger(HBaseUtils.class);

    private final static String CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String CONNECT_VALUE = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private static Connection connection;

    static {
        //1. ��ȡ�������ö���
        Configuration configuration = HBaseConfiguration.create();
        //2. ��������hbase�Ĳ���
        configuration.set(CONNECT_KEY, CONNECT_VALUE);
        //3. ��ȡconnection����
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            logger.warn("����HBase��ʱ���쳣��", e);
        }
    }

    /**
     * ��ȡAdmin����
     */
    public static Admin getAdmin() {
        Admin admin = null;
        try {
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
            } catch (IOException e) {
                logger.warn("�ر�admin��ʱ���쳣!", e);
            }
        }
    }

    public static Table getTable() {
        return getTable("user_info");
    }

    public static Table getTable(String tablename) {
        Table table = null;
        if(StringUtils.isNotEmpty(tablename)) {
            try {
                table = connection.getTable(TableName.valueOf(tablename));
            } catch (IOException e) {
                logger.warn("��ȡ������쳣��", e);
            }
        }
        return table;
    }

    public static void close(Table table) {
        if(table != null) {
            try {
                table.close();
            } catch (IOException e) {
                logger.warn("�ر�table��ʱ������쳣��", e);
            }
        }
    }
    public static  void show(Result rs) throws IOException {
        CellScanner cellScanner = rs.cellScanner();
        System.out.println("rowkey : " + new String(rs.getRow()));
        //4. ����
        while (cellScanner.advance()) {
            //5. ��ȡ��ǰ���
            Cell cell = cellScanner.current();
            //5.1 ��ȡ���е��д�
            System.out.print(new String(CellUtil.cloneFamily(cell), "utf-8")+":");
            System.out.print(new String(CellUtil.cloneQualifier(cell), "utf-8")+":");
            System.out.println(new String(CellUtil.cloneValue(cell), "utf-8"));
        }
    }
    public static void showFilterResult(Filter filter) {
        //4.����ɨ��������ɨ��
        Scan scan = new Scan();
        //5. ���ù�����
        scan.setFilter(filter);
        //6. ��ȡ�����
        Table table = HBaseUtils.getTable();
        //7. ɨ���
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            //8. ��ӡ����
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                HBaseUtils.show(result);
            }
        } catch (IOException e) {
            logger.warn("��ȡtable��ʱ���쳣��", e);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.warn("�ر�table��ʱ���쳣��", e);
            }
        }
    }
}
