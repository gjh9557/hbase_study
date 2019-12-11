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
 * HBase Client工具类
 */
public class HBaseUtils {

    private static final Logger logger = Logger.getLogger(HBaseUtils.class);

    private final static String CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String CONNECT_VALUE = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private static Connection connection;

    static {
        //1. 获取连接配置对象
        Configuration configuration = HBaseConfiguration.create();
        //2. 设置连接hbase的参数
        configuration.set(CONNECT_KEY, CONNECT_VALUE);
        //3. 获取connection对象
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            logger.warn("连接HBase的时候异常！", e);
        }
    }

    /**
     * 获取Admin对象
     */
    public static Admin getAdmin() {
        Admin admin = null;
        try {
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
            } catch (IOException e) {
                logger.warn("关闭admin的时候异常!", e);
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
                logger.warn("获取表产生异常！", e);
            }
        }
        return table;
    }

    public static void close(Table table) {
        if(table != null) {
            try {
                table.close();
            } catch (IOException e) {
                logger.warn("关闭table的时候产生异常！", e);
            }
        }
    }
    public static  void show(Result rs) throws IOException {
        CellScanner cellScanner = rs.cellScanner();
        System.out.println("rowkey : " + new String(rs.getRow()));
        //4. 遍历
        while (cellScanner.advance()) {
            //5. 获取当前表格
            Cell cell = cellScanner.current();
            //5.1 获取所有的列簇
            System.out.print(new String(CellUtil.cloneFamily(cell), "utf-8")+":");
            System.out.print(new String(CellUtil.cloneQualifier(cell), "utf-8")+":");
            System.out.println(new String(CellUtil.cloneValue(cell), "utf-8"));
        }
    }
    public static void showFilterResult(Filter filter) {
        //4.创建扫描器进行扫描
        Scan scan = new Scan();
        //5. 设置过滤器
        scan.setFilter(filter);
        //6. 获取表对象
        Table table = HBaseUtils.getTable();
        //7. 扫描表
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            //8. 打印数据
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                HBaseUtils.show(result);
            }
        } catch (IOException e) {
            logger.warn("获取table的时候异常！", e);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.warn("关闭table的时候异常！", e);
            }
        }
    }
}
