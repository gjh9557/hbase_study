package Test;

import Utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

public class Demo7_PageFilter {

    /**
     * 测试分页显示user_info表中的所有数据，分页显示为3行记录
     */
    @Test
    public void testPageFilter() throws IOException {
        //1. 创建分页过滤器，并设置每页显示3条记录
        PageFilter pageFilter = new PageFilter(3);
        //2. 构造扫描器
        Scan scan = new Scan();
        //3. 给扫描器设置过滤器
        scan.setFilter(pageFilter);
        //4. 获取表的管理器
        Table table = HBaseUtils.getTable();
        //5. 遍历显示
        String maxKey = ""; // 最大key值记录器
        while(true) {
            int count = 0; // 计算器
            //6. 获取结构扫描器
            ResultScanner scanner = table.getScanner(scan);
            //7. 获取迭代器迭代
            Iterator<Result> iterator = scanner.iterator();
            //8. 迭代
            while (iterator.hasNext()) {
                Result result = iterator.next();
                System.out.println(new String(result.getRow()));
                count++;
                maxKey = Bytes.toString(result.getRow());
                //9. 打印
                HBaseUtils.show(result);
            }
            System.out.println("------------------------------------");
            //10. 判断是否可以结束
            if (count < 3) break;

            //11. 设置下一次开始查询的行键号
            scan.setStartRow(Bytes.toBytes(maxKey + "\001"));
        }
    }
}