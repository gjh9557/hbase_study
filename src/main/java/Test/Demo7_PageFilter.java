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
     * ���Է�ҳ��ʾuser_info���е��������ݣ���ҳ��ʾΪ3�м�¼
     */
    @Test
    public void testPageFilter() throws IOException {
        //1. ������ҳ��������������ÿҳ��ʾ3����¼
        PageFilter pageFilter = new PageFilter(3);
        //2. ����ɨ����
        Scan scan = new Scan();
        //3. ��ɨ�������ù�����
        scan.setFilter(pageFilter);
        //4. ��ȡ��Ĺ�����
        Table table = HBaseUtils.getTable();
        //5. ������ʾ
        String maxKey = ""; // ���keyֵ��¼��
        while(true) {
            int count = 0; // ������
            //6. ��ȡ�ṹɨ����
            ResultScanner scanner = table.getScanner(scan);
            //7. ��ȡ����������
            Iterator<Result> iterator = scanner.iterator();
            //8. ����
            while (iterator.hasNext()) {
                Result result = iterator.next();
                System.out.println(new String(result.getRow()));
                count++;
                maxKey = Bytes.toString(result.getRow());
                //9. ��ӡ
                HBaseUtils.show(result);
            }
            System.out.println("------------------------------------");
            //10. �ж��Ƿ���Խ���
            if (count < 3) break;

            //11. ������һ�ο�ʼ��ѯ���м���
            scan.setStartRow(Bytes.toBytes(maxKey + "\001"));
        }
    }
}