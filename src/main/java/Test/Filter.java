package Test;

import Utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

public class Filter {
    //select * from user_info where age <=18 and name=gjh;
//    @Test
    public void listFilter() throws IOException {
//        1.����������
//    and ����ʹ��MUST_PASS_ALL��Ϊ����
//            or ����ʹ�� MUST_PASS_ONE
        FilterList filterList=new FilterList(FilterList.Operator.MUST_PASS_ALL);
        FilterList fileterLIst1=new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        2:������ѯ����
//    ���ڵ���ֵ�Ƚ���ʹ��singlecolumnvaluefilter
//            columnfamily,qualifier,�ȽϹ����������ڣ�С�ڣ����ڣ�
        SingleColumnValueFilter ageFilter=new SingleColumnValueFilter(Bytes.toBytes("abase_info"),Bytes.toBytes("age"),CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes("18"));


        SingleColumnValueFilter nameFilter=new SingleColumnValueFilter(Bytes.toBytes("abase_info"),Bytes.toBytes("name"),CompareFilter.CompareOp.EQUAL,Bytes.toBytes("hah"));
        SingleColumnValueFilter ageFilter1=new SingleColumnValueFilter(Bytes.toBytes("abase_info"),Bytes.toBytes("age"),CompareFilter.CompareOp.EQUAL,Bytes.toBytes("gjh"));
        ageFilter1.setFilterIfMissing(true);
        ageFilter.setFilterIfMissing(true);
        nameFilter.setFilterIfMissing(true);
       //���������뵽��������
        filterList.addFilter(ageFilter);
        filterList.addFilter(nameFilter);
        fileterLIst1.addFilter(filterList);
        fileterLIst1.addFilter(ageFilter1);
        HBaseUtils.showFilterResult(fileterLIst1);
////        4������ɨ��������ɨ��
//        Scan scan =new Scan();
////        5������������������ɨ����
//        scan.setFilter(fileterLIst1);
////        6����ȡ�����
//        Table table =HBaseUtils.getTable();
////        7��ɨ���
//        ResultScanner scanner=table.getScanner(scan);
////        8����ӡ����
//        Iterator<Result> iterator=scanner.iterator();
//        while(iterator.hasNext()){
//            Result rs=iterator.next();
//            HBaseUtils.show(rs);
//        }
    }
//seletc * form user_info where name like '%i%';
    @Test
    public void regexFilter(){
//        1�������Ƚ�����������wΪ��ͷ��
        RegexStringComparator regexStringComparator=new RegexStringComparator("^w");
//    2����ȡ����ֵ������
    SingleColumnValueFilter namefilter=new SingleColumnValueFilter(Bytes.toBytes("abase_info"),Bytes.toBytes("name"),CompareFilter.CompareOp.EQUAL,regexStringComparator);
    namefilter.setFilterIfMissing(true);
    HBaseUtils.showFilterResult(namefilter);
    }
}
