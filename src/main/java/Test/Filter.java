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
//        1.创建过滤器
//    and 条件使用MUST_PASS_ALL作为条件
//            or 条件使用 MUST_PASS_ONE
        FilterList filterList=new FilterList(FilterList.Operator.MUST_PASS_ALL);
        FilterList fileterLIst1=new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        2:创建查询条件
//    对于单列值比较器使用singlecolumnvaluefilter
//            columnfamily,qualifier,比较过滤器（大于，小于，等于）
        SingleColumnValueFilter ageFilter=new SingleColumnValueFilter(Bytes.toBytes("abase_info"),Bytes.toBytes("age"),CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes("18"));


        SingleColumnValueFilter nameFilter=new SingleColumnValueFilter(Bytes.toBytes("abase_info"),Bytes.toBytes("name"),CompareFilter.CompareOp.EQUAL,Bytes.toBytes("hah"));
        SingleColumnValueFilter ageFilter1=new SingleColumnValueFilter(Bytes.toBytes("abase_info"),Bytes.toBytes("age"),CompareFilter.CompareOp.EQUAL,Bytes.toBytes("gjh"));
        ageFilter1.setFilterIfMissing(true);
        ageFilter.setFilterIfMissing(true);
        nameFilter.setFilterIfMissing(true);
       //将条件加入到过滤器中
        filterList.addFilter(ageFilter);
        filterList.addFilter(nameFilter);
        fileterLIst1.addFilter(filterList);
        fileterLIst1.addFilter(ageFilter1);
        HBaseUtils.showFilterResult(fileterLIst1);
////        4：创建扫描器进行扫描
//        Scan scan =new Scan();
////        5：将过滤条件关联到扫描器
//        scan.setFilter(fileterLIst1);
////        6：获取表对象
//        Table table =HBaseUtils.getTable();
////        7：扫描表
//        ResultScanner scanner=table.getScanner(scan);
////        8：打印数据
//        Iterator<Result> iterator=scanner.iterator();
//        while(iterator.hasNext()){
//            Result rs=iterator.next();
//            HBaseUtils.show(rs);
//        }
    }
//seletc * form user_info where name like '%i%';
    @Test
    public void regexFilter(){
//        1：创建比较器，正则以w为开头的
        RegexStringComparator regexStringComparator=new RegexStringComparator("^w");
//    2：获取单列值过滤器
    SingleColumnValueFilter namefilter=new SingleColumnValueFilter(Bytes.toBytes("abase_info"),Bytes.toBytes("name"),CompareFilter.CompareOp.EQUAL,regexStringComparator);
    namefilter.setFilterIfMissing(true);
    HBaseUtils.showFilterResult(namefilter);
    }
}
