package Test;

import Utils.HBaseUtils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static Utils.HBaseUtils.show;

/**
 * haha
 */

public class Demoput {
    Table table =HBaseUtils.getTable();
    @After
    public void after(){
        HBaseUtils.close(table);
    }
//    @Test
    //插入一条数据
    public void putData() throws IOException {
//        1:获取table对象
        Table table =HBaseUtils.getTable();
//        2：获取put对象
    Put put =new Put(Bytes.toBytes("003"));
//    3:设置插入的数据
    //列族，列名，value
        put.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"),Bytes.toBytes("wt"));
        put.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("age"),Bytes.toBytes("gjh"));
        put.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("sex"),Bytes.toBytes("male"));
        table.put(put);
    }
    //批量插入数据
//     @Test
    public void batchputdatas() throws IOException {
         List<Put> list=new ArrayList<Put>();
         Put rk004=new Put(Bytes.toBytes("004"));
         Put rk005=new Put(Bytes.toBytes("005"));
         Put rk006=new Put(Bytes.toBytes("006"));
    rk004.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"),Bytes.toBytes("wl"));
    rk005.addColumn(Bytes.toBytes("ex_info"),Bytes.toBytes("age"),Bytes.toBytes("12"));
    rk005.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"),Bytes.toBytes("wt1"));
    rk006.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"),Bytes.toBytes("hah"));
    rk006.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("age"),Bytes.toBytes("13"));
    list.add(rk004);
    list.add(rk005);
    list.add(rk006);
    table.put(list);
    }
    //get查询数据
//    @Test
    public void getData(Get get) throws IOException {
//        1:获得get对象
//        Get get=new Get(Bytes.toBytes("003"));
    //2:通过table获取结果对象
        Result result=table.get(get);
        //3:获取表格扫描器
        CellScanner cellScannable=result.cellScanner();
        System.out.println("rowkey:"+ result.getRow());
        //遍历
        while(cellScannable.advance()){
            //获取当前表格
            Cell cell =cellScannable.current();
            //获取所有列族
            byte[] familyArray=cell.getFamilyArray();
            System.out.println(new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
            //获取所有列
            byte[] qualifierArray=cell.getQualifierArray();
            System.out.println(new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength()));
        //获取所有的值
            byte[] valueArray =cell.getValueArray();
            System.out.println(new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
        }
    }
//@Test
public void getdata() throws IOException {
        Get get=new Get(Bytes.toBytes("005"));
        Result rs=table.get(get);
        show(rs);
}
//    @Test
    public void batchGetdata() throws IOException {
//        1:创建集合存储get对象
        List<Get> gets=new ArrayList<Get>();
//        2：创建多个get对象
        Get get=new Get(Bytes.toBytes("004"));
        get.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"));

        Get get1=new Get(Bytes.toBytes("005"));
        get1.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"));
//        get.addColumn(Bytes.toBytes("ex_info"),Bytes.toBytes("age"));

        gets.add(get);
        gets.add(get1);

        Result[] results=table.get(gets);
        for(Result rs:results){
show(rs);
        }
    }
    //扫描指定的表
    @Test
    public void scanTable() throws IOException {
//        1：创建扫描器
    Scan scan=new Scan();
    scan.setStartRow(Bytes.toBytes("001"));
    //添加扫描的行数包头不包括尾
    scan.setStopRow(Bytes.toBytes("005"+"\001")); //包头不包尾部，这样可以包括尾部
//添加扫描的列
    scan.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"));
    //获取扫描器
        ResultScanner scanner=table.getScanner(scan);
        Iterator<Result> it=scanner.iterator();
        while(it.hasNext()){
            Result result=it.next();
            HBaseUtils.show(result);
        }
    }
    //删除数据
    @Test
    public void deleteData() throws IOException {
        List<Delete> deletes=new ArrayList<Delete>();
        Delete del=new Delete(Bytes.toBytes("005"));
        del.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"));
        deletes.add(del);
        table.delete(deletes);
    }
}
