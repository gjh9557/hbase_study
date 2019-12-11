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
    //����һ������
    public void putData() throws IOException {
//        1:��ȡtable����
        Table table =HBaseUtils.getTable();
//        2����ȡput����
    Put put =new Put(Bytes.toBytes("003"));
//    3:���ò��������
    //���壬������value
        put.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"),Bytes.toBytes("wt"));
        put.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("age"),Bytes.toBytes("gjh"));
        put.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("sex"),Bytes.toBytes("male"));
        table.put(put);
    }
    //������������
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
    //get��ѯ����
//    @Test
    public void getData(Get get) throws IOException {
//        1:���get����
//        Get get=new Get(Bytes.toBytes("003"));
    //2:ͨ��table��ȡ�������
        Result result=table.get(get);
        //3:��ȡ���ɨ����
        CellScanner cellScannable=result.cellScanner();
        System.out.println("rowkey:"+ result.getRow());
        //����
        while(cellScannable.advance()){
            //��ȡ��ǰ���
            Cell cell =cellScannable.current();
            //��ȡ��������
            byte[] familyArray=cell.getFamilyArray();
            System.out.println(new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
            //��ȡ������
            byte[] qualifierArray=cell.getQualifierArray();
            System.out.println(new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength()));
        //��ȡ���е�ֵ
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
//        1:�������ϴ洢get����
        List<Get> gets=new ArrayList<Get>();
//        2���������get����
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
    //ɨ��ָ���ı�
    @Test
    public void scanTable() throws IOException {
//        1������ɨ����
    Scan scan=new Scan();
    scan.setStartRow(Bytes.toBytes("001"));
    //���ɨ���������ͷ������β
    scan.setStopRow(Bytes.toBytes("005"+"\001")); //��ͷ����β�����������԰���β��
//���ɨ�����
    scan.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"));
    //��ȡɨ����
        ResultScanner scanner=table.getScanner(scan);
        Iterator<Result> it=scanner.iterator();
        while(it.hasNext()){
            Result result=it.next();
            HBaseUtils.show(result);
        }
    }
    //ɾ������
    @Test
    public void deleteData() throws IOException {
        List<Delete> deletes=new ArrayList<Delete>();
        Delete del=new Delete(Bytes.toBytes("005"));
        del.addColumn(Bytes.toBytes("abase_info"),Bytes.toBytes("name"));
        deletes.add(del);
        table.delete(deletes);
    }
}
