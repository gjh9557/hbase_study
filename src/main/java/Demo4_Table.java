import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;

public class Demo4_Table {
    Admin admin=HBaseUtils.getAdmin();
    @After
    public void after(){
        HBaseUtils.close(admin);
    }
    /**
     * 建表：create 't2' ,{NAME=>'fault', VERSIONS=>1}
     * */
//    @Test
    public void createTable() throws IOException {
        //创建表描述器
        HTableDescriptor tableDescriptor=new HTableDescriptor(TableName.valueOf("user_info"));
    //创建列出表述
        HColumnDescriptor columnDescriptor=new HColumnDescriptor("abase_info");
        //设置列族版本从1到5
        columnDescriptor.setMinVersions(1);
        columnDescriptor.setMaxVersions(5);
        columnDescriptor.setTimeToLive(24*60*60);//秒为单位
        columnDescriptor.setBloomFilterType(BloomType.ROW);
        HColumnDescriptor columnDescriptor1=new HColumnDescriptor("extra_info");
        columnDescriptor1.setMaxVersions(5);
        columnDescriptor1.setMinVersions(1);
        columnDescriptor1.setTimeToLive(24*3600);
        tableDescriptor.addFamily(columnDescriptor);
        tableDescriptor.addFamily(columnDescriptor1);
        admin.createTable(tableDescriptor);
    }
    //@Test
    //增加列族
    public void modifyTable() throws IOException {
        TableName tableName =TableName.valueOf("user_info");
        HTableDescriptor tableDescriptor=new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor=new HColumnDescriptor("lixi_info");

        columnDescriptor.setTimeToLive(24*3600);
        columnDescriptor.setMinVersions(1);
        columnDescriptor.setMaxVersions(5);
        tableDescriptor.addFamily(columnDescriptor);
        admin.modifyTable(tableName,tableDescriptor);
    }
    //增加列族
//    @Test
    public void modifyTable2() throws IOException {
        //1. 创建表描述器
        TableName tableName = TableName.valueOf("user_info");
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        //2. 创建列簇表述qi
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("abase_info");
        //2.1 设置列簇版本从1到5
        columnDescriptor.setMaxVersions(5);
        columnDescriptor.setMinVersions(1);
        columnDescriptor.setTimeToLive(24*60*60); // 秒为单位
        HColumnDescriptor columnDescriptor1 = new HColumnDescriptor("ex_info");
        //2.1 设置列簇版本从1到5
        columnDescriptor.setMaxVersions(5);
        columnDescriptor.setMinVersions(1);
        columnDescriptor.setTimeToLive(24*60*60); // 秒为单位
        //2.2 将列簇添加到表中
        tableDescriptor.addFamily(columnDescriptor);
        tableDescriptor.addFamily(columnDescriptor1);
        //3. 提交
        admin.modifyTable(tableName, tableDescriptor);
    }
    //修改表删除列族
//    @Test
    public void deleteColumnFamily() throws IOException {
        TableName tableName=TableName.valueOf("user_info");
        admin.deleteColumn(tableName,"lixi_info".getBytes());
    }
//    @Test
    public void deletecolumns() throws IOException {
        TableName tableName=TableName.valueOf("user_info");
        HTableDescriptor tableDescriptor=admin.getTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor=tableDescriptor.removeFamily("base_info".getBytes());
        admin.modifyTable(tableName,tableDescriptor);
    }
//    列举出某表的所有列族一些属性
//@Test
    public void listcolumns() throws IOException {
        TableName tableName=TableName.valueOf("user_info");
        HTableDescriptor tableDescriptor=admin.getTableDescriptor(tableName);
        HColumnDescriptor[] columnDescriptor=tableDescriptor.getColumnFamilies();
        for(HColumnDescriptor hColumnDescriptor:columnDescriptor){
            System.out.println(hColumnDescriptor.getNameAsString());
            System.out.println(hColumnDescriptor.getBlocksize());
            System.out.println(hColumnDescriptor.getBloomFilterType());
        }
}
@Test
    public void deletetable() throws IOException {
        TableName tableName=TableName.valueOf("ns1:user_info");
        if(admin.tableExists(tableName)){
            if(!admin.isTableDisabled(tableName)){
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
        }
}
}
