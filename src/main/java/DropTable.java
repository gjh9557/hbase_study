import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

public class DropTable {
    public static void main(String[] args) throws IOException {
        Admin admin =HBaseUtils.getAdmin();
    if(admin.tableExists(TableName.valueOf("user_info"))){
        admin.truncateTable(TableName.valueOf("user_info"),true);//必须先禁用，在清空
        if(!admin.isTableDisabled(TableName.valueOf("user_info"))){
            admin.disableTable(TableName.valueOf("user_info"));
        }
        admin.deleteTable(TableName.valueOf("user_info"));


    }
    }
}
