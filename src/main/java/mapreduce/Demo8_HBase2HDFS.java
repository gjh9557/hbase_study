package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Demo8_HBase2HDFS implements Tool {

    //1. �������ö���
    private Configuration configuration;
    private final static String HBASE_CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String HBASE_CONNECT_VALUE = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private final static String HDFS_CONNECT_KEY = "fs.defaultFS";
    private final static String HDFS_CONNECT_VALUE = "hdfs://hadoop01:9000";
    private final static String MAPREDUCE_CONNECT_KEY = "mapreduce.framework.name";
    private final static String MAPREDUCE_CONNECT_VALUE = "yarn";

    @Override
    public int run(String[] args) throws Exception {
        //1. ��ȡjob
        Job job = Job.getInstance(configuration, "hbase2hdfs");
        //2. ��������jar
        job.setJarByClass(Demo8_HBase2HDFS.class);
        /*
         * 3. ����TableMapper��ʼ����
         * ���ô�HBase���ж�ȡ������Ϊ����
         * ����tablename, ɨ����scan,mapper�࣬mapper���key���࣬mapper�����value�࣬jo
         */
//            job.setMapperClass(HBaseMapper.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(NullWritable.class);
        TableMapReduceUtil.initTableMapperJob("user_info", getScan(), HBaseMapper.class,
                Text.class, NullWritable.class, job);
        //4. ���������ʽ
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        /*
         * 5. ���ô�HBase���ж�ȡ������Ϊ����
         * ����tablename, ɨ����scan,mapper�࣬mapper���key���࣬mapper�����value�࣬job
         */
        //6. �ύ
        boolean b = job.waitForCompletion(true);
        return b ? 1 : 0;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set(HBASE_CONNECT_KEY, HBASE_CONNECT_VALUE); // �������ӵ�hbase
        conf.set(HDFS_CONNECT_KEY, HDFS_CONNECT_VALUE); // �������ӵ�hadoop
        conf.set(MAPREDUCE_CONNECT_KEY, MAPREDUCE_CONNECT_VALUE); // ����ʹ�õ�mr����ƽ̨
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    /**
     * һ�� �Զ���Mapper��
     * ��HBase�ж�ȡĳ�����ݣ�ns1:user_info
     * 1. ��ȡһ��
     * 003                                                column=base_info:age, timestamp=1546957041028, value=15
     * 003                                                column=base_info:name, timestamp=1546957041028, value=narudo
     * 003                                                column=base_info:sex, timestamp=1546957041028, value=male
     *
     *  2. ���
     *  age:15 name:narudo sex:male*
     *
     */
    public static class HBaseMapper extends TableMapper<Text, NullWritable> {

        private Text k = new Text();

        /**
         *
         * @param key ��
         * @param value : ���ظ���rowkey��һ�н��
         * @param context
         */

        protected void map(ImmutableBytesWritable key, Result value, Reducer.Context context) throws IOException, InterruptedException, IOException {
            //0. �����ַ���������ս��
            StringBuffer sb = new StringBuffer();
            //1. ��ȡɨ��������ɨ�����
            CellScanner cellScanner = value.cellScanner();
            //2. �ƽ�
            while (cellScanner.advance()) {
                //3. ��ȡ��ǰ��Ԫ��
                Cell cell = cellScanner.current();
                //4. ƴ���ַ���
                sb.append(new String(CellUtil.cloneQualifier(cell)));
                sb.append(":");
                sb.append(new String(CellUtil.cloneValue(cell)));
                sb.append("\t");
            }
            //5. д��
            k.set(sb.toString());
            context.write(k, NullWritable.get());

        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(HBaseConfiguration.create(), new Demo8_HBase2HDFS(), args);
    }
    private static Scan getScan() {
        return new Scan();
    }
}
