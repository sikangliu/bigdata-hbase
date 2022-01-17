package com.lsk.mr2;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2022-01-17 20:04
 */
public class WriteFruitMRFromTxtReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
            throws IOException, InterruptedException {
        //读出来的每一行数据写入到 fruit_hdfs 表中
        for (Put put : values) {
            context.write(NullWritable.get(), put);
        }
    }
}
