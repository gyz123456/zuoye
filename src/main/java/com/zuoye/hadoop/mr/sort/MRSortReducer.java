package com.zuoye.hadoop.mr.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MRSortReducer extends Reducer<IntWritable, NullWritable, Text, NullWritable> {

    int i = 0;
    Text k = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //拼接新的key
        i += 1;
        String newKey = i + "\t" + key.get();
        k.set(newKey);

        context.write(k, NullWritable.get());
    }
}
