package com.zuoye.hadoop.mr.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MRSortMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    IntWritable k = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int data = Integer.parseInt(value.toString());
        k.set(data);
        context.write(k, NullWritable.get());
    }
}
