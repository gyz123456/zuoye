package com.zuoye.hadoop.mr.sort;

import com.zuoye.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  要求：MapReduce 程序读取这三个文件，对三个文件中的数字进行整体升序排序，并输出到一个结果文件中，
 *  结果文件中的每一行有两个数字（两个数字之间使⽤用制表符分隔)，第一个数字代表排名，第二个数字代表原始数据
 *
 *  步骤  1.map端将数值作为key输出
 *        2.reduce端拉取数据的时候会对key进行排序，
 *        3.reduce方法中取出的key已经按照从小到大排列好了，只需要拼接上他是第几位就可以了
 * */
public class MRSortDriver {

    public static void main(String[] args) throws Exception {

        String inputPath = "input/data/hadoop/mr/sortData/";
        String outputPath = "output/hadoop/mr/sortData";

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        FileUtils.deleteOutput(configuration, outputPath);
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(MRSortDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(MRSortMapper.class);
        job.setReducerClass(MRSortReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
