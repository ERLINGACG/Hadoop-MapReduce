package com.javaweb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhoneCount {
    public static class PhoneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text phone = new Text();
        private IntWritable traffic = new IntWritable(); // 用于存储流量

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 处理输入数据
            String line = value.toString();
            String[] fields = line.split("\t");
            if (fields.length > 8) { // 确保有足够的列
                phone.set(fields[1]); // 假设第二列是手机号码
                // 假设第8列是入流量，第9列是出流量，将二者相加
                int incomingTraffic = Integer.parseInt(fields[8]); // 假设第8列是入流量
                int outgoingTraffic = Integer.parseInt(fields[9]); // 假设第9列是出流量
                int totalTraffic = incomingTraffic + outgoingTraffic;
                traffic.set(totalTraffic); // 设置流量
                context.write(phone, traffic); // 输出手机号码和总流量
            }
        }
    }

    public static class PhoneReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get(); // 汇总流量
            }
            result.set(sum);
            context.write(key, result); // 输出手机号码和汇总流量
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "phone traffic count");
        job.setJarByClass(PhoneCount.class);
        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
