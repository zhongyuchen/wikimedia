package main.java;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.commons.math3.analysis.function.Inverse;
import org.apache.directory.api.util.Position;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

//import org.wikiclean.WikiClean;
//import org.wikiclean.WikiCleanBuilder;

public class WordIndex {
    public static class WordIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
        // data types: input key, input value, output key, output value
        // output key and value
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // data types: input key, input value, context for writing results
            //System.out.println("#KEY#" + key.toString());
            //System.out.println("#VALUE#" + value.toString().split("\t", 2)[0]);
            int end = value.find("\t");
            StringBuffer word = new StringBuffer();
            for (int i = 0; i < end; i++)
                word.append((char)value.charAt(i));
            //String word = value.toString().split("\t", 2)[0];

            keyInfo.set(word.toString());
            String name = ((FileSplit)context.getInputSplit()).getPath().getName();
            valueInfo.set(name + "," + key.toString());
            context.write(keyInfo, valueInfo);
        }
    }

    public static void main(String[] args) throws Exception {
        // id, title + contributor(username, id) + timestamp + (text_start, text_length)

        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf, WordIndex.class.getName());
        job.setJarByClass(WordIndex.class);
        job.setNumReduceTasks(15);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(WordIndexMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}