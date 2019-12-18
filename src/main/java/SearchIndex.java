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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

//import org.wikiclean.WikiClean;
//import org.wikiclean.WikiCleanBuilder;

public class SearchIndex {
    public static String slice(String doc, String rgex) {
        Pattern pattern = Pattern.compile(rgex);
        Matcher matcher = pattern.matcher(doc);
        while (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    public static long slicePos(String doc, String rgex) {
        Pattern pattern = Pattern.compile(rgex);
        Matcher matcher = pattern.matcher(doc);
        while (matcher.find()) {
            return (long) matcher.start();
        }
        return 0L;
    }


    public static class SearchIndexMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
        // data types: input key, input value, output key, output value
        // output key and value
        private LongWritable keyInfo = new LongWritable();
        private Text valueInfo = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // data types: input key, input value, context for writing results
            if (key == null || value == null)
                return;
            int start, end;

            // page
            String page = value.toString().toLowerCase();
            if (page.length() == 0)
                return;

            // id
            String idString = slice(page, "<id>(.*?)</id>");
            if (idString == null || idString.length() == 0)
                return;
            LongWritable id = new LongWritable(Long.parseLong(idString));

            // title
            String title = slice(page, "<title>(.*?)</title>");

            // contributor
            start = page.indexOf("<contributor>");
            end = page.indexOf("</contributor>");
            if (start == -1 || end == -1)
                return;
            start += 13;
            String contri_id = "";
            String contri_name = "";
            //String contributor = slice(page, "<contributor>([\\s\\S]*)</contributor>");
            if (start < end) {
                String contributor = page.substring(start, end);
                contri_name = slice(contributor, "<username>(.*?)</username>");
                contri_id = slice(contributor, "<id>(.*?)</id>");
            }

            // time
            String time = slice(page, "<timestamp>(.*?)</timestamp>");

            // text
            start = page.indexOf("<text");
            end = page.indexOf("</text>");
            if (start == -1 || end == -1)
                return;
            start = page.indexOf('>', start);
            if (start == -1)
                return;
            int textLength = end - (start + 1);
            if (textLength <= 0)
                return;
            LongWritable textStart = new LongWritable(key.get() + start + 1);


            keyInfo.set(id.get());
            valueInfo.set(title + "," + contri_name + "," + contri_id + "," + time + "," + textStart + "," + textLength);
            context.write(keyInfo, valueInfo);
        }
    }

    public static void main(String[] args) throws Exception {
        // id, title + contributor(username, id) + timestamp + (text_start, text_length)

        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf, SearchIndex.class.getName());
        job.setJarByClass(SearchIndex.class);
        job.setNumReduceTasks(15);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(SearchIndexMapper.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}