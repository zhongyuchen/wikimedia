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

public class Lossless {
    public static String slice(String doc, String rgex) {
        Pattern pattern = Pattern.compile(rgex);
        Matcher matcher = pattern.matcher(doc);
        while (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    public static ArrayList<String> sliceList(String doc, String rgex) {
        ArrayList<String> list = new ArrayList<String>();
        Pattern pattern = Pattern.compile(rgex);
        Matcher matcher = pattern.matcher(doc);
        while (matcher.find()) {
            list.add(matcher.group() + "-" + matcher.start());
        }
        return list;
    }

    // (word, id, tf) writable comparable
    public static class TripletWritable implements WritableComparable<TripletWritable> {
        private Text word;
        private LongWritable id, tf;
        public TripletWritable() {
            this.word = new Text();
            this.id = new LongWritable();
            this.tf = new LongWritable();
        }
        public TripletWritable(Text word, LongWritable id, LongWritable tf) {
            this.word = word;
            this.id = id;
            this.tf = tf;
        }
        public Text getWord() {
            return this.word;
        }
        public LongWritable getID() {
            return this.id;
        }
        public LongWritable getTF() {
            return this.tf;
        }
        public void set(TripletWritable other) {
            this.word = other.getWord();
            this.id = other.getID();
            this.tf = other.getTF();
        }

        // deserialize!
        @Override
        public void readFields(DataInput in) throws IOException {
            this.word.readFields(in);
            this.id.readFields(in);
            this.tf.readFields(in);
        }
        // serialize!
        @Override
        public void write(DataOutput out) throws IOException {
            this.word.write(out);
            this.id.write(out);
            this.tf.write(out);
        }

        @Override
        public String toString() {
            return this.word.toString() + "-" + this.id.toString() + "-" + this.tf.toString();
        }

        // compare!
        @Override
        public int compareTo(TripletWritable other) {
            if (this.getWord().equals(other.getWord())) {
                if (this.getTF().equals(other.getTF()))
                    return this.getID().compareTo(other.getID());
                return this.getTF().compareTo(other.getTF()) * -1;
            }
            return this.getWord().compareTo(other.getWord());
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof TripletWritable)) {
                return false;
            }
            TripletWritable other = (TripletWritable) obj;
            return this.getWord().equals(other.getWord()) && this.getID().equals(other.getID()) && this.getTF().equals(other.getTF());
        }

        @Override
        public int hashCode() {
            return this.word.hashCode();
        }
    }

    // grouping rule
    public static class TripletComparator extends WritableComparator{
        protected TripletComparator() {
            super(TripletWritable.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TripletWritable tw1 = (TripletWritable) a;
            TripletWritable tw2 = (TripletWritable) b;
            // group by word
            return tw1.getWord().compareTo(tw2.getWord());
        }
    }

    public static class LosslessMapper extends Mapper<LongWritable, Text, TripletWritable, Text>{
        // data types: input key, input value, output key, output value
        // output key and value
        private TripletWritable keyInfo = new TripletWritable();
        private Text valueInfo = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // data types: input key, input value, context for writing results
            //System.out.println("#MAP INPUT KEY#" + key.toString());
//            System.out.println("#MAP INPUT VALUE#" + value.toString());
            if (key == null || value == null)
                return;
            String page = value.toString().toLowerCase();
            if (page.length() == 0)
                return;

            String idString = slice(page, "<id>(.*?)</id>");
            if (idString == null || idString.length() == 0)
                return;
            LongWritable id = new LongWritable(Long.parseLong(idString));
//            String title = slice(page, "<title></title>");
            // text的提取有问题！！！
            //page = page.replaceAll("[^(A-Za-z)]", " ");
            int start = page.indexOf("<text");
            int end = page.indexOf("</text>");
            if (start == -1 || end == -1)
                return;
            start = page.indexOf('>', start);
            if (start == -1)
                return;
            if (start + 1 >= end)
                return;
            page = page.substring(start + 1, end);

            //System.out.println("#"+page+"#");
/*
            String text = slice(page, "<text[\\s\\S]*>([\\s\\S]*)</text>");

            //System.out.println("#MAP INPUT VALUE#" + text);
            //if (text == null || text.length() == 0)
            //   return;
            ArrayList<String> words = sliceList(text, "[a-zA-Z]+");
            //String[] words = page.split(" ");
            if (words.size() == 0)
                return;

*/
            ArrayList<String> words = sliceList(page, "[a-zA-Z]+");

            HashMap<String, ArrayList<Long>> wordPos = new HashMap<String, ArrayList<Long>>();

            for (String w: words) {
                String[] split = w.split("-", 2);
                if (split.length == 2) {
                    if (!wordPos.containsKey(split[0])) {
                        wordPos.put(split[0], new ArrayList<Long>());
                    }
                    wordPos.get(split[0]).add(Long.parseLong(split[1]));
                }
            }

            for (String w: wordPos.keySet()) {
                keyInfo.set(new TripletWritable(new Text(w), id, new LongWritable(wordPos.get(w).size())));
                StringBuffer posList = new StringBuffer();
                Long posPast = 0L;
                Long posDiff;
                for (Long position: wordPos.get(w)) {
                    posDiff = position - posPast;
                    posList.append(posDiff);
                    posList.append(',');
                    //posList += posDiff.toString() + ",";
                    posPast = position;
                }
                posList.insert(0, '-');
                posList.insert(0, wordPos.get(w).size());
                posList.insert(0, '-');
                posList.insert(0, id);
                //valueInfo.set(id.toString() + "-" + wordPos.get(w).size() + "-" + posList.substring(0, posList.length() - 1));
                valueInfo.set(posList.substring(0, posList.length() - 1));
                // key: word, id, tf
                // value: id + tf + list(position)
                //System.out.println("#MAP OUTPUT KEY#" + keyInfo.toString());
                //System.out.println("#MAP OUTPUT VALUE#" + valueInfo.toString());
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class LosslessReducer extends Reducer<TripletWritable, Text, Text, Text> {
        // data types: input key and value from mapper, output key, output value
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();

        public void reduce(TripletWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key: the same word
            // values: list(id + tf + list(position))
            //System.out.println("#REDUCE INPUT KEY#" + key.toString());

            if (key == null || key.toString().length() == 0 || values == null)
                return;
            //String fileList = "";
            StringBuffer fileList = new StringBuffer();
            Long length = 0L;
            Long idPast = 0L;
            Long idDiff;
            Long tfPast = 0L;
            for(Text value : values) {
                //System.out.println("#REDUCE INPUT VALUE#" + value.toString());
                String[] split = value.toString().split("-", 3);
                Long id = Long.parseLong(split[0]);
                Long tf = Long.parseLong(split[1]);

                length = length + 1L;
                if (tf.equals(tfPast)) {
                    idDiff = id - idPast;
                    fileList.append(idDiff.toString());
                    fileList.append('-');
                    fileList.append(tf.toString());
                    fileList.append(split[2]);
                    fileList.append(';');
                    //fileList += idDiff.toString() + "-" + tf.toString() + "-" + split[2] + ";";
                }
                else {
                    fileList.append(value);
                    fileList.append(';');
                    //fileList += value + ";";
                }

                idPast = id;
                tfPast = tf;
            }
            if (length > 0) {
                keyInfo.set(key.getWord());
                fileList.insert(0, ';');
                fileList.insert(0, length.toString());
                valueInfo.set(fileList.toString());
                // key: word
                // value: df + list(id + tf + list(position))
                //System.out.println("#REDUCE OUTPUT KEY#" + keyInfo.toString());
                //System.out.println("#REDUCE OUTPUT KEY#" + valueInfo.toString());
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // word, df + list(id + tf + list(position))
        // LongWritable tf, id is sorted (in a segment with the same tf, the difference of id is stored)
        // LongWritable position is sorted (store the difference of position)

        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf, Lossless.class.getName());
        job.setJarByClass(Lossless.class);
        //job.setNumReduceTasks(15);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(LosslessMapper.class);
        job.setMapOutputKeyClass(TripletWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setGroupingComparatorClass(TripletComparator.class);
        job.setReducerClass(LosslessReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}