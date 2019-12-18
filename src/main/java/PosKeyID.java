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

public class PosKeyID {
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

    // (word, id) writable comparable
	public static class WordID implements WritableComparable<WordID> {
		private Text word;
		private LongWritable id;
        public WordID() {
            this.word = new Text();
            this.id = new LongWritable();
        }
		public WordID(Text word, LongWritable id) {
			this.word = word;
			this.id = id;
		}
		public Text getWord() {
            return this.word;
        }
        public LongWritable getID() {
            return this.id;
        }
        public void set(WordID other) {
            this.word = other.getWord();
            this.id = other.getID();
        }

		// deserialize!
        @Override
		public void readFields(DataInput in) throws IOException {
			this.word.readFields(in);
			this.id.readFields(in);
		}
		// serialize!
		@Override
		public void write(DataOutput out) throws IOException {
			this.word.write(out);
			this.id.write(out);
		}

		@Override
		public String toString() {
			return this.word.toString() + "-" + this.id.toString();
		}

		// compare!
        @Override
		public int compareTo(WordID other) {
            if (this.getWord().equals(other.getWord())) {
                return this.getID().compareTo(other.getID());
            }
			return this.getWord().compareTo(other.getWord());
		}

		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof WordID)) {
				return false;
			}
			WordID other = (WordID) obj;
			return this.getWord().equals(other.getWord()) && this.getID().equals(other.getID());
		}

        @Override
		public int hashCode() {
            return this.word.hashCode();
		}
	}

	// grouping rule
    public static class WordIDGroup extends WritableComparator{
        protected WordIDGroup() {
            super(WordID.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            WordID wordid1 = (WordID) a;
            WordID wordid2 = (WordID) b;
            // group by word
            return wordid1.getWord().compareTo(wordid2.getWord());
        }
    }

    public static class PosKeyIDMapper extends Mapper<LongWritable, Text, WordID, Text>{
        // data types: input key, input value, output key, output value
        // output key and value
        private WordID keyInfo = new WordID();
        private Text valueInfo = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // data types: input key, input value, context for writing results
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
            String text = slice(page, "<text.*>([\\s\\S]*)</text>");
            if (text == null || text.length() == 0)
                return;
            ArrayList<String> words = sliceList(text, "[a-zA-Z]+");
            if (words.size() == 0)
                return;

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
                keyInfo.set(new WordID(new Text(w), id));
                String posList = "";
                for (Long position: wordPos.get(w)) {
                    posList += position.toString() + ",";
                }
                valueInfo.set(id.toString() + "-" + wordPos.get(w).size() + "-" + posList.substring(0, posList.length() - 1));
                // key: word, id
                // value: id + tf + list(position)
                context.write(keyInfo, valueInfo);
            }
        }
    }
/*
    public static class PosKeyIDCombiner extends Reducer<WordID, LongWritable, WordID, Text> {
        // data types: input key and value from mapper, output key, output value
        // <Text, Text, Text, Text>
        private WordID keyInfo = new WordID();
        private Text valueInfo = new Text();

        public void reduce(WordID key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // key: word + id
            // values: list of 1
            if (key == null || key.toString().length() == 0 || values == null)
                return;
            Long length = 0L;
            String posList = "";
            for(LongWritable value : values) {
                length = length + 1L;
                posList += value.toString() + ",";
            }
            // key是这个时候拆还是在reducer里面才拆开？
            keyInfo.set(key);
            valueInfo.set(key.getID().toString() + "-" + length.toString() + "-" + posList.substring(0, posList.length() - 1));
            // key: word + id
            // value: id + tf + list(position)
            context.write(keyInfo, valueInfo);
        }
    }
*/
    public static class PosKeyIDReducer extends Reducer<WordID, Text, Text, Text> {
        // data types: input key and value from mapper, output key, output value
		private Text keyInfo = new Text();
        private Text valueInfo = new Text();

        public void reduce(WordID key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key: word, id
            // values: list(id + tf + list(position))
            if (key == null || key.toString().length() == 0 || values == null)
                return;
            String fileList = "";
            Long length = 0L;
            for(Text value : values) {
                if (value != null && value.toString().length() != 0) {
                    length = length + 1L;
                    fileList += value.toString() + ";";
                }
            }
            if (length > 0) {
                keyInfo.set(key.getWord());
                valueInfo.set(length.toString() + ";" + fileList);
                // key: word
                // value: df + list(id + tf + list(position))
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // word, df + list(id + tf + list(position))
        // LongWritable id is sorted
        // LongWritable position is sorted

        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(conf, PosKeyID.class.getName());
        job.setJarByClass(PosKeyID.class);
		job.setNumReduceTasks(15);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(PosKeyIDMapper.class);
        job.setMapOutputKeyClass(WordID.class);
        job.setMapOutputValueClass(Text.class);

        job.setGroupingComparatorClass(WordIDGroup.class);
        job.setReducerClass(PosKeyIDReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}