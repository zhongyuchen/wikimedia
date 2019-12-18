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

//import org.wikiclean.WikiClean;
//import org.wikiclean.WikiCleanBuilder;

public class InvertedIndex {
//	public class Tuple<String, Long> {
//		private String word;
//		private Long position;
//		public Tuple(String w, Long p) {
//			this.word = w;
//			this.position = p;
//		}
//		public Tuple() {
//			this.word = new String("");
//			this.position = new Long(-1L);
//		}
//	}

	public static String slice(String doc, String rgex) {
		Pattern pattern = Pattern.compile(rgex);
		Matcher matcher = pattern.matcher(doc);
//		Pair<Integer, String> pair = new Pair<>(1, "One");
//		Integer key = pair.getKey();
//		String value = pair.getValue();
		while (matcher.find()) {
			return matcher.group(1);
		}
		return "";
	}

	public static List<String> sliceList(String doc, String rgex) {
		List<String> list = new ArrayList<String>();
		Pattern pattern = Pattern.compile(rgex);
		Matcher matcher = pattern.matcher(doc);
		while (matcher.find()) {
//			list.add(matcher.group(), matcher.start());
			list.add(matcher.group());
		}
		return list;
	}

	// (word, n) writable
/*
	public class WordN implements WritableComparable<IDTF> {
		public Text word;
		public LongWritable n;
		public WordN(Text word, LongWritable n) {
			this.word = word;
			this.n = n;
		}
		public WordN() {
			this.word = new Text("");
			this.n = new LongWritable(0);
		}

		// deserialize!
		public void readFields(DataInput in) throws IOException {
			id = in.readLong();
			tf = in.readLong();
		}
		// serialize!
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(id);
			out.writeLong(tf);
		}

		public String toString() {
			return "X:"+Float.toString(x) + ", "
					+ "Y:"+Float.toString(y) + ", "
					+ "Z:"+Float.toString(z);
		}
		public float distanceFromOrigin() {
			return (float) Math.sqrt( x*x + y*y +z*z);
		}
		// compare!
		public int compareTo(Point3D other) {
			return Float.compare(
					distanceFromOrigin(),
					other.distanceFromOrigin());
		}
		public boolean equals(Object o) {
			if( !(o instanceof Point3D)) {
				return false;
			}
			Point3D other = (Point3D) o;
			return this.x == other.x && this.y == other.y && this.z == other.z;
		}
		// 实现 hashCode() 方法很重要
		// Hadoop的Partitioners会用到这个方法，后面再说

		public int hashCode() {
			return Float.floatToIntBits(x)
					^ Float.floatToIntBits(y)
					^ Float.floatToIntBits(z);
		}

	}
*/

	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
		// data types: input key, input value, output key, output value
		// output key and value
		private Text keyInfo = new Text();
		private static Text valueInfo = new Text("1");
//		private static LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// data types: input key, input value, context for writing results
			if (key == null || value == null)
				return;
			String page = value.toString().toLowerCase();
			if (page.length() == 0)
				return;

			String id = slice(page, "<id>(.*?)</id>");
//			String title = slice(page, "<title></title>");
			String text = slice(page, "<text.*>([\\s\\S]*)</text>");
			if (id == null || id.length() == 0 || text == null || text.length() == 0)
				return;
			List<String> words = sliceList(text, "[a-zA-Z]+");
			if (words.size() == 0)
				return;
/*
			HashMap<String, Long> wordCount = new HashMap<String, Long>();

			for (String w: words) {
				Long cnt = 1L;
				if (wordCount.containsKey(w)) {
					cnt = wordCount.get(w) + 1L;
				}
				wordCount.put(w, cnt);
			}

			for (String w: wordCount.keySet()) {
				keyInfo.set(w);
				valueInfo.set(id + "-" + wordCount.get(w).toString());
				context.write(keyInfo, valueInfo);
			}
*/

			for (String w: words) {
				if (w != null && w.length() != 0) {
					keyInfo.set(w + "-" + id);
					context.write(keyInfo, valueInfo);
				}
			}
		}
	}

	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
		// data types: input key and value from mapper, output key, output value
		// <Text, Text, Text, Text>
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// key: word + id
			// values: list of 1
			if (key == null || key.toString().length() == 0 || values == null)
				return;
			String[] split = key.toString().split("-", 2);
			if (split.length != 2)
				return;

			Long length = 0L;
			for(Text value : values) {
				length = length + 1L;
			}
			keyInfo.set(split[0]);
			valueInfo.set(split[1] + "-" + length.toString());
			// key: word
			// value: id + tf
			context.write(keyInfo, valueInfo);
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		// data types: input key and value from mapper, output key, output value
//		private Text keyInfo = new Text();
		private Text valueInfo = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// key: the same word
			// values: list(values with the same word as key)
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
				//keyInfo.set(key + "-" + length.toString());
				valueInfo.set(length.toString() + ";" + fileList);
				// key: word
				// value: df + list(id + tf)
				context.write(key, valueInfo);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// word, df + list(id + tf)

		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		Job job = Job.getInstance(conf, InvertedIndex.class.getName());
		job.setJarByClass(InvertedIndex.class);
//		job.setNumReduceTasks(15);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}