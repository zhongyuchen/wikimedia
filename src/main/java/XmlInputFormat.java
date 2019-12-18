package main.java;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class XmlInputFormat extends TextInputFormat {
    // 1. create record reader
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) {
        try {
            return new XmlRecordReader(inputSplit, context.getConfiguration());
        } catch (IOException e) {
            return null;
        }
    }

    // 2. is splitable?
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return super.isSplitable(context, file);
    }

    // 3. record reader
    public class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private long start, end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private byte[] startTag, endTag;
        private LongWritable currentKey;
        private Text currentValue;
        public static final String START_TAG = "xmlinput.start";
        public static final String END_TAG = "xmlinput.end";

        public XmlRecordReader() {}
        public XmlRecordReader(InputSplit inputSplit, Configuration context) throws IOException {
            // start/end tag
            startTag = context.get(START_TAG).getBytes("UTF-8");
            endTag = context.get(END_TAG).getBytes("UTF-8");
            // the start and end of the split
            FileSplit fileSplit = (FileSplit) inputSplit;
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();
            // input stream
            FileSystem fs = file.getFileSystem(context);
            fsin = fs.open(fileSplit.getPath());
            // start
            fsin.seek(start);
        }

        // close file inpute stream
        @Override
        public void close() throws IOException {
            fsin.close();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }

        // progress
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return fsin.getPos() - start / (float) end - start;
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {}

        // new key-value
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            currentKey = new LongWritable();
            currentValue = new Text();
            return next(currentKey, currentValue);
        }

        private boolean next(LongWritable key, Text value) throws IOException {
            if( fsin.getPos() < end && readUntilMatch(startTag, false)) {
                // start tag found!
                // write start tag into buffer
                buffer.write(startTag);
                try {
                    // try to match end tag, while writing into buffer
                    if(readUntilMatch(endTag, true)) {
                        // end tag found!
                        // the relative position -> key
                        // xml segment in buffer -> value
                        key.set(fsin.getPos() - buffer.getLength());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
            return false;
        }

        // try to match start/end tag
        private boolean readUntilMatch(byte[] tag, boolean isWrite) throws IOException {
            int i = 0;
            while(true) {
                // read 1 byte
                int b = fsin.read();
                if( b == -1) {
                    return false;
                }
                // write bytes while trying to match
                if(isWrite) {
                    buffer.write(b);
                }
                // match start/end tag
                if(b == tag[i]) {
                    i ++;
                    if( i >= tag.length) {
                        return true;
                    }
                } else {
                    i = 0;
                }
                // match?
                if (!isWrite && i == 0 && fsin.getPos() >= end) {
                    return false;
                }
            }
        }
    }
}