package other;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;

public class EntryFormat extends TextInputFormat {
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new EntryRecordReader();
  }
  
  public static class EntryRecordReader extends RecordReader<LongWritable, Text> {
    private long start;
    
    private long end;
    
    private FSDataInputStream fsin;
    
    private LongWritable key = new LongWritable();
    
    private Text value = new Text();
    
    private FileSystem fs;
    
    private FileSplit fileSplit;
    
    private LineReader linereader;
    
    private int count;
    
    private long pos;
    
    private long fileLength;
    
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      this.fileSplit = (FileSplit)inputSplit;
      this.start = this.fileSplit.getStart();
      this.end = this.start + this.fileSplit.getLength();
      Path file = this.fileSplit.getPath();
      this.pos = this.start;
      this.fs = file.getFileSystem(taskAttemptContext.getConfiguration());
      this.fsin = this.fs.open(this.fileSplit.getPath());
      this.fileLength = this.fs.getLength(this.fileSplit.getPath());
      this.fsin.seek(this.start);
      this.linereader = new LineReader((InputStream)this.fsin);
      this.count = 0;
      if (this.start != 0L)
        this.pos += this.linereader.readLine(new Text()); 
    }
    
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (this.pos >= this.fileLength)
        return false; 
      if (this.count < 100) {
        if (this.pos >= this.end)
          this.count++; 
        this.pos += this.linereader.readLine(this.value);
        this.key.set((this.count == 0) ? 0L : 1L);
        return true;
      } 
      return false;
    }
    
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return this.key;
    }
    
    public Text getCurrentValue() throws IOException, InterruptedException {
      return this.value;
    }
    
    public float getProgress() throws IOException, InterruptedException {
      return (float)(this.fsin.getPos() - this.start) / (float)(this.end - this.start);
    }
    
    public void close() throws IOException {
      this.fsin.close();
    }
  }
}
