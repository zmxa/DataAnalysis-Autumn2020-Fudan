package other;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Entry {
	static class CarIDTime {
		private boolean inSplit;
		
		private int car;
		
		private int time;
		
		CarIDTime(boolean inSplit, int car, int time) {
			this.inSplit = inSplit;
			this.car = car;
			this.time = time;
		}
		
		public boolean getInSplit() {
			return this.inSplit;
		}
		
		public int getCar() {
			return this.car;
		}
		
		public int getTime() {
			return this.time;
		}
		
		public void setInSplit(boolean inSplit) {
			this.inSplit = inSplit;
		}
		
		public void setCar(int car) {
			this.car = car;
		}
		
		public void setTime(int time) {
			this.time = time;
		}
	}
	
	static class TwoEntryMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private int deltaTime = 10;
		
		private int windowPosition = 0;
		
		private Queue<Entry.CarIDTime> window = new LinkedList<>();
		
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			boolean inSplit = (key.get() == 0L);
			String[] entry = value.toString().split("\t");
			int position = Integer.parseInt(entry[0]);
			int time = Integer.parseInt(entry[1]);
			int car = Integer.parseInt(entry[2]);
			if (this.window.size() == 0) {
				if (!inSplit)
					return; 
			} else if (position == this.windowPosition) {
				int count = 0;
				for (Entry.CarIDTime carIDTime : this.window) {
					if (time - carIDTime.getTime() <= this.deltaTime) {
						if (carIDTime.getCar() != car) {
							String outputValue = carIDTime.getCar() + "\t" + car;
							context.write(NullWritable.get(), new Text(outputValue));
						} 
						continue;
					} 
					count++;
				} 
				for (int i = 0; i < count; i++)
					this.window.remove(); 
			} else {
				this.window.clear();
			} 
			if (inSplit) {
				this.windowPosition = position;
				this.window.add(new Entry.CarIDTime(true, car, time));
			} 
		}
	}
	
	static class MultiEntryMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private int deltaTime = 10;
		
		private int windowPosition = 0;
		
		private Queue<Entry.CarIDTime> window = new LinkedList<>();
		
		int outSplitCount = 0;
		
		final int outSplitSize = 100;
		
		List<Integer> multiTuple = new ArrayList<>();
		
		Set<Integer> dwMultiTuple = new HashSet<>();
		
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			boolean inSplit = (key.get() == 0L);
			String[] entry = value.toString().split("\t");
			int position = Integer.parseInt(entry[0]);
			int time = Integer.parseInt(entry[1]);
			int car = Integer.parseInt(entry[2]);
			if (!inSplit)
				this.outSplitCount++; 
			if (this.window.size() != 0) {
				if (!((Entry.CarIDTime)this.window.peek()).getInSplit())
					return; 
				if (position == this.windowPosition) {
					if (time - ((Entry.CarIDTime)this.window.peek()).getTime() > this.deltaTime)
						for (Entry.CarIDTime i : this.window)
							this.multiTuple.add(Integer.valueOf(i.getCar()));  
					while (this.window.peek() != null && time - ((Entry.CarIDTime)this.window.peek()).getTime() > this.deltaTime)
						this.window.poll(); 
				} else {
					while (this.window.peek() != null)
						this.multiTuple.add(Integer.valueOf(((Entry.CarIDTime)this.window.poll()).getCar())); 
				} 
				this.dwMultiTuple.addAll(this.multiTuple);
				if (this.dwMultiTuple.size() > 1) {
					StringBuilder outputValue = new StringBuilder();
					Iterator<Integer> iter = this.dwMultiTuple.iterator();
					while (true) {
						outputValue.append(iter.next());
						if (iter.hasNext()) {
							outputValue.append("\t");
							continue;
						} 
						break;
					} 
					context.write(NullWritable.get(), new Text(String.valueOf(outputValue)));
				} 
				this.multiTuple.clear();
				this.dwMultiTuple.clear();
			} 
			this.windowPosition = position;
			this.window.add(new Entry.CarIDTime(inSplit, car, time));
			if (this.outSplitCount >= 100) {
				if (!((Entry.CarIDTime)this.window.peek()).getInSplit())
					return; 
				while (this.window.peek() != null)
					this.multiTuple.add(Integer.valueOf(((Entry.CarIDTime)this.window.poll()).getCar())); 
				this.dwMultiTuple.addAll(this.multiTuple);
				if (this.dwMultiTuple.size() > 1) {
					StringBuilder outputValue = new StringBuilder();
					Iterator<Integer> iter = this.dwMultiTuple.iterator();
					while (true) {
						outputValue.append(iter.next());
						if (iter.hasNext()) {
							outputValue.append("\t");
							continue;
						} 
						break;
					} 
					context.write(NullWritable.get(), new Text(String.valueOf(outputValue)));
				} 
				this.multiTuple.clear();
				this.dwMultiTuple.clear();
			} 
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("");
			System.exit(2);
		} 
		Job job = new Job(conf, "MultiEntryTuple");
		job.setJarByClass(Entry.class);
		job.setInputFormatClass(EntryFormat.class);
		job.setMapperClass(MultiEntryMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
