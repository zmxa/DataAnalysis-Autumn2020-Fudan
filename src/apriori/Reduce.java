package apriori;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Funcs;

import java.io.IOException;


public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
{
	@Override
    public void reduce(Text itemSet, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int minSup = Integer.parseInt(context.getConfiguration().get("minSup"));
        
        //sum up.
        int sum = 0;
        for(IntWritable val : values)
        	sum += val.get();
        
        //write 
        if(sum >= minSup) {        
	        IntWritable result = new IntWritable();
	        result.set(sum);
	        context.write(itemSet, result); 
        }
    }
}
