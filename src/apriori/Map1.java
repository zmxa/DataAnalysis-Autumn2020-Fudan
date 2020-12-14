package apriori;

import data.Transaction;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Funcs;

import java.io.IOException;

public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>
{
    final static IntWritable one = new IntWritable(1);
    Text item = new Text();

    @Override
    public void map(LongWritable key, Text txnRecord, Context context)
            throws IOException, InterruptedException {
    	//Get one line, read the line, then output.
        Transaction txn = Funcs.getTransaction((int) key.get(), txnRecord.toString());
        
        for(Integer val : txn) { 
        	item.set("[" + val + "]"); 
        	context.write(item, one); 
        }
    }
}
