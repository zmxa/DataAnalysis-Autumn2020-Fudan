package apriori;

import data.Transaction;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Funcs;

import java.io.IOException;

public class Map2 extends Mapper<LongWritable, Text, Text, IntWritable>
{
    final static IntWritable one = new IntWritable(1);
    Text item = new Text();

    @Override
    public void map(LongWritable key, Text txnRecord, Context context)
            throws IOException, InterruptedException {
    	
    	//Get the transaction, n^2 traversal, output all the pairs.
        Transaction txn = Funcs.getTransaction((int) key.get(), txnRecord.toString());
        int size = txn.size();
        for(int i=0;i<size;i++) { 
        	for(int j=i+1;j<size;j++)
        	item.set("[" + txn.get(i)+", "+txn.get(j) + "]"); 
        	context.write(item, one); 
        }
    }
}
