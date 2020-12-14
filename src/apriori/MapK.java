package apriori;

import data.ItemSet;
import data.Transaction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Funcs;
import data.Trie;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//Now it's in phase K.
public class MapK extends Mapper<LongWritable, Text, Text, IntWritable>
{
    static IntWritable one = new IntWritable(1);
    Text item = new Text();

    //All results in Phase K-1.
    List<ItemSet> itemSetsPrevPass = new ArrayList<>();
    
    //Results in Phase K.
    List<ItemSet> candidateItemSets = null;
    
    //Index Structure in Phase K.
    Trie trie = null;
    
    //empty one.
    ArrayList<ItemSet> empt = new ArrayList<>();
    @Override
    public void setup(Context context) throws IOException {
    	//Consider this as K.
        int passNum = context.getConfiguration().getInt("passNum", 2);
        
        //result file in Phase K-1.
        String lastPassOutputFile = "a/output" + (passNum - 1) + "/part-r-00000";	
        
        try {
        	//Read from HDFS.
        	Path path = new Path(lastPassOutputFile);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader p = new BufferedReader(new InputStreamReader(fs.open(path)));
            String currLine;
            while ((currLine = p.readLine()) != null) {
                currLine = currLine.replace("[", "");
                currLine = currLine.replace("]", "");
                String[] words = currLine.split("[\\s\\t]+");
                if (words.length < 2) {
                    continue;
                }

                String finalWord = words[words.length - 1];
                int support = Integer.parseInt(finalWord);
                ItemSet itemSet = new ItemSet(support);
                //Make {itemSetsPrevPass}
                for (int k = 0; k < words.length - 1; k++) {
                    String csvItemIds = words[k];
                    String[] itemIds = csvItemIds.split(",");
                    for (String itemId : itemIds) {
                        itemSet.add(Integer.parseInt(itemId));
                    }
                }
                if(itemSet.size() ==(passNum-1))
                	itemSetsPrevPass.add(itemSet);
            }
        }
        catch (Exception e) {
        	System.out.println(e);
        }
        //See annotation in file: utils/Funcs.java
        candidateItemSets = Funcs.getCandidateItemSets(itemSetsPrevPass, (passNum - 1));
        trie = new Trie(passNum);

        int candidateItemSetsSize = candidateItemSets.size();
        for (int i = 0; i < candidateItemSetsSize; i++) {
            ItemSet itemSet = candidateItemSets.get(i);
            trie.add(itemSet);
        }
        
    }
    
    @Override
    public void map(LongWritable key, Text txnRecord, Context context)
            throws IOException, InterruptedException {
        Transaction txn = Funcs.getTransaction((int) key.get(), txnRecord.toString());
        Collections.sort(txn);
        
        //Input sorted transaction, get all pairs from Trie.
        ArrayList<ItemSet> matchedItemSet = new ArrayList<>();
        trie.findItemSets(matchedItemSet, txn);
        if(!matchedItemSet.equals(empt)) {
	        for(ItemSet itemSet : matchedItemSet) { 
	        	item.set(itemSet.toString());
	        	context.write(item, one);
	        }
        }
    }
}
