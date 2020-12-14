package apriori;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.NewCookie;

import data.ItemSet;
import data.Transaction;
import data.Trie;
import utils.Funcs;

public class st {
	//Local Test Only!
	//Local Test Only!
	//Local Test Only!
	//Local Test Only!
	//Local Test Only!
	//Local Test Only!
	//Local Test Only!
	//Local Test Only!
	//Local Test Only!
	//Local Test Only!
	public static void main(String[] args) throws Exception {

	    //All results in Phase K-1.
	    List<ItemSet> itemSetsPrevPass = new ArrayList<>();
	    
	    //Results in Phase K.
	    List<ItemSet> candidateItemSets = null;
	    int passNum=3;
	    //Index Structure in Phase K.
	    Trie trie = null;
	    
		String lastPassOutputFile = "E:\\复旦小学的资料\\part-r-00000";	
        
        try {
            File fp = new File(lastPassOutputFile);
            BufferedReader p = new BufferedReader(new FileReader(fp));
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

        
        System.out.println("OK1!");
        //See annotation in file: utils/Funcs.java
        candidateItemSets = Funcs.getCandidateItemSets(itemSetsPrevPass, (passNum - 1));
//        File fout = new File("E:\\复旦小学的资料\\tri.txt");
//        PrintWriter output = new PrintWriter(fout);
//        for(ItemSet s:candidateItemSets) {
//        	output.println(s);
//        }
//        output.close();
        System.out.println("OK2!");
        
        trie = new Trie(passNum);

        int candidateItemSetsSize = candidateItemSets.size();
        for (int i = 0; i < candidateItemSetsSize; i++) {
            ItemSet itemSet = candidateItemSets.get(i);
            //System.out.println(itemSet);
            trie.add(itemSet);
        }
        
        
        File fp = new File("C:\\Users\\DELL6\\Desktop\\mul");
        BufferedReader p = new BufferedReader(new FileReader(fp));
        String currLine;
        ArrayList<ItemSet> empt = new ArrayList<ItemSet>();
        while((currLine = p.readLine()) != null) {
	        Transaction txn = Funcs.getTransaction(2, currLine);
	        Collections.sort(txn);
	        ArrayList<ItemSet> matchedItemSet = new ArrayList<>();
	        trie.findItemSets(matchedItemSet, txn);
	        if(!matchedItemSet.equals(empt)) {
	        	for(ItemSet itemSet : matchedItemSet) 
		        	System.out.println(itemSet);
		        }
	    }
    }
}

