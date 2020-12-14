package utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import data.ItemSet;
import data.Transaction;

public class Funcs
{

    public static Transaction getTransaction(int id, String txnRecord) {
        String currLine = txnRecord.trim();
        String[] words = currLine.split("[\\s\\t]");
        Transaction transaction = new Transaction(id);

        for (int i = 0; i < words.length; i++) {
            transaction.add(Integer.parseInt(words[i].trim()));
        }

        return transaction;
    }

    public static List<ItemSet> getCandidateItemSets(List<ItemSet> prevPassItemSets, int itemSetSize) {
        List<ItemSet> candidateItemSets = new ArrayList<>();
        Collections.sort(prevPassItemSets);
        int prevPassItemSetsSize = prevPassItemSets.size();
        
        //for each line with size K, use the former K-1 element to generate a user-defined hashcode.
        //each time the adjacent elements have the same hashcode, 
        for(int i = 0; i < prevPassItemSetsSize; i++) {
        	ItemSet subItemSet_i = new ItemSet();
        	subItemSet_i.addAll(prevPassItemSets.get(i).subList(0, itemSetSize - 1));
        	for(int j = i + 1; j < prevPassItemSetsSize; j++) {
        		ItemSet subItemSet_j = new ItemSet();
            	subItemSet_j.addAll(prevPassItemSets.get(j).subList(0, itemSetSize - 1));
        		if(subItemSet_i.hashCode() == subItemSet_j.hashCode()) {
        			//System.out.println(" "+i+" "+j);
        			ItemSet newItemSet = new ItemSet();
        			if(itemSetSize > 1) newItemSet.addAll(prevPassItemSets.get(i).subList(0, itemSetSize - 1));
        			newItemSet.add(prevPassItemSets.get(i).get(itemSetSize - 1));
        			newItemSet.add(prevPassItemSets.get(j).get(itemSetSize - 1));
        			candidateItemSets.add(newItemSet);
        		} else {
        			break;
        		}
        	}
        }
        return candidateItemSets;
    }

    public static Map<Integer, ItemSet> generateItemSetMap(List<ItemSet> itemSets) {
        Map<Integer, ItemSet> itemSetMap = new HashMap<>();

        for (ItemSet itemSet : itemSets) {
            int hashCode = itemSet.hashCode();
            if (!itemSetMap.containsKey(hashCode)) {
                itemSetMap.put(hashCode, itemSet);
            }
        }
        return itemSetMap;
    }
}
