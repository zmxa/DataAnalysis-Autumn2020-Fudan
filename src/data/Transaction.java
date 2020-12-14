package data;

import java.util.ArrayList;
import java.util.Collections;

//Used to save any sequence.
public class Transaction extends ArrayList<Integer>
{
	//Redundant Integer remains.
	int tid;

	public Transaction(int tid) {
		this.tid = tid;
	}

	@Override
	public String toString() {
		return "Transaction [tid=" + tid + ",items=" + super.toString() + "]";
	}

	@Override
	public boolean add(Integer integer) {
        super.add(integer);
		Collections.sort(this);
		return true;
	}
}
