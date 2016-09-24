package org.hy.hadoop.mission8.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 继承WritableComparator
 * @author andy
 *
 */
public class GroupingComparator extends WritableComparator{
	
	protected GroupingComparator(){
		super(IntPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IntPair ip1 = (IntPair)w1;
		IntPair ip2 = (IntPair)w2;
		int l = ip1.getFirst();
		int r = ip2.getFirst();
		return l==r?0:(1<r?-1:1);
	}

}
