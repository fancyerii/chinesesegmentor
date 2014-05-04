package com.antbrains.crf.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyKey implements WritableComparable{
	public MyKey(){
		
	}
	public MyKey(int id, double score){
		this.id=id;
		this.score=score;
	}
	public int id;
	public double score;
	@Override
	public void readFields(DataInput in) throws IOException {
		id=in.readInt();
		score=in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeDouble(score);
	}

	@Override
	public int compareTo(Object o) {
		MyKey other=(MyKey)o;
		if(score<other.score){
			return 1;
		}else if(score>other.score){
			return -1;
		}
		if(this.id<other.id) return -1;
		return 1;
	}

}
