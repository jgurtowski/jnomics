package edu.cshl.schatz.jnomics.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author james
 * A FastaRecord, backed by Text (Writables)
 */
public class FastaRecord implements WritableComparable<FastaRecord> {

	private final Text _name = new Text(); 
	private final Text _sequence = new Text();
	
	public void set(String name, String sequence){
		_sequence.set(sequence);
		_name.set(name);
	}
	
	public void set(Text name, Text sequence){
		_name.set(name);
		_sequence.set(sequence);
	}	
	
	public static FastaRecord newInstance(FastaRecord o){
		FastaRecord n = new FastaRecord();
		n.set(o.getName(),o.getSequence());
		return n;
	}
	
	public void setSequence(byte[] arr){
		_sequence.set(arr);
	}
	
	public void setName(Text name){
		_name.set(name);	
	}
	
	public void setName(byte[] arr, int start, int len){
		_name.set(arr,start,len);
	}
	
	public void setName(byte[] arr){
		_name.set(arr);		
	}
	
	public void readFields(DataInput in) throws IOException {
		_name.readFields(in);
		_sequence.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		_name.write(out);
		_sequence.write(out);
	}

	
	public Text getName() {
		return _name;
	}

	public Text getSequence() {
		return _sequence;
	}
	
	public String toString(){
		String linechar = System.getProperty("line.separator"); 
		return ">" + _name.toString() + linechar + _sequence.toString();
	}

	public int compareTo(FastaRecord r){
		int t;
		if((t = _name.compareTo(r.getName())) == 0)
			return _sequence.compareTo(r.getSequence());
		return t;
	}
	
	@Override
	public int hashCode(){
		return _name.hashCode() ^ _sequence.hashCode();
	}
	
	@Override
	public boolean equals(Object o){
		FastaRecord r = (FastaRecord)o; 
		return _name.equals(r.getName()) && _sequence.equals(r.getSequence());
	}
	
}
