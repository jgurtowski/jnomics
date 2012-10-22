package edu.cshl.schatz.jnomics.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author james
 * A FileInputFormat for reading Fasta Files
 * Gives the mapper a record view of the Fasta data
 * General Hadoop Note: You will get one mapper per fasta
 * entry. Large entries (ie. hg19) are inefficient because only
 * one Mapper is run per chromosome. Consider splitting large
 * records like this into smaller entries that can be processed
 * independently.
 * @return FastaRecordReader
 */
public class FastaInputFormat extends FileInputFormat<Writable, Writable> {

	@Override
	public RecordReader<Writable, Writable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
            
            return null;
            //return new FastaRecordReader();
	}
	
	
	
	/**
	 * Use for paired end reads in two fasta files the reducer will be fed
	 * reads with the same prefix (not include '/1' or '/2'
	 * Designed for use with Illumina read naming scheme
	 * @author james
	 *
	 */
	public static class PairedEndMap extends Mapper<LongWritable, FastaRecord, 
            Text, FastaRecord>{

		private final Text newKey = new Text(); 
		
		@Override
		protected void map(LongWritable key, FastaRecord record, Context context)
				throws IOException, InterruptedException {
			String name = record.getName().toString();
			int len = name.length();
			if(len > 1 && (name.charAt(len - 2) == '/' || name.charAt(len - 2) == '#')){
				newKey.set(name.substring(0,len-2));
			}else{
				newKey.set(name);
			}
			context.write(newKey, record);
		}
				
	}
	
}
