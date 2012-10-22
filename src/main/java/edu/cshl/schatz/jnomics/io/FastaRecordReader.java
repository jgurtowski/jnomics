package edu.cshl.schatz.jnomics.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * @author james
 * Reads records from Fasta file on hdfs
 * The key will be the byte position of the Fasta entry within the file
 * The value will be the FastaRecord itself
 */

/*
public class FastaRecordReader extends JnomicsFileRecordReader {

	private static char newline = '\n';
	private static char recordDelimiter = '>';
	
	private BufferedByteReader fsin;
	private long splitEnd;
	private long splitStart;
	private long filelen;
	private byte buffer;
	
	
	private final ByteArrayOutputStream strBuffer= new ByteArrayOutputStream();
    private final QueryTemplate fastaRecord = new QueryTemplate();
	private final Text pos = new Text();
	private final SequencingRead read = new SequencingRead();
		
	public FastaRecordReader(){
		fastaRecord.add(read);
	}
	
	@Override
	public void close() throws IOException {
		fsin.close();
	}

	@Override
	public Text getCurrentKey() throws IOException,InterruptedException {
		return pos;
	}

	@Override
	public QueryTemplate getCurrentValue() throws IOException, InterruptedException {
		return fastaRecord;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		float prog = ((float)(fsin.getPos() - splitStart )) / (splitEnd - splitStart);
		if (prog > 1)
			return ((float)(fsin.getPos() - splitStart)) / (filelen - splitStart); 
		return prog;
	}

	@Override
	public void initialize(FileSplit split, Configuration conf)
			throws IOException, InterruptedException {
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		filelen = fs.getFileStatus(path).getLen();
		fsin = new BufferedByteReader(fs.open(path));  
		splitStart = split.getStart();
		splitEnd = splitStart + split.getLength();
		fsin.seek(splitStart);
	}
	

    //Reads an Entry from the split, will cross split boundaries if necessary

	private boolean readEntry() throws IOException{
		strBuffer.reset();
		//pos.set(Long.toString(fsin.getPos()));
		while((buffer = fsin.readByte()) != -1 
				&& buffer != newline ){
			strBuffer.write(buffer);
		}
		pos.set(strBuffer.toString()); //set key to name so templates are grouped
		fastaRecord.setTemplateName(strBuffer.toString());
		strBuffer.reset();
		while((buffer = fsin.readByte()) != -1
				&& buffer != recordDelimiter){
			if(buffer == newline)	
				continue;
			strBuffer.write(buffer);
		}
		
		SequencingRead r = fastaRecord.getEmptyRead();
		r.setSequence(strBuffer.toByteArray(),true);
		fastaRecord.set(r);
		fsin.previous();// go back one so that seekToEntry works
		return true;
	}
		
	
	private boolean seekToEntry() throws IOException{
		while((buffer=fsin.readByte()) != -1 && fsin.getPos() <= splitEnd){
			if(buffer == recordDelimiter)
				return true;
		}
		return false;
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return seekToEntry() && readEntry();
	}		
}
*/
