package edu.cshl.schatz.jnomics.io;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsCounter;
import edu.cshl.schatz.jnomics.ob.AlignmentCollectionWritable;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InputStream;

/**
 * User: james
 */
public class AlignmentReaderContextWriter{

    private boolean pairedEnd;
    private final AlignmentCollectionWritable alignmentCollection = new AlignmentCollectionWritable();

    public AlignmentReaderContextWriter(boolean pairedEnd){
        this.pairedEnd = pairedEnd;
        System.out.println("PairedEnd ? :" + pairedEnd);
        alignmentCollection.addAlignment(new SAMRecordWritable());
        if(pairedEnd)
            alignmentCollection.addAlignment(new SAMRecordWritable());

        System.out.println("Collectionsize: " + alignmentCollection.size());
    }

    public void read(InputStream inputStream, Mapper.Context context)
            throws IOException, InterruptedException {
        SAMFileReader reader = new SAMFileReader(inputStream);

        reader.setValidationStringency(SAMFileReader.ValidationStringency.LENIENT);
        Counter mapped_counter = context.getCounter(JnomicsCounter.Alignment.MAPPED);
        Counter totalreads_counter = context.getCounter(JnomicsCounter.Alignment.TOTAL);
        int i = 0;
        for(SAMRecord record: reader){
            alignmentCollection.getAlignment(i).set(record);

            totalreads_counter.increment(1);
            if(2 == (2 & record.getFlags()))
                mapped_counter.increment(1);
            if((!pairedEnd) || (i >= 1) ){
                context.write(alignmentCollection, NullWritable.get());
                context.progress();
                i=0;
            }else{
                i++;
            }
        }
        reader.close();
    }
}