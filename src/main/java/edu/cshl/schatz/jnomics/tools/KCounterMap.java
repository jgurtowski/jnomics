package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.FixedKmerWritable;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.util.Nucleotide;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Takes reads from a ReadCollectionWritable and breaks them into kmers of size kmer_size
 */
public class KCounterMap extends JnomicsMapper<ReadCollectionWritable, NullWritable, FixedKmerWritable, LongWritable> {

    private final JnomicsArgument ksize_arg = new JnomicsArgument("kmer_size", "KmerAndCount size for counting", true);

    private final LongWritable one = new LongWritable(1);
    
    private FixedKmerWritable kmerWritable, revKmerWritable;

    
    @Override
    public Class getOutputKeyClass() {
        return FixedKmerWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{ksize_arg};
    }

    @Override
    public Class<? extends Reducer> getCombinerClass(){
        return KCounterReduce.class;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int ksize = Integer.parseInt(conf.get(ksize_arg.getName()));
        if(ksize < 1 )
            throw new InterruptedException("Bad KmerAndCount Size");
        kmerWritable = new FixedKmerWritable(ksize);
        revKmerWritable = new FixedKmerWritable(ksize);
    }

    @Override
    protected void map(ReadCollectionWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        byte[] seq_bytes, rev_seq_bytes;
        for( ReadWritable read: key.getReads()){
            seq_bytes = read.getSequence().toString().getBytes();
            for(int i=0; i< seq_bytes.length - kmerWritable.getKsize() + 1; i++){
                try {
                    kmerWritable.set(seq_bytes, i, i + kmerWritable.getKsize());
                    revKmerWritable.set(Nucleotide.reverseComplement(seq_bytes,i, i+kmerWritable.getKsize()));
                } catch (Exception e) {
                    throw new IOException(e);
                }
                context.write(kmerWritable.compareTo(revKmerWritable) < 0 ? kmerWritable : revKmerWritable, one);
            }
        }
    }
}

