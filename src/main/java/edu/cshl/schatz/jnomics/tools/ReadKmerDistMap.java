package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.util.Nucleotide;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.sound.midi.SysexMessage;
import java.io.*;


/**
 * Takes kmers from reads and lookups up the global count in kmer_db.
 * Takes a specific number of occurances and counts them.
 * ex. if occ is 1, emits how many kmers from that read occ.
 */
public class ReadKmerDistMap extends JnomicsMapper<ReadCollectionWritable, NullWritable, LongWritable, LongWritable> {

    private final JnomicsArgument ksize_arg = new JnomicsArgument("kmer_size", "KmerAndCount size for counting", true);
    private final JnomicsArgument kmer_db_arg = new JnomicsArgument("kmer_db", "Kmer db for looking up counts", true);
    private final JnomicsArgument kmer_lookup_bin_arg = new JnomicsArgument("kmerlookup_bin",
            "Kmerlookup binary", true);
    private final JnomicsArgument kmer_occ_arg = new JnomicsArgument("kmer_occ",
            "kmer count that you want to see a distribution for", true);
    
    private final LongWritable one = new LongWritable(1);
    private final LongWritable readCount = new LongWritable();
    private Process klookup;
    private int ksize;
    private long kmer_occ;
    private String cmd;
    
    //private BufferedWriter out;
    private OutputStream out;
    //private PrintWriter out;
    private BufferedReader in;
    private Thread errorWriter;
    
    @Override
    public Class getOutputKeyClass() {
        return LongWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{ksize_arg,kmer_db_arg,kmer_lookup_bin_arg,kmer_occ_arg};
    }

    @Override
    public Class<? extends Reducer> getCombinerClass(){
        return KCounterHistReduce.class;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        ksize = conf.getInt(ksize_arg.getName(), -1);
        if(ksize < 1 )
            throw new InterruptedException("Bad KmerAndCount Size");
        kmer_occ = conf.getLong(kmer_occ_arg.getName(),-1);
        if(kmer_occ < 0)
            throw new InterruptedIOException("Bad kmerOcc size");

        String kmerlookup_bin = conf.get(kmer_lookup_bin_arg.getName());
        if(!new File(kmerlookup_bin).exists())
            throw new IOException("Cannot find kmerlookup binary");
        String kmer_db = conf.get(kmer_db_arg.getName());
        File kmerdbFile = new File(kmer_db);
        if(!kmerdbFile.exists())
            throw new IOException("Cannot find kmerdb");
        if(kmerdbFile.exists()){
            System.out.println("Found KmerDb at : "+ kmerdbFile.getAbsolutePath());
        }
        cmd = String.format("%s %s",kmerlookup_bin,kmer_db);
        System.out.println("Running cmd: "+ cmd);
        ProcessBuilder pb = new ProcessBuilder(kmerlookup_bin,kmer_db);
        klookup = pb.start();
        out = klookup.getOutputStream();
        in = new BufferedReader(new InputStreamReader(klookup.getInputStream()));
        errorWriter = new Thread(new ThreadedStreamConnector(klookup.getErrorStream(), System.err));
        errorWriter.start();
    }

    @Override
    protected void map(ReadCollectionWritable key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        
        String []arr;
        long count;
        long read_count;
        String forward,reverse,lower;
        String line;
        for( ReadWritable read: key.getReads() ){
            String readSeq = read.getSequence().toString();
            read_count = 0;
            for(int i=0; i< readSeq.length() - ksize +1 ; i++){
                forward = readSeq.substring(i,ksize+i);
                reverse = Nucleotide.reverseComplement(forward);
                lower = forward.compareTo(reverse) < 0 ? forward : reverse;
                out.write(lower.getBytes(),0,ksize);
                out.write("\n".getBytes());
                out.flush();
                line = in.readLine();
                if(null == line){
                    throw new IOException("Could not read from kmerlookup");
                }
                System.out.println(line);
                arr= line.split("\t");
                count = Long.parseLong(arr[1]);
                if(-1 == count)
                    throw new IOException("Could not find kmer "+ readSeq.substring(i,i+ksize)+" in db");
                if(count == kmer_occ){
                    read_count += 1;
                }
            }
            readCount.set(read_count);
            context.write(readCount,one);
        } 
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Cleaning up");
        out.close();
        klookup.waitFor();
        System.out.println("exit value: " + klookup.exitValue());
        errorWriter.join();
    }
}

