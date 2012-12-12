package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.FastaParser;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.mapreduce.PacbioCorrectorCounter;
import edu.cshl.schatz.jnomics.util.Command;
import edu.cshl.schatz.jnomics.util.DefaultInputStreamHandler;
import edu.cshl.schatz.jnomics.util.InputStreamHandler;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * User: james
 */
public class PBCorrectReduce extends JnomicsReducer<Text,Text,Text,Text>{

    private JnomicsArgument blast_idx_arg = new JnomicsArgument("blast_index","Blast Index", true);
    private JnomicsArgument blastdb_cmd_arg = new JnomicsArgument("blastdbcmd_binary", "blastdbcmd binary", true);

    private String blast_idx, blastdbcmd;

    private Text correctedTitle = new Text();
    private Text correctedSequence = new Text();

    private FSDataOutputStream pileupOut;

    private FileSystem fs;

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{blast_idx_arg, blastdb_cmd_arg};
    }

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return Text.class;
    }

    private FastaParser.FastaRecord getSequenceFromDb(String sequence_id) throws IOException{
        String lookup_entry_cmd = String.format("%s -db %s -entry '%s'",
                  blastdbcmd, blast_idx, sequence_id);

        final FastaParser.FastaRecord[] outRecord = {null};

        ProcessUtil.runCommandEZ(new Command(lookup_entry_cmd,
                new InputStreamHandler() {
                    @Override
                    public void handle(InputStream in) {
                        FastaParser parser = new FastaParser(in);
                        FastaParser.FastaIterator it = (FastaParser.FastaIterator) parser.iterator();
                        if(it.hasNext()){
                            outRecord[0] = it.next();
                        }
                    }
                },new DefaultInputStreamHandler(System.err)));
        
        if(null != outRecord[0])
            return outRecord[0];
        throw new IOException(String.format("Could not retrieve %s from db %s", sequence_id, blast_idx));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        blast_idx = conf.get(blast_idx_arg.getName());
        blastdbcmd = conf.get(blastdb_cmd_arg.getName());

        if(!new File(blast_idx+".nhr").exists())
            throw new IOException("Could not find blast index : " + blast_idx);
        if(!new File(blastdbcmd).exists())
            throw new IOException("Could not find blastdbcmd binary : " + blastdbcmd);

        String task_attempt = context.getTaskAttemptID().toString();
        fs = FileSystem.get(conf);
        pileupOut = fs.create(FileOutputFormat.getPathForWorkFile(context,"_"+task_attempt,".pileups"));
    }

    @Override
    protected void reduce(Text key, final Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        FastaParser.FastaRecord pb_sequence = getSequenceFromDb(key.toString());
        String title = pb_sequence.getName();
        String sequence = pb_sequence.getSequence();

        PacbioCorrector.PBCorrectionResult correctionResult = new PacbioCorrector.PBCorrectionResult(sequence,
                new int[]{1,5,20,100});

        PacbioCorrector.correct(sequence,new Iterable<PacbioCorrector.PBBlastAlignment>() {
            @Override
            public Iterator<PacbioCorrector.PBBlastAlignment> iterator() {
                return new Iterator<PacbioCorrector.PBBlastAlignment>() {
                    private Iterator<Text> vit = values.iterator();
                    @Override
                    public boolean hasNext() {
                        return vit.hasNext();
                    }

                    @Override
                    public PacbioCorrector.PBBlastAlignment next() {
                        String v = vit.next().toString();
                        String[] arr = v.split(",");
                        assert(arr.length == 3);
                        return new PacbioCorrector.PBBlastAlignment(
                                Integer.parseInt(arr[0]),
                                Integer.parseInt(arr[1]),
                                arr[2]
                        );
                    }

                    @Override
                    public void remove() {
                    }
                };
            }
        },correctionResult);

        PacbioCorrector.PBCorrectionResult.PBBaseCoverageStatistics stats = correctionResult.getCoverageStatistics();

        Counter c_one = context.getCounter(PacbioCorrectorCounter.CoverageStatistics.BASES_AT_LEAST_ONE_COVERAGE);
        Counter c_five = context.getCounter(PacbioCorrectorCounter.CoverageStatistics.BASES_AT_LEAST_FIVE_COVERAGE);
        Counter c_twenty = context.getCounter(PacbioCorrectorCounter.CoverageStatistics.BASES_AT_LEAST_TWENTY_COVERAGE);
        Counter c_hundred = context.getCounter(PacbioCorrectorCounter.CoverageStatistics.BASES_AT_LEAST_HUNDRED_COVERAGE);
        Counter c_total = context.getCounter(PacbioCorrectorCounter.CoverageStatistics.TOTAL_BASES);
        c_total.increment(stats.getTotalDatum());
        c_one.increment(stats.getCountForBin(1));
        c_five.increment(stats.getCountForBin(5));
        c_twenty.increment(stats.getCountForBin(20));
        c_hundred.increment(stats.getCountForBin(100));

        try {
            pileupOut.write(new String(">" + title +"\n").getBytes());
            correctionResult.getPileup().printPileup(sequence, pileupOut);
            pileupOut.write("\n\n".getBytes());
        } catch (Exception e) {
            throw new IOException(e);
        }

        correctedTitle.set(title);
        correctedSequence.set(correctionResult.getCorrectedRead());
        context.write(correctedTitle, correctedSequence);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        pileupOut.close();
    }
}
