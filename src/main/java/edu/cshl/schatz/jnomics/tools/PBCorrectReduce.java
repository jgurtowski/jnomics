package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.FastaParser;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.util.Command;
import edu.cshl.schatz.jnomics.util.DefaultInputStreamHandler;
import edu.cshl.schatz.jnomics.util.InputStreamHandler;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

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

    private PacbioCorrector pbCorrector = new PacbioCorrector();

    private FSDataOutputStream pileupOut;


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

        FileSystem fs = FileSystem.get(conf);
        String task_attempt = context.getTaskAttemptID().toString();
        String outputDir = conf.get("marped.output.dir");
        pileupOut = fs.create(new Path(outputDir, "_" + task_attempt + ".pileups"));
    }

    @Override
    protected void reduce(Text key, final Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        FastaParser.FastaRecord pb_sequence = getSequenceFromDb(key.toString());
        String title = pb_sequence.getName();
        String sequence = pb_sequence.getSequence();

        PacbioCorrector.PBCorrectionResult correctionResult = new PacbioCorrector.PBCorrectionResult(sequence);

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

        /*try {
            correctionResult.getPileup().printPileup(sequence,pileupOut);
        } catch (Exception e) {
            throw new IOException(e);
        } */

        correctedTitle.set(title);
        correctedSequence.set(correctionResult.getCorrectedRead());
        context.write(correctedTitle, correctedSequence);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        pileupOut.close();
    }
}
