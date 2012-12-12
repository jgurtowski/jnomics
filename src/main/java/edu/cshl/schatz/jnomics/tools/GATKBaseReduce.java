package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.Command;
import edu.cshl.schatz.jnomics.util.OutputStreamHandler;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import net.sf.samtools.SAMSequenceRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: james
 * Base class reducer for many GATK Operations
 */

public abstract class GATKBaseReduce<VALIN,VALOUT>
        extends JnomicsReducer<SamtoolsMap.SamtoolsKey,SAMRecordWritable,VALIN,VALOUT> {

    private static Logger logger = LoggerFactory.getLogger(GATKBaseReduce.class);

    private final JnomicsArgument gatk_jar_arg = new JnomicsArgument("gatk_jar","GATK jar file",true);
    private final JnomicsArgument db_snp_arg = new JnomicsArgument("dbsnp", "dbSNP vcf file", true);
    private final JnomicsArgument markduplicates_jar_arg = new JnomicsArgument("markduplicates_jar","MarkDuplicates.jar",true);
    private final JnomicsArgument samtools_bin_arg = new JnomicsArgument("samtools_binary","Samtools binary file",true);
    private final JnomicsArgument reference_fa_arg = new JnomicsArgument("reference_fa", "Reference genome", true);

    private Map<String,File> binaries = new HashMap<String, File>();

    protected File samtools_binary, reference_fa, gatk_binary, tmpBam, markduplicates_jar,dbsnp;

    private File recal_bam;

    private List<File> tmpFiles = new ArrayList<File>();
    
    protected long binsize, startRange, endRange;

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public Class<? extends WritableComparator> getGrouperClass() {
        return SamtoolsReduce.SamtoolsGrouper.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{gatk_jar_arg, samtools_bin_arg,reference_fa_arg,
                markduplicates_jar_arg,db_snp_arg};
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        File f = null;
        for(JnomicsArgument binary_arg : getArgs()){
            if(!binary_arg.isRequired())
                continue;
            f =  new File(conf.get(binary_arg.getName(),""));
            if(!f.exists())
                throw new IOException("Missing : " + binary_arg.getName() + ":have:" + f);
            binaries.put(binary_arg.getName(),f);
        }

        samtools_binary = binaries.get(samtools_bin_arg.getName());
        reference_fa = binaries.get(reference_fa_arg.getName());
        gatk_binary = binaries.get(gatk_jar_arg.getName());
        markduplicates_jar = binaries.get(markduplicates_jar_arg.getName());
        dbsnp = binaries.get(db_snp_arg.getName());
        
        binsize = context.getConfiguration().getLong(SamtoolsMap.genome_binsize_arg.getName(),
                SamtoolsMap.DEFAULT_GENOME_BINSIZE);

        /**Symlink reference_fa to local dir, GATK has locking issues on the .dict file**/
        File local_reference_fa = new File(reference_fa.getName());
        ProcessUtil.runCommandEZ(new Command("ln -s " + reference_fa + " " + local_reference_fa));
        reference_fa = local_reference_fa;
    }

    private void removeTempFiles(){
        for(File f : tmpFiles){
            f.delete();
        }
    }


    /**
     * Run base operations common to both Haplotypecaller and UnifiedGenotyper
     */

    @Override
    protected void reduce(final SamtoolsMap.SamtoolsKey key, final Iterable<SAMRecordWritable> values, final Context context)
            throws IOException, InterruptedException {
        removeTempFiles();
        tmpFiles.clear();
        
        tmpBam = new File(context.getTaskAttemptID()+".tmp.bam");
        tmpFiles.add(tmpBam);
        
        //hack to get data from inner function
        final int[] ref_len = new int[1];
        /**Write bam to temp file**/
        System.out.println("Writing Alignments to bam file");
        ProcessUtil.runCommandEZ(new Command(String.format("%s view -Sb -o %s -", samtools_binary, tmpBam),

                new OutputStreamHandler() {
                    @Override
                    public void handle(OutputStream out) {
                        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
                        long count = 0;
                        try {
                            for(SAMRecordWritable record: values){
                                if(0 == count){
                                    writer.write(record.getTextHeader()+"\n");
                                    String ref = key.getRef().toString();
                                    SAMSequenceRecord headerSequence = record.getSAMRecord().getHeader().getSequence(ref);
                                    if(null != headerSequence)
                                        ref_len[0] = headerSequence.getSequenceLength();
                                    if(ref_len[0] < 0)
                                        throw new IOException("Could not get Reference Length from header");
                                }
                                writer.write(record.toString()+"\n");
                                if(0 == ++count % 1000 ){
                                    context.progress();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace(System.err);
                        }finally{
                            try {
                                writer.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
        ));
        
        context.progress(); //we're still alive

        startRange = key.getBin().get() * binsize + 1;
        endRange = startRange + binsize - 1;
        endRange = endRange > ref_len[0] ? ref_len[0] : endRange;
        System.out.println("Working on Region:" + key.getRef() + ":" + startRange + "-" + endRange);

        /**Index Bam**/
        System.out.println("Indexing Bam");
        ProcessUtil.runCommandEZ(new Command(String.format("%s index %s", samtools_binary, tmpBam)));
        tmpFiles.add(new File(tmpBam.getAbsolutePath().replace(".bam", ".bai")));
        context.progress(); //we're still alive

        /**Mark Duplicates**/
        System.out.println("Marking Duplicates");
        File tmpBamMD = new File(tmpBam.getAbsolutePath().replace(".bam",".md.bam"));
        File tmpBamMDBai = new File(tmpBamMD.getAbsolutePath().replace(".bam",".bai"));
        File tmpMetrics = new File(tmpBamMD,".metrics");
        String mrkdup_cmd = String.format("java -jar %s INPUT=%s OUTPUT=%s ASSMUME_SORTED=true METRICS_FILE=%s CREATE_INDEX=true VALIDATION_STRINGENCY=LENIENT",
                markduplicates_jar,tmpBam,tmpBamMD,tmpMetrics);
        ProcessUtil.runCommandEZ(new Command(mrkdup_cmd));
        tmpFiles.add(tmpBamMD);
        tmpFiles.add(tmpMetrics);
        tmpFiles.add(tmpBamMDBai);
        context.progress(); //we're still alive

        /**Realigner Target Creator **/
        System.out.println("Realigner Target Creator");
        File tmpIntervals = new File(tmpBamMD,".intervals");
        ProcessUtil.runCommandEZ(new Command(String.format("java -jar %s -T RealignerTargetCreator -R %s -I %s -o %s",
                gatk_binary, reference_fa, tmpBamMD, tmpIntervals
        )));
        tmpFiles.add(tmpIntervals);
        context.progress(); //we're still alive

        /**Indel Realigner**/
        System.out.println("Indel Realigner");
        File realignBam = new File(tmpBamMD.getAbsolutePath().replace(".bam",".realign.bam"));
        ProcessUtil.runCommandEZ(
                new Command(String.format("java -jar %s -T IndelRealigner -R %s -I %s --targetIntervals %s -o %s",
                        gatk_binary,reference_fa,tmpBamMD, tmpIntervals,realignBam
                ))
        );
        tmpFiles.add(realignBam);
        context.progress(); //still alive

        /**Base Recalibrator**/
        System.out.println("Base Recalibrator");
        File recal_grp = new File("recal_data.grp");
        String.format("java -jar %s -T BaseRecalibrator -I %s -R %s -knownSites %s -o %s",
                gatk_binary, realignBam, reference_fa, dbsnp, recal_grp
        );
        tmpFiles.add(recal_grp);
        context.progress(); //still alive
        
        /**Print Reads**/
        System.out.println("Print Reads");
        recal_bam = new File(realignBam.getAbsolutePath().replace(".bam", ".recal.bam"));
        ProcessUtil.runCommandEZ(new Command(String.format("java -jar %s -T PrintReads -R %s -I %s -BQSR %s -o %s",
                gatk_binary,reference_fa,realignBam,recal_grp, recal_bam
        )));
        tmpFiles.add(recal_bam);
        context.progress(); //still alive

    }

    protected void markForDeletion(File f){
        tmpFiles.add(f);
    }
    
    protected File getRecalBam(){
        return recal_bam;
    }
    
}
