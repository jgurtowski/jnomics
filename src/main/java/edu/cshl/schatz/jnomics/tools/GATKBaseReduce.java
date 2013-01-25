package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.*;
import net.sf.samtools.SAMSequenceRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.omg.CORBA.PRIVATE_MEMBER;
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
    private static int DEFAULT_GATK_MEM = 2040;

    private final JnomicsArgument gatk_jar_arg = new JnomicsArgument("gatk_jar","GATK jar file",true);
    private final JnomicsArgument db_snp_arg = new JnomicsArgument("dbsnp", "dbSNP vcf file", true);
    private final JnomicsArgument markduplicates_jar_arg = new JnomicsArgument("markduplicates_jar","MarkDuplicates.jar",true);
    private final JnomicsArgument samtools_bin_arg = new JnomicsArgument("samtools_binary","Samtools binary file",true);
    private final JnomicsArgument reference_fa_arg = new JnomicsArgument("reference_fa", "Reference genome", true);
    private final JnomicsArgument gatk_jvm_memory_arg = new JnomicsArgument("gatk_jvm_mem","Memory (MB) used per GATK task", false);
    private final JnomicsArgument addreplace_readgroups_jar_arg = new JnomicsArgument("addreplace_readgroup_jar","AddOrReplacereadGroups.jar", true);
    private Map<String,File> binaries = new HashMap<String, File>();

    protected File samtools_binary, reference_fa, gatk_binary, tmpBam, markduplicates_jar,dbsnp,alterreadgroups_jar;

    private File recal_bam,recal_grp;

    private List<File> tmpFiles = new ArrayList<File>();
    
    protected long binsize, startRange, endRange;
    protected int gatk_jvm_mem;
    
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
                markduplicates_jar_arg,db_snp_arg, gatk_jvm_memory_arg, addreplace_readgroups_jar_arg};
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        File f = null;
        JnomicsArgument[] binary_arguments =  new JnomicsArgument[]{gatk_jar_arg,
                db_snp_arg,markduplicates_jar_arg, samtools_bin_arg,reference_fa_arg,
                addreplace_readgroups_jar_arg
        };
        for(JnomicsArgument binary_arg : binary_arguments){
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
        alterreadgroups_jar = binaries.get(addreplace_readgroups_jar_arg.getName());

        binsize = conf.getLong(SamtoolsMap.genome_binsize_arg.getName(),
                SamtoolsMap.DEFAULT_GENOME_BINSIZE);

        gatk_jvm_mem = conf.getInt(gatk_jvm_memory_arg.getName(),DEFAULT_GATK_MEM); 
        
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

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        removeTempFiles();
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
        String ibc = String.format("%s index %s", samtools_binary, tmpBam);
        ProcessUtil.runCommandEZ(new Command(ibc));
        tmpFiles.add(new File(tmpBam.getAbsolutePath().replace(".bam", ".bai")));
        context.progress(); //we're still alive

        /**Add read group**/
        System.out.println("Adding Read Group");
        File rdgrp_file = new File(tmpBam.getAbsolutePath().replace(".bam",".rg.bam"));
        String rg_cmd= String.format("java -Xmx%dm -jar %s VALIDATION_STRINGENCY=LENIENT INPUT=%s OUTPUT=%s RGLB=%s RGPL=%s RGPU=%s RGSM=%s",
                gatk_jvm_mem, alterreadgroups_jar, tmpBam, rdgrp_file, "jnomics_lib", "illumina", "jnomics_b","jnomics_sample"
                );
        ProcessUtil.runCommandEZ(new Command(rg_cmd));
        tmpFiles.add(rdgrp_file);
        context.progress();

        /**Mark Duplicates**/
        System.out.println("Marking Duplicates");
        File tmpBamMD = new File(rdgrp_file.getAbsolutePath().replace(".bam",".md.bam"));
        File tmpBamMDBai = new File(tmpBamMD.getAbsolutePath().replace(".bam",".bai"));
        File tmpMetrics = new File(tmpBamMD.getAbsolutePath().replace(".md.bam",".md.bam.metrics"));
        String mrkdup_cmd = String.format("java -Xmx%dm -jar %s INPUT=%s OUTPUT=%s ASSUME_SORTED=true METRICS_FILE=%s CREATE_INDEX=true VALIDATION_STRINGENCY=LENIENT",
                gatk_jvm_mem,markduplicates_jar,rdgrp_file,tmpBamMD,tmpMetrics);
        System.out.println(mrkdup_cmd);
        ProcessUtil.runCommandEZ(new Command(mrkdup_cmd));
        tmpFiles.add(tmpBamMD);
        tmpFiles.add(tmpMetrics);
        tmpFiles.add(tmpBamMDBai);
        context.progress(); //we're still alive

        /**Realigner Target Creator **/
        System.out.println("Realigner Target Creator");
        File tmpIntervals = new File(tmpBamMD.getAbsolutePath().replace(".md.bam",".md.bam.intervals"));
        String rtc = String.format("java -Xmx%dm -jar %s -T RealignerTargetCreator -R %s -I %s -o %s",
                gatk_jvm_mem, gatk_binary, reference_fa, tmpBamMD, tmpIntervals);
        System.out.println(rtc);
        ProcessUtil.runCommandEZ(new Command(rtc));
        tmpFiles.add(tmpIntervals);
        context.progress(); //we're still alive

        /**Indel Realigner**/
        System.out.println("Indel Realigner");
        File realignBam = new File(tmpBamMD.getAbsolutePath().replace(".bam",".realign.bam"));
        String ir = String.format("java -Xmx%dm -jar %s -T IndelRealigner -R %s -I %s --targetIntervals %s -o %s",
                gatk_jvm_mem,gatk_binary, reference_fa, tmpBamMD, tmpIntervals, realignBam);
        System.out.println(ir);
        
        ProcessUtil.runCommandEZ(new Command(ir,new InputStreamHandler() {
            @Override
            public void handle(InputStream in) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                //while(true){
                //needs work to set progress
                //}
            }
        },new DefaultInputStreamHandler(System.err)));
        tmpFiles.add(realignBam);
        context.progress(); //still alive

        /**Base Recalibrator**/
        System.out.println("Base Recalibrator");
        recal_grp = new File("recal_data.grp");
        String br = String.format("java -Xmx%dm -jar %s -T BaseRecalibrator -I %s -R %s -knownSites %s -o %s",
                gatk_jvm_mem,gatk_binary,realignBam, reference_fa, dbsnp, recal_grp
        );
        ProcessUtil.runCommandEZ(new Command(br));
        tmpFiles.add(recal_grp);
        context.progress(); //still alive
        
        /**Print Reads**/
        System.out.println("Print Reads");
        recal_bam = new File(realignBam.getAbsolutePath().replace(".bam", ".recal.bam"));
        String pr = String.format("java -Xmx%dm -jar %s -T PrintReads -R %s -I %s -BQSR %s -o %s",
                gatk_jvm_mem, gatk_binary, reference_fa, realignBam, recal_grp, recal_bam);
        ProcessUtil.runCommandEZ(new Command(pr));
        tmpFiles.add(recal_bam);
        context.progress(); //still alive
    }

    protected void markForDeletion(File f){
        tmpFiles.add(f);
    }
    
    protected File getRecalBam(){
        return recal_bam;
    }
    
    protected File getRecalFile(){
        return recal_grp;
    }
    
}
