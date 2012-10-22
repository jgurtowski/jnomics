package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.AlignmentReaderContextWriter;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.util.FileUtil;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Author: James
 *
 * Distributed BWA
 */
public class BWAMap extends AlignmentBaseMap{

    private File[] tmpFiles;
    private String[] aln_cmds_pair,aln_cmds_single;
    private String sampe_cmd, samse_cmd;
    private Process process;

    private AlignmentReaderContextWriter reader = null;

    private final JnomicsArgument bwa_aln_opts_arg = new JnomicsArgument("bwa_aln_opts",
            "Alignment options for BWA Align", false);
    private final JnomicsArgument bwa_sampe_opts_arg = new JnomicsArgument("bwa_sampe_opts",
            "Alignment options for BWA Sampe", false);
    private final JnomicsArgument bwa_samse_opts_arg = new JnomicsArgument("bwa_samse_opts",
            "Alignment options for BWA Samse", false);
    private final JnomicsArgument bwa_idx_arg = new JnomicsArgument("bwa_index", "bwa index location", true);
    private final JnomicsArgument bwa_binary_arg = new JnomicsArgument("bwa_binary", "bwa binary location", true);

    @Override
    public JnomicsArgument[] getArgs() {
        JnomicsArgument[] superArgs = super.getArgs();
        JnomicsArgument[] newArgs = new JnomicsArgument[]{bwa_aln_opts_arg,bwa_binary_arg,bwa_idx_arg,
                bwa_sampe_opts_arg,bwa_samse_opts_arg};
        return ArrayUtils.addAll(superArgs, newArgs);
    }

    @Override
    protected void setup(final Context context) throws IOException,InterruptedException {
        super.setup(context);
        String bwa_aln_opts = context.getConfiguration().get(bwa_aln_opts_arg.getName(),"");
        String bwa_sampe_opts = context.getConfiguration().get(bwa_sampe_opts_arg.getName(), "");
        String bwa_samse_opts = context.getConfiguration().get(bwa_samse_opts_arg.getName(),"");
        String bwa_idx = context.getConfiguration().get(bwa_idx_arg.getName());
        String bwa_binary = context.getConfiguration().get(bwa_binary_arg.getName());
        if(!new File(bwa_binary).exists())
            throw new IOException("Can't find bwa binary: " + bwa_binary);

        String taskAttemptId = context.getTaskAttemptID().toString();

        tmpFiles = new File[]{
                new File(taskAttemptId + ".1.sai"),
                new File(taskAttemptId + ".2.sai"),
        };

        FileUtil.markDeleteOnExit(tmpFiles);

        File[] superTmpFiles = getTempFiles();

        System.out.println("BWA index--->"+bwa_idx);
        aln_cmds_pair = new String[]{
                String.format(
                        "%s aln %s %s %s",
                        bwa_binary, bwa_aln_opts, bwa_idx, superTmpFiles[0]),

                String.format(
                        "%s aln %s %s %s",
                        bwa_binary, bwa_aln_opts, bwa_idx, superTmpFiles[1])
        };

        aln_cmds_single = new String[]{aln_cmds_pair[0]};
        
        sampe_cmd = String.format(
                "%s sampe %s %s %s %s %s %s",
                bwa_binary, bwa_sampe_opts, bwa_idx, tmpFiles[0], tmpFiles[1],
                superTmpFiles[0], superTmpFiles[1]
        );

        samse_cmd = String.format(
                "%s samse %s %s %s %s",
                bwa_binary, bwa_samse_opts, bwa_idx, tmpFiles[0],  superTmpFiles[0]
        );

    }


    @Override
    public void align(final Context context) throws IOException, InterruptedException {
        if(null == reader)
            reader = new AlignmentReaderContextWriter(isPairedEnd());
        Thread connecterr,connectout;
        System.out.println("launching alignment");
        
        /**Launch Processes **/
        int idx = 0;
        FileOutputStream fout;

        for(String cmd: isPairedEnd() ? aln_cmds_pair : aln_cmds_single){
            process = Runtime.getRuntime().exec(cmd);
            System.out.println(cmd);
            // Reattach stderr and write System.stdout to tmp file
            connecterr = new Thread(
                    new ThreadedStreamConnector(process.getErrorStream(), System.err){
                        @Override
                        public void progress() {
                            context.progress();
                        }
                    });
            fout = new FileOutputStream(tmpFiles[idx]);
            connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
            connecterr.start();connectout.start();
            connecterr.join();connectout.join();
            process.waitFor();
            fout.close();
            idx++;
            context.progress();
        }

        System.out.println("running sampe/samse");
        
        /**Launch sampe/samse command*/
        final Process sam_process = Runtime.getRuntime().exec(isPairedEnd() ? sampe_cmd : samse_cmd);

        connecterr = new Thread(new ThreadedStreamConnector(sam_process.getErrorStream(), System.err));
        connecterr.start();
        
        /** setup reader thread - reads lines from bwa stdout and print them to context **/
        reader.read(sam_process.getInputStream(), context);

        sam_process.waitFor();
        connecterr.join();

        System.out.println("removing tmp files");
        FileUtil.removeFiles(tmpFiles);
    }


}