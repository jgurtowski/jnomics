package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.util.Command;
import edu.cshl.schatz.jnomics.util.FileUtil;
import edu.cshl.schatz.jnomics.util.OutputStreamHandler;
import edu.cshl.schatz.jnomics.util.ProcessUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;


/**
 * Author: Sri
 *
 * Distributed Tophat
 */
public class AlignTophat{

	public AlignTophat() {
		// TODO Auto-generated constructor stub
	}	
	private  Configuration conf = null;
	private static String workingdir;
	private static String userhome;
	private String tophat_cmd= null;
	private Process process=null;
	private boolean ispaired = false;
	private static String jobid = null;

	private final String[] tophat_bin ={ "tophat" , "bowtie" , "bowtie2-build" ,"bowtie-inspect" };
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(AlignTophat.class);

	public AlignTophat(Configuration conf){
		this.conf = conf;
		try {
			AlignTophat.workingdir = new File(".").getCanonicalPath();
			userhome = System.getProperty("user.home");
			jobid = System.getenv("JOB_ID");	
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	protected boolean prepareBinaries(final FileSystem fs1, final Configuration conf) throws IOException,InterruptedException {
		String tophat_binary = conf.get("tophat_binary");
		Path bin_path = new Path(tophat_binary);
		try{
			if(!fs1.exists(bin_path))
				throw new IOException(" Can't find Tophat binary: " + tophat_binary);
			String binaryfile =  bin_path.getName();
			fs1.copyToLocalFile(false,new Path(tophat_binary), new Path(workingdir));
			if(new File(workingdir+"/"+binaryfile).isFile()){
				org.apache.hadoop.fs.FileUtil.unTar(new File(workingdir+"/"+binaryfile),new File(workingdir));
				if(new File(workingdir+"/"+tophat_bin[0]).isFile() || new File(workingdir+"/"+tophat_bin[1]).isFile() || new File(tophat_bin[2]).isFile()
						||  new File(workingdir+"/"+tophat_bin[3]).isFile() ){
					System.out.println("Tophat binaries are loaded");
				}else {
					throw new IOException("Error untaring the Tophat binaries");
				}
			}
		}catch(Exception e){
			throw new IOException("Error loading the Tophat Binaries");
		}
		return true;
	}

	public void align(FileSystem fs2, Configuration  conf1) throws Exception {
		System.out.println("Starting tophat process");
		String tophat_input = conf.get("grid.input.dir","");
		final String tophat_output_dir = workingdir + "/" + conf.get("grid.output.dir","");
		String ref_genome = conf.get("tophat_ref_genome","");
		String tophat_opts = conf.get("tophat_align_opts","");	
		String gtf_file = conf.get("tophat_gtf","");
		String genome_file = new Path(ref_genome).getName();
		String genome = null;
		int ret ;
		Path hdfs_job_path = new Path( fs2.getHomeDirectory().toString());		

		List<String> inputfiles = Arrays.asList(tophat_input.split(","));

		if(inputfiles.size() > 1 ) { ispaired = true; }
		try{
			System.out.println("Copying the input files ");
			for(String line : inputfiles){	
				fs2.copyToLocalFile(false,new Path(line), new Path(workingdir));
			}
			fs2.copyToLocalFile(false,new Path(ref_genome),new Path(workingdir));
			FileUtil.untar(fs2, workingdir+"/"+genome_file.toString(), workingdir);
			File [] idxfiles = new File(workingdir).listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.contains(".fa");		 
				}
			});
			for( File index : idxfiles){
				genome = index.getName().substring(0,index.getName().lastIndexOf("."));
				System.out.println("genome is "  + genome);
			}
			System.out.println("Input files are loaded");
			/* Verifying the output directory*/
			File dir = new File(tophat_output_dir);
			if(dir.mkdir()){
				System.out.println("Output directory : "+ dir.toString() );
			}
			if(gtf_file.isEmpty()){
				if(!ispaired){	
					String infile = new Path(inputfiles.get(0)).getName();
					tophat_cmd =String.format("%s/%s %s -o %s %s %s/%s",workingdir,tophat_bin[0], tophat_opts,tophat_output_dir,genome,workingdir,infile);
				}else {
					StringBuilder sb =  new StringBuilder();
					for(String in : inputfiles){
						sb.append(workingdir).append("/").append(new Path(in).getName()).append(" ");
					}
					tophat_cmd =String.format("%s/%s %s -o %s %s %s",workingdir,tophat_bin[0], tophat_opts,tophat_output_dir,genome,sb.toString());
				}
			}else {
				fs2.copyToLocalFile(false,new Path(gtf_file),new Path(workingdir));
				String gtf = new Path(gtf_file).getName();
				if(!ispaired){	
					String infile = new Path(inputfiles.get(0)).getName();
					tophat_cmd =String.format("%s/%s %s -o %s -G %s/%s %s %s/%s",workingdir,tophat_bin[0],tophat_opts,tophat_output_dir,workingdir,gtf,genome,workingdir,infile);
				}else {
					StringBuilder sb =  new StringBuilder();
					for(String in : inputfiles){
						sb.append(workingdir).append("/").append(new Path(in).getName()).append(" ");
					}
					tophat_cmd =String.format("%s/%s %s -o %s -G %s/%s %s %s",workingdir,tophat_bin[0],tophat_opts,tophat_output_dir,workingdir,gtf,genome,sb.toString());
				}
			}
			String cmd1 = tophat_cmd;
			System.out.println("Executing Tophat Command : " + cmd1);
			ret = ProcessUtil.runCommand(new Command(tophat_cmd));
			if(ret == 0 ){
				fs2.copyFromLocalFile(false,new Path(tophat_output_dir), hdfs_job_path);
			}
			/*File[] files = new File(userhome).listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.matches(".*-tophat-.*[.]."+jobid);
				}
			});
			for(File file : files)
			{
				logger.info("Copying log files to hdfs  : " +  hdfs_job_path+"/"+ file.getName());
				fs2.copyFromLocalFile(true,new Path(file.getAbsolutePath()),hdfs_job_path);
			}
			File[] pfiles = new File(userhome).listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.matches(".*-tophat-.*[.]p."+jobid.toString());
				}
			});
			logger.info("getting here in tophat");
			for(File file : pfiles)
			{
				logger.info("Deleting file" + file.getAbsolutePath() );
				file.delete();		
			}
			*/
			
			logger.info("Tophat Process is Complete ");
		}catch (Exception e) {
			throw new Exception(e.toString());
		}
	}
}
