package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

//unused imports
//import org.apache.commons.lang3.StringUtils;
//import java.net.URI;
//import java.net.URL;
//import java.util.Properties;

/**
 * Author: Sri
 *
 * Distributed Tophat
 */
public class alignTophat{

	public alignTophat() {
		// TODO Auto-generated constructor stub
	}	
	private  Configuration conf = null;
	private static String workingdir;
	private static String userhome;
	private String Tophat_cmd= null;
	private String bowtie_cmd= null;
	private Process process=null;
	private boolean ispaired = false;
	private static String jobid = null;

	private final String[] Tophat_bin ={ "tophat" , "bowtie" , "bowtie-build" ,"bowtie-inspect" };

	public alignTophat(Configuration conf){
		this.conf = conf;
		try {
			alignTophat.workingdir = new File(".").getCanonicalPath();
			userhome = System.getProperty("user.home");
			jobid = System.getenv("JOB_ID");	
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	protected boolean Preparebinaries(final FileSystem fs1, final Configuration conf) throws IOException,InterruptedException {
		String tophat_binary = conf.get("tophat_binary");
		Path bin_path = new Path(tophat_binary);
		try{
		if(!fs1.exists(bin_path))
			throw new IOException(" Can't find Tophat binary: " + tophat_binary);
		String binaryfile =  bin_path.getName();
		fs1.copyToLocalFile(false,new Path(tophat_binary), new Path(workingdir));
		if(new File(workingdir+"/"+binaryfile).isFile()){
			org.apache.hadoop.fs.FileUtil.unTar(new File(workingdir+"/"+binaryfile),new File(workingdir));
				if(new File(workingdir+"/"+Tophat_bin[0]).isFile() || new File(workingdir+"/"+Tophat_bin[1]).isFile() || new File(Tophat_bin[2]).isFile()
						||  new File(workingdir+"/"+Tophat_bin[3]).isFile() ){
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
		
		String Tophat_input = conf.get("grid.input.dir");
		String Tophat_output_dir = workingdir + "/" + conf.get("grid.output.dir","");
		String ref_genome = conf.get("tophat_ref_genome","");
		String Tophat_opts = conf.get("tophat_align_opts","");	
		String genome_file = new Path(ref_genome).getName();
		String genome = genome_file.replace(".fa", "");
		Path hdfs_job_path = new Path( fs2.getHomeDirectory().toString());		
		
		FileOutputStream fout = null;
		FileOutputStream fout1 = null;
		List<String> inputfiles = Arrays.asList(Tophat_input.split(","));
		if(inputfiles.size() > 1 ) { ispaired = true; }
		
		try{
			for(String line : inputfiles){	
			fs2.copyToLocalFile(false,new Path(hdfs_job_path + "/" + line), new Path(workingdir));
			}
			fs2.copyToLocalFile(false,new Path(ref_genome),new Path(workingdir));
			File dir = new File(Tophat_output_dir);
			if(!dir.exists()){
			dir.mkdir();
			}
			/* Verifying the output directory*/
			if(dir.isDirectory()){
				System.out.println("Output directory : "+ dir.toString() );

			}
			bowtie_cmd = String.format("%s/%s %s/%s %s",workingdir,Tophat_bin[2],workingdir,genome_file,genome) ;
			if(!ispaired){	
				String infile = new Path(inputfiles.get(0)).getName();
				Tophat_cmd =String.format("%s/%s %s -o %s %s %s/%s",workingdir,Tophat_bin[0], Tophat_opts,Tophat_output_dir,genome,workingdir,infile);
			}else {
			StringBuilder sb =  new StringBuilder();
				for(String in : inputfiles){
					sb.append(workingdir).append("/").append(new Path(in).getName()).append(" ");
				}
				Tophat_cmd =String.format("%s/%s %s -o %s %s %s",workingdir,Tophat_bin[0], Tophat_opts,Tophat_output_dir,genome,sb.toString());
			}
			Thread connecterr,connectout;
			String cmd = bowtie_cmd;
			System.out.println("Executing Bowtie Command : " + cmd);
			process = Runtime.getRuntime().exec(cmd);
			connecterr = new Thread(
					new ThreadedStreamConnector(process.getErrorStream(), System.err){
						@Override
						public void progress() {
						}
					});
			fout = new FileOutputStream(new File(Tophat_output_dir+"/log"));
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
			connecterr.start();connectout.start();
			connecterr.join();connectout.join();
			process.waitFor();
			fout.close();

			/* setting the process for tophat commands */
			String cmd1 = Tophat_cmd;
			System.out.println("Executing Tophat Command : " + cmd1);
			fout1 = new FileOutputStream(new File(Tophat_output_dir+"/log1"));
			final Process  process1 =  Runtime.getRuntime().exec(Tophat_cmd);
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout1));
			connecterr = new Thread(new ThreadedStreamConnector(process1.getErrorStream(), System.err));
			connectout.start(); connecterr.start();
			process1.waitFor();
			connectout.join();connecterr.join();
			fout1.close();
			fs2.copyFromLocalFile(false,new Path(Tophat_output_dir), hdfs_job_path);
			
			File[] files = new File(userhome).listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.matches(".*-tophat-.*[.]."+jobid);
				}
			});
			for(File file : files)
			{
				fs2.copyFromLocalFile(true,new Path(file.getAbsolutePath()),hdfs_job_path);
			}
//			if(fs2.isDirectory(hdfs_job_path)){
//				System.out.println("Output Copied to HDFS dir "+ hdfs_job_path);
//			}else{
//				System.err.println("Error copying files to HDFS");				
//			}		
			System.out.println("Tophat Process is Complete ");
		} catch (Exception e) {
			throw new Exception(e.toString());
		}
	}
}