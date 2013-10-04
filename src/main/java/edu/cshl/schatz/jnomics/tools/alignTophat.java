package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Properties;

/**
 * Author: Sri
 *
 * Distributed Tophat
 */
public class alignTophat{

	private File[] tmpFiles;
	private String[] Tophat_cmd;
	private Process process;
	private static String workingdir = System.getProperty("user.dir");
	private Properties properties;
	private final String[] Tophat_bin ={ "tophat" , "bowtie" , "bowtie-build" ,"bowtie-inspect" };
	

	protected void setup(final Configuration conf,String Tophat_input, String Tophat_opts,
			String Tophat_output_dir,String ref_genome, String Tophat_binary) throws IOException,InterruptedException {
		
		System.out.println("Tophat opts are "+Tophat_opts +" Tophat output dir "+ 
				Tophat_output_dir +" Tophat binary is "+Tophat_binary);
		FileSystem fs = FileSystem.get(conf);
		
		/*Setup the files paths and names for tophat*/
		Path p = new Path(Tophat_binary);
		Path pinput = new Path(Tophat_input);
		Path pgenome = new Path(ref_genome);
		System.out.println("filename is " + p.getName());
		String binaryfile =  p.getName();
		String infile = pinput.getName();
		String genome_file = pgenome.getName();
		
		if(!fs.exists(p))
			throw new IOException(" Can't find Tophat binary: " + Tophat_binary);
		
		/* Copying the required files to the local disk*/
		fs.copyToLocalFile(false,new Path(Tophat_binary), new Path(workingdir));
		fs.copyToLocalFile(false,new Path(Tophat_input), new Path(workingdir));
		fs.copyToLocalFile(false,new Path(ref_genome),new Path(workingdir));
		
		/*Verifying the files are copied properly*/
		
		if(new File(workingdir+"/"+binaryfile).isFile()){
			org.apache.hadoop.fs.FileUtil.unTar(new File(workingdir+"/"+binaryfile),new File(workingdir));
			if(new File(Tophat_bin[0]).isFile() || new File(Tophat_bin[1]).isFile() || new File(Tophat_bin[2]).isFile()
					||  new File(Tophat_bin[3]).isFile() ){
				System.out.println("Tophat binaries are loaded");

			}else {
				System.err.println("Error loading Tophat binaries!");
				return;
			}
		}
		if(new File(Tophat_input).isFile()){
			System.out.println("Input file  " + Tophat_input+ " is loaded");
		}
		/* Getting the bowtie index file name */
		
		String genome = genome_file.replace(".fa", "");
		
		/* Setting the command for Tophat */
		
		Tophat_cmd =  new String[]{ String.format("%s %s %s",Tophat_bin[2],genome_file,genome) , 
				String.format("%s %s -o %s %s %s",Tophat_bin[0], Tophat_opts,Tophat_output_dir,genome,infile)};
		System.out.println("command is "+ Tophat_cmd[0] +" " + Tophat_cmd[1]) ;
		
		/* Setting the tophat output directory */
		
		Path path = new Path(workingdir+Tophat_output_dir);
		File dir = new File(Tophat_output_dir);
		dir.mkdir();
		
		/* Verifying the output directory*/
		
		if(dir.isDirectory()){
			System.out.println("Tophat Output directory "+ dir.toString() + " is setup");

		}
		fs.close();
		return;
	}

	public void align(final Configuration  conf, String Tophat_input,String Tophat_opts, String Tophat_output_dir,
			String ref_genome, String Tophat_binary) throws IOException, InterruptedException {

		System.out.println("Entered align. conf is" +  conf.toString());
		FileOutputStream fout = null;
		FileOutputStream fout1 = null;
		FileSystem fs = null;
		
		try{
			setup(conf, Tophat_input, Tophat_opts, Tophat_output_dir, ref_genome,Tophat_binary);
			fs = FileSystem.get(conf);
			String hdfs_base = fs.getUri().toString();
			Path outdir = new Path(Tophat_output_dir.substring(Tophat_output_dir.lastIndexOf("/")+1));
			Path path = new Path(hdfs_base + "/user/sramakri/" + outdir );
			Thread connecterr,connectout;
			System.out.println("launching alignment");
			String cmd = Tophat_cmd[0];
			System.out.println("Command formed is " + cmd);
			
			/* setting the process for tophat commands */
			//for(String cmd1: Tophat_cmd){
			process = Runtime.getRuntime().exec(cmd);
			System.out.println(cmd);
		
			connecterr = new Thread(
					new ThreadedStreamConnector(process.getErrorStream(), System.err){
						@Override
						public void progress() {
							// conf.progress();
						}
					});
			System.out.println("hdfs output url path is " +  path);
			System.out.println("ouput dir path is " +  outdir);
			fout = new FileOutputStream(new File(workingdir+"/log"));
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
			connecterr.start();connectout.start();
			connecterr.join();connectout.join();
			process.waitFor();
			fout.close();
			
			/* setting the process for tophat commands */
			
			String cmd1 = Tophat_cmd[1];
			fout1 = new FileOutputStream(new File(Tophat_output_dir+"/log"));
			final Process  process1 =  Runtime.getRuntime().exec(Tophat_cmd[1]);
			System.out.println(cmd1);
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout1));
			connecterr = new Thread(new ThreadedStreamConnector(process1.getErrorStream(), System.err));
			connectout.start(); connecterr.start();
			process1.waitFor();
			connectout.join();connecterr.join();
			fout1.close();
			//			//fs.create(path);
			fs.mkdirs(path);
			if(fs.isDirectory(path)){
				System.out.println("Output Copied to HDFS dir "+ outdir);
			}else{
				System.err.println("Error copying files to HDFS");				
			}
			
			/* Copying the results back to HDFS NOT WORKING !!!!!!!!!!!!! FIX IT!!!!!!!! */
			
			fs.copyFromLocalFile(false,new Path(Tophat_output_dir) , path);
			//org.apache.hadoop.fs.FileUtil.copy(fs, new Path(Tophat_output_dir+"/*"),path,false,conf);
			if(fs.isDirectory(path)){
				System.out.println("Output Copied to HDFS dir "+ outdir);
			}else{
				System.err.println("Error copying files to HDFS");				
			}		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			//fout.close();
			fs.close();
		}
		//context.progress();
		// }
		System.out.println(" Tophat Process Complete  ");

	}
}