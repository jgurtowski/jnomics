package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.AlignmentReaderContextWriter;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.util.FileUtil;




//import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Author: Sri
 *
 * Distributed cufflinks
 */
public class CufflinksMap1{
	private String cufflinks_cmd;
	private Process process;
	private static String workingdir = System.getProperty("user.dir");

	private final String[] cuff_bin = {"cufflinks","cuffmerge","cuffdiff","cuffcompare" };

	protected boolean Preparebinaries(final FileSystem fs, String cufflinks_binary) throws IOException,InterruptedException {
		Path bin_path = new Path(cufflinks_binary);
		if(!fs.exists(bin_path))
			throw new IOException(" Can't find cufflinks binary: " + cufflinks_binary);
		String binaryfile =  bin_path.getName();
		fs.copyToLocalFile(false,new Path(cufflinks_binary), new Path(workingdir));
		if(new File(workingdir+"/"+binaryfile).isFile()){
			org.apache.hadoop.fs.FileUtil.unTar(new File(workingdir+"/"+binaryfile),new File(workingdir));
			if(new File(cuff_bin[0]).isFile() && new File(cuff_bin[1]).isFile() 
					&& new File(cuff_bin[2]).isFile() && new File(cuff_bin[3]).isFile()) {
				System.out.println("Cufflinks binaries are loaded");
			}else {
				System.err.println("Error loading Cufflink binaries!");
				return false;
			}
		}
		return true;
	}
	protected boolean setup(final FileSystem fs,String cuff_bam_input, String ref_genome, String gtf_file,String cufflinks_opts,
			String cufflinks_output_dir, String cuff_command) throws IOException,InterruptedException {
		System.out.println("cufflinks opts are "+cufflinks_opts +" cufflinks output dir "+ cufflinks_output_dir );
		if(cuff_command.equals("cufflinks")) {
			Path pinput = new Path(cuff_bam_input);
			String infile = pinput.getName();
			fs.copyToLocalFile(false,new Path(cuff_bam_input), new Path(workingdir));
			if(new File(cuff_bam_input).isFile()){
				System.out.println("Input file  " + cuff_bam_input+ " is loaded");
			}
			cufflinks_cmd =  String.format("%s %s -o %s %s",cuff_command, cufflinks_opts,cufflinks_output_dir,infile);
		}
		else if(cuff_command.equals("cuffmerge")){
		
		cufflinks_cmd =  String.format("%s %s -o %s %s",
				cuff_command, cufflinks_opts,cufflinks_output_dir,cuff_bam_input);
		}
		else if(cuff_command.equals("cuffdiff")){
			
		cufflinks_cmd =  String.format("%s -o %s -b %s %s -u %s",
				cuff_command,cufflinks_output_dir,ref_genome,cufflinks_opts,gtf_file,cuff_bam_input);
		}
		System.out.println("command is "+ cufflinks_cmd);
		
		File dir = new File(cufflinks_output_dir);
		dir.mkdir();
		if(dir.isDirectory()){
			System.out.println("Cufflinks Output directory "+ dir.toString() + " is setup");
		}
		return true;
	}

	public boolean callCufflinks(final FileSystem  fs, String cuff_input,String cufflinks_opts, 
			String cufflinks_output_dir) throws IOException, InterruptedException {
		//System.out.println("Entered align. conf is" +  conf.toString());
		FileOutputStream fout = null;
		//FileSystem  fs = FileSystem.get(conf);

		String hdfs_base = fs.getUri().toString();
		//Path outdir = new Path(cufflinks_output_dir.substring(cufflinks_output_dir.lastIndexOf("/")+1));
		Path path = new Path(hdfs_base + "/user/sramakri/");
		try{
			boolean ret = setup(fs, cuff_input, null,null,cufflinks_opts,  cufflinks_output_dir,"cufflinks");
			if(!ret) { 
				System.out.println("Error in Setup");
			}else{
				Thread connecterr,connectout;
				System.out.println("launching alignment");

				String cmd = cufflinks_cmd;
				System.out.println("Command formed is " + cmd);
				// for(String cmd: isPairedEnd() ? aln_cmds_pair : aln_cmds_single){
				process = Runtime.getRuntime().exec(cmd);
				System.out.println(cmd);
				// Reattach stderr and write System.stdout to tmp file
				connecterr = new Thread(
						new ThreadedStreamConnector(process.getErrorStream(), System.err){
							@Override
							public void progress() {
								// conf.progress();
							}
						});

				System.out.println("hdfs output url path is " +  path);
				//	System.out.println("ouput dir path is " +  outdir);
				fout = new FileOutputStream(new File(cufflinks_output_dir+"/log"));
				connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
				connecterr.start();connectout.start();
				connecterr.join();connectout.join();
				process.waitFor();
				fout.close();
				//fs.create(path);
				if(!fs.exists(path)) {
					fs.mkdirs(path);	
				}
				fs.copyFromLocalFile(false,new Path(cufflinks_output_dir) , path);
				//org.apache.hadoop.fs.FileUtil.copy(fs, new Path(cufflinks_output_dir+"/*"),path,false,conf);
				if(fs.isDirectory(path)){
					System.out.println("Output Copied to HDFS dir "+ path);
				}else{
					System.err.println("Error copying files to HDFS");				
				}		
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		//context.progress();
		// }
		System.out.println(" Cufflinks Process Complete  ");
		return true;
	}

	public boolean callCuffmerge(final FileSystem fs, String assembly_path_file , String ref_genome, String cuffmerge_opts, String cuffmerge_out) throws IOException, InterruptedException {
		FileOutputStream fout = null;
		String hdfs_base = fs.getUri().toString();
		if(cuffmerge_out.equals(null)  || cuffmerge_out.isEmpty()) {
			System.out.println("Coming inside cuffmerge");
			cuffmerge_out = workingdir + "/merged_asm";
		}
		Path path = new Path(hdfs_base + "/user/sramakri/");
		boolean ret = setup(fs, assembly_path_file, ref_genome,null,cuffmerge_opts,cuffmerge_out,"cuffmerge");
		if(!ret) { 
			System.out.println("Error in Setup");
		}else{
			Thread connecterr,connectout;
			System.out.println("Running Cuffmerge...");

			String cmd = cufflinks_cmd;
			System.out.println("Command formed is " + cmd);
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
			fout = new FileOutputStream(new File(cuffmerge_out+"/log"));
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
			connecterr.start();connectout.start();
			connecterr.join();connectout.join();
			process.waitFor();
			fout.close();
			//fs.create(path);
			if(!fs.exists(path)) {
				fs.mkdirs(path);	
			}
			fs.copyFromLocalFile(false,new Path(cuffmerge_out) , path);
			//org.apache.hadoop.fs.FileUtil.copy(fs, new Path(cufflinks_output_dir+"/*"),path,false,conf);
			if(fs.isDirectory(path)){
				System.out.println("Output Copied to HDFS dir "+ path);
			}else{
				System.err.println("Error copying files to HDFS");				
			}		
		}
		System.out.println(" Cuffmerge Process Complete  ");
		return true;	
	}
		public boolean callCuffdiff(final FileSystem fs, ArrayList<String> cuffdiff_bam_input ,
				String ref_genome, String merged_gtf, String cuffdiff_opts, String cuffdiff_out) throws IOException, InterruptedException {
			FileOutputStream fout = null;
			String hdfs_base = fs.getUri().toString();
//			if(cuffdiff_out !=  null  || cuffdiff_out.isEmpty()) {
//				cuffdiff_out = workingdir + "/diff_out";
//			}
			Path path = new Path(hdfs_base + "/user/sramakri/");
			File dir = new File(cuffdiff_out);
			dir.mkdir();
			if(dir.isDirectory()){
				System.out.println("Cuffdiff Output directory "+ dir.toString() + " is setup");
			}
			//boolean ret = setup(fs, cuff_diff_bam_input,null,null, cuffdiff_opts,cuffdiff_out,"cuffdiff");
//			if(!ret) { 
//				System.out.println("Error in Setup");
//			}else{
			cufflinks_cmd =  String.format("%s -o %s -b %s %s -u %s",
					"cuffdiff",cuffdiff_out,ref_genome,cuffdiff_opts,merged_gtf);
			StringBuilder sb =  new StringBuilder();
			for(String bam_file : cuffdiff_bam_input){
				System.out.println("Inside the bam file list loop");
				sb.append(" ").append(bam_file).append(",");
				System.out.println("Cuffdiff command formed is"+  sb.toString());
			}
			cufflinks_cmd = cufflinks_cmd.concat(sb.toString());
			
				Thread connecterr,connectout;
				System.out.println("Running Cuffdiff...");
	
				String cmd = cufflinks_cmd;
				//System.out.println("Command formed is " + cmd);
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
				fout = new FileOutputStream(new File(cuffdiff_out+"/log"));
				connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
				connecterr.start();connectout.start();
				connecterr.join();connectout.join();
				process.waitFor();
				fout.close();
				//fs.create(path);
				if(!fs.exists(path)) {
					fs.mkdirs(path);	
				}
				fs.copyFromLocalFile(false, new Path(cuffdiff_out), path);
				//org.apache.hadoop.fs.FileUtil.copy(fs, new Path(cufflinks_output_dir+"/*"),path,false,conf);
				if(fs.isDirectory(path)){
					System.out.println("Output Copied to HDFS dir "+ path);
				}else{
					System.err.println("Error copying files to HDFS");				
				}		
		//	}
		System.out.println(" Cuffdiff Process Complete  ");
		return true;	
		}
}