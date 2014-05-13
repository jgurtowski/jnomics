package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.grid.JnomicsGridJobBuilder;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.util.Command;
import edu.cshl.schatz.jnomics.util.FileUtil;
import edu.cshl.schatz.jnomics.util.OutputStreamHandler;
import edu.cshl.schatz.jnomics.util.ProcessUtil;

import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Author: Sri
 *
 * Distributed cufflinks
 */
public class CufflinksSuite{

	Configuration conf;

	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(CufflinksSuite.class);

	private String cufflinks_cmd;
	private static String workingdir;
	private static String jobdirname;
	private static String userhome ;
	private static String jobid  = null;
	private static String jobname  = null;
	private final String[] cuff_bin = {"cufflinks","cuffmerge","cuffdiff","cuffcompare" };

	public CufflinksSuite(Configuration conf) {
		this.conf = conf;
		try {
			CufflinksSuite.workingdir = new File(".").getCanonicalPath();
			jobdirname =  conf.get("grid.output.dir");
			userhome = System.getProperty("user.home");
			jobname= System.getenv("JOB_NAME");
			jobid = System.getenv("JOB_ID");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected boolean prepareBinaries(final FileSystem fs, final Configuration conf) throws IOException,InterruptedException {
		String cufflinks_binary = conf.get("cufflinks_binary");
		Path bin_path = new Path(cufflinks_binary);
		if(!fs.exists(bin_path))
			throw new IOException(" Can't find cufflinks binary: " + cufflinks_binary);
		String binaryfile =  bin_path.getName();
		fs.copyToLocalFile(false,new Path(cufflinks_binary), new Path(workingdir));
		if(new File(workingdir+"/"+binaryfile).isFile()){
			org.apache.hadoop.fs.FileUtil.unTar(new File(workingdir+"/"+binaryfile),new File(workingdir));
			if(new File(cuff_bin[0]).isFile() && new File(cuff_bin[1]).isFile() 
					&& new File(cuff_bin[2]).isFile() && new File(cuff_bin[3]).isFile()) {
				logger.info("Cufflinks binaries are loaded");
			}else {
				System.err.println("Error loading Cufflinks binaries!");
				throw new IOException();
			}
		}
		return true;
	}

	public boolean callCufflinks(final FileSystem  fs, Configuration conf1) throws IOException, InterruptedException {
		System.out.println(" Starting cufflinks process ");
		String cuff_bam_input = conf.get("grid.input.dir","");
		String infile = new Path(cuff_bam_input).getName();
		String cufflinks_opts =  conf.get("cufflinks_opts","");
		String ref_gtf = conf.get("cufflinks_gtf","");
		final String cuff_output_dir = conf.get("grid.output.dir","");
		FileOutputStream fout = null;
		Path hdfs_job_path = new Path(fs.getHomeDirectory().toString()+"/"+cuff_output_dir);
		File dir = new File(cuff_output_dir);
		int ret;
		try{
			System.out.println(" Copying cufflinks input files ");
			fs.copyToLocalFile(false,new Path(cuff_bam_input), new Path(workingdir));
			
			System.out.println(" cufflinks input files are loaded ");
			if(new File(workingdir + "/" + cuff_bam_input).isFile()){
				logger.info("Input file  : " + cuff_bam_input+ " ");
			}
			if(!dir.exists()){
				dir.mkdirs();
			}
			if(dir.isDirectory()){
				logger.info("Output directory : "+ dir.toString());
			}
			if(ref_gtf.isEmpty()){
			cufflinks_cmd =  String.format("%s/cufflinks %s -o %s %s/%s",workingdir, cufflinks_opts,cuff_output_dir,workingdir,infile);
			}else{
				fs.copyToLocalFile(false,new Path(ref_gtf), new Path(workingdir));
				String gtf = new Path(ref_gtf).getName();
				cufflinks_cmd =  String.format("%s/cufflinks %s -o %s -G %s/%s %s/%s",workingdir, cufflinks_opts,cuff_output_dir,workingdir,gtf,workingdir,infile);
			}
			String cmd = cufflinks_cmd;
			System.out.println("Executing Cufflinks cmd : "+ cmd);
			ProcessUtil.runCommandEOE(new Command(cufflinks_cmd));
			logger.info("Copying Results to hdfs : " +  hdfs_job_path);
			fs.copyFromLocalFile(false,new Path(cuff_output_dir) , hdfs_job_path);
			System.out.println(" Cufflinks Process is Complete  ");
		}catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	public boolean callCuffmerge(final FileSystem fs,Configuration conf2) throws IOException, InterruptedException {
		System.out.println(" Starting cuffmerge process ");
		final String cuffmerge_output_dir = conf2.get("grid.output.dir","");
		String cuffmerge_in = conf2.get("grid.input.dir","");
		String inputfile = new Path(cuffmerge_in).getName();
		String cuffmerge_opts = conf.get("cuffmerge_opts","");
		String ref_genome = conf.get("cuffmerge_ref_genome","");
		String ref_genome_local = new Path(ref_genome).getName();
		String gtf_file = conf.get("cuffmerge_gtf","");
		String genome = null;
		int ret; 
		
		BufferedReader reader;
		File dir = new File(cuffmerge_output_dir);
		FileOutputStream fout = null;
		Path hdfs_job_path = new Path(fs.getHomeDirectory().toString()+"/" +cuffmerge_output_dir);
		String line = null;
		List<String> filelist =  new ArrayList<String>();

		try {
			logger.info(" Copying cuffmerge input files ");

			fs.copyToLocalFile(false,new Path(cuffmerge_in), new Path(workingdir));
			fs.copyToLocalFile(false,new Path(ref_genome), new Path(workingdir));
			FileUtil.untar(fs, workingdir+"/"+ref_genome_local.toString(), workingdir);
			File [] idxfiles = new File(workingdir).listFiles(new FilenameFilter() {
			    @Override
			    public boolean accept(File dir, String name) {
			        return name.endsWith(".fa") || name.endsWith(".fasta");
			    }
			});
			for( File index : idxfiles){
				 genome = index.getName();
			}
			logger.info(" cuffmerge input files are loaded ");
			if(new File(workingdir + "/" + inputfile).isFile()){
				System.out.println("Input file  :" + cuffmerge_in);
			}
			reader = new BufferedReader(new FileReader(new File(workingdir + "/" +inputfile)));
			while((line = reader.readLine()) != null){
				filelist.add(line);
			}
			if(!FileUtil.copyFromHdfs(fs, filelist, workingdir)){
				System.err.println("Error copying the input files ");
			}
			if(!dir.exists()){
				dir.mkdirs();
			}
			if(!dir.isDirectory()){
				System.err.println("Error creating the output dir " + cuffmerge_output_dir);
			}
			if(gtf_file.isEmpty()){
				cufflinks_cmd =  String.format("%s/cuffmerge %s -o %s -s %s/%s %s/%s",
					workingdir, cuffmerge_opts,cuffmerge_output_dir,workingdir,genome,workingdir,inputfile);
			}else {
				fs.copyToLocalFile(false,new Path(gtf_file), new Path(workingdir));
				String gtf = new Path(gtf_file).getName();
				cufflinks_cmd =  String.format("%s/cuffmerge %s -o %s -g %s/%s -s %s/%s %s/%s",
						workingdir, cuffmerge_opts,cuffmerge_output_dir,workingdir,gtf,workingdir,genome,workingdir,inputfile);
			}
			String cmd = cufflinks_cmd;
			System.out.println("Executing Cuffmerge command :" + cmd);
			ProcessUtil.runCommandEOE(new Command(cufflinks_cmd));
			logger.info("Copying Results to hdfs : " +  hdfs_job_path);
			fs.copyFromLocalFile(false,new Path(cuffmerge_output_dir) , hdfs_job_path);
			fs.deleteOnExit(new Path("cuffmerge-"+jobname+".txt"));
			logger.info(" Cuffmerge Process is Complete  ");
		}catch(Exception e){
			e.printStackTrace();
		}
		return true;	

	}

	public boolean callCuffdiff(final FileSystem fs, Configuration conf) throws Exception {
		System.out.println(" Starting cufflinks process ");
		String[] jobparts = conf.get("grid.job.name").split("-");
		String cuffdiff_input = conf.get("input_files","");
		String cuffdiff_out = conf.get("grid.output.dir","");
		String ref_genome = conf.get("cuffdiff_ref_genome","");
		String ref_genome_local = new Path(ref_genome).getName();
		String cuffdiff_opts = conf.get("cuffdiff_opts","");
		String merged_gtf = conf.get("cuffdiff_merged_gtf","");
		String cuffdiff_lbs = conf.get("cuffdiff_condn_labels","");
		String genome = null;
		int ret ;
		Path hdfs_job_path = new Path(fs.getHomeDirectory().toString()+"/"+cuffdiff_out);
		final File dir = new File(workingdir + "/" + cuffdiff_out);

		try {
			logger.info("Copying cuffdiff input files ");

			List<String> cuffdiff_in = Arrays.asList(cuffdiff_input.split(","));
			if(!FileUtil.copyFromHdfs(fs,cuffdiff_in, workingdir)){
				System.err.println("Error in Copying the Cuffdiff Input files "); 
			}
			String gtf = merged_gtf.substring(merged_gtf.lastIndexOf("/") + 1);
			fs.copyToLocalFile(false, new Path(merged_gtf), new Path(workingdir));
			fs.copyToLocalFile(false,new Path(ref_genome),  new Path(workingdir));
			FileUtil.untar(fs, workingdir+"/"+ref_genome_local.toString(), workingdir);
			File [] idxfiles = new File(workingdir).listFiles(new FilenameFilter() {
			    @Override
			    public boolean accept(File dir, String name) {
			        return name.endsWith(".fa") || name.endsWith(".fasta");
			    }
			});
			for( File index : idxfiles){
				 genome = index.getName();
			}
			System.out.println(" input files are loaded ");

			FileOutputStream fout = null;
			if(!dir.exists()){
				dir.mkdirs();
			}
			if(dir.isDirectory()){
				System.out.println("Output directory : "+ dir.toString());
			}
			cufflinks_cmd =  String.format("%s/cuffdiff -o %s -b %s/%s %s -L %s -u %s/%s",
					workingdir,cuffdiff_out,workingdir,genome,cuffdiff_opts,cuffdiff_lbs,workingdir,gtf);
			StringBuilder sb =  new StringBuilder();
			for(String bam_file : cuffdiff_in){
				sb.append(" ").append(workingdir).append("/").append(bam_file).append(",");
			}
			cufflinks_cmd = cufflinks_cmd.concat(sb.toString());
			System.out.println("Executing Cuffdiff command " + cufflinks_cmd);
			ProcessUtil.runCommandEOE(new Command(cufflinks_cmd));
			logger.info("Copying Results to hdfs : " +  hdfs_job_path);
			fs.copyFromLocalFile(false, new Path(cuffdiff_out), hdfs_job_path);
			logger.info(" Cuffdiff Process is Complete  ");
		}catch(IOException e){
			throw new IOException(e.toString());
		}
		return true;	
	}
	
	public boolean callCuffcompare(final FileSystem fs,Configuration conf ) throws Exception {
		System.out.println("Starting cuffcompare process ");
		String cuffcompare_in = conf.get("grid.input.dir","");
		String infile = new Path(cuffcompare_in).getName();
		String cuffcompare_opts = conf.get("cuffcompare_opts","");
		String ref_gtf = fs.getHomeDirectory().toString() + "/" +conf.get("cuffcompare_gtf","");
		String ref_gtfname = new Path(ref_gtf).getName();
		final String cuffcompare_out = conf.get("grid.output.dir","");
		Path hdfs_job_path = new Path(fs.getHomeDirectory().toString() + "/" + jobdirname);
		FileOutputStream fout = null;
		String line = null;
		List<String> filelist = new ArrayList<String>();

		int ret;
		try{
			System.out.println("copying the input files ");

			fs.copyToLocalFile(false,new Path(cuffcompare_in), new Path(workingdir));
			fs.copyToLocalFile(false,new Path(ref_gtf), new Path(workingdir));

			System.out.println(" Input files are loaded  ");

			if(new File(workingdir + "/" + infile).isFile()){
				System.out.println("Input file  : " + cuffcompare_in+ " ");
			}
			BufferedReader reader = new BufferedReader(new FileReader(new File(workingdir + "/" +infile)));
			while((line = reader.readLine()) != null){
				filelist.add(line);
			}
			if(!FileUtil.copyFromHdfs(fs, filelist, workingdir)){
				System.err.println("Error copying the input files");
			}
			cufflinks_cmd =  String.format("%s/cuffcompare -i %s/%s -o %s -r %s/%s",
					workingdir,workingdir,infile,cuffcompare_out,workingdir,ref_gtfname);
			String cmd = cufflinks_cmd;
			System.out.println("Executing Cuffcompare Command : " + cmd);
			ProcessUtil.runCommandEOE(new Command(cufflinks_cmd));
			logger.info("Copying Results to hdfs : " +  hdfs_job_path);
			if(!fs.exists(hdfs_job_path)){
				fs.mkdirs(hdfs_job_path);
			}
			fs.copyFromLocalFile(false,new Path( workingdir + "/" +cuffcompare_out+".combined.gtf") , hdfs_job_path);
			fs.copyFromLocalFile(false,new Path( workingdir + "/" +cuffcompare_out+".loci") , hdfs_job_path);
			fs.copyFromLocalFile(false,new Path( workingdir + "/" +cuffcompare_out+".stats") , hdfs_job_path);
			fs.copyFromLocalFile(false,new Path( workingdir + "/" +cuffcompare_out+".tracking") , hdfs_job_path);
			fs.deleteOnExit(new Path("cuffcompare-"+jobname+".txt"));
			logger.info(" Cuffcompare Process is Complete  ");
	
		}catch(Exception e){
			e.printStackTrace();
		}
		return true;	
	}	
}

