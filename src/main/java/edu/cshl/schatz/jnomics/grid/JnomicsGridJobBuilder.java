package edu.cshl.schatz.jnomics.grid;

import org.apache.hadoop.conf.Configuration;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//unused imports
//import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
//import edu.cshl.schatz.jnomics.mapreduce.JnomicsJobBuilder;
//import edu.cshl.schatz.jnomics.util.TextUtil;
//import org.apache.commons.lang3.ArrayUtils;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.ggf.drmaa.JobInfo;
//import org.ggf.drmaa.Version;
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.Collections;
//import java.util.Properties;


/**
 * User: Sri
 */
public class JnomicsGridJobBuilder {

	private Configuration conf = null;
	List<String> archives = new ArrayList<String>();
	public static Map<Integer,String> returnCode =  new HashMap<Integer, String>();

	public JnomicsGridJobBuilder(Configuration conf){
		this.conf = conf;
		conf.set("hdfs-conf-path","/etc/hadoop/conf");
		conf.set("this.is.test","SUCCESSFUL");
		returnCode.put(0, "UNDETERMINED");
		returnCode.put(16, "QUEUED_ACTIVE");
		returnCode.put(17, "SYSTEM_ON_HOLD");
		returnCode.put(18, "USER_ON_HOLD");
		returnCode.put(19, "USER_SYSTEM_ON_HOLD");
		returnCode.put(32, "RUNNING");
		returnCode.put(33, "SYSTEM_SUSPENDED");
		returnCode.put(34, "USER_SUSPENDED");
		returnCode.put(35, "USER_SYSTEM_SUSPENDED");
		returnCode.put(48, "DONE");
		returnCode.put(64, "FAILED");
	}

	public JnomicsGridJobBuilder setParam(String name, String value){	
		conf.set(name,value);
		return this;
	}
	public JnomicsGridJobBuilder setInputPath(String in){
		conf.set("grid.input.dir",in);
		return this;
	}

	public JnomicsGridJobBuilder setOutputPath(String out){
		conf.set("grid.output.dir", out);
		return this;
	}

	public JnomicsGridJobBuilder addConfModifiers(Map<String,String> modifiers){
		for(Map.Entry<String,String> entry: modifiers.entrySet()){
			conf.set(entry.getKey(),entry.getValue());
		}
		return this;
	} 

	public JnomicsGridJobBuilder setJobName(String name){
		conf.set("grid.job.name", name);
		return this;
	}  
	
//	public JnomicsGridJobBuilder scriptCommandbuilder(){
//		String hdfs_conf = conf.get("hdfs-conf-path");
//		System.out.println("hdfs_conf is " + hdfs_conf );
//		//		String jarfile = conf.get("mapred.jar");   ///change this configuration
//		String jarfile = conf.get("grid_jar_file");
//		String classpath = conf.get("grid_class_path");
//		String functionname =  conf.get("calling_function");
//		//String args = conf.get("tophat_align_opts");
//		System.out.println("Commandbuilder Conf is " + conf.get("this.is.test"));
//		System.out.println("Inside Command builder" + jarfile + " " +  classpath);
//		String command = String.format("$HOME/sources/jdk1.7.0_25/bin/java -cp %s:%s:%s %s ",hdfs_conf,jarfile,classpath,functionname);
//		conf.set("grid_script_command", command);
//		System.out.println("Scriptcommand builder Conf is " + conf.get("grid_script_command"));
//		return this;
//	}

//	public JnomicsGridJobBuilder createjobScript() throws Exception {
//		System.out.println("createjobScript Conf is " + conf.get("this.is.test"));
//		String jobname = conf.get("grid.job.name");
//		String command = conf.get("grid_script_command");
//		String workingdir = conf.get("grid_working_dir");
//		File scriptfile  = new File(System.getProperty("user.home") + "/" + jobname  + ".sh");
//		String usrhome = System.getProperty("user.home");
//		String line = null;
//		if(scriptfile.canWrite()) {
//			System.out.println("I can write into the file");
//		}
//		PrintWriter pw = null;
//		try {
//			//reader  = new BufferedReader(new FileReader(Templatescript));
//			pw = new PrintWriter(new FileWriter(scriptfile));
//			//while((line = reader.readLine()) != null) {
//			//	pw.println(line);
//			//}
//			pw.println("#!/bin/sh");
//			pw.println("#$ -S /bin/sh");
//			pw.println("#$ -N "+ jobname);
//			pw.println("source $HOME/.bashrc");
//			if(!workingdir.isEmpty()){
//				pw.println("temp=" + usrhome + "/" + workingdir);
//				if(!new File(usrhome + "/"+ workingdir).exists()){
//					pw.println("mkdir " + usrhome + "/" + workingdir);
//				}
//				pw.println("cd " + usrhome + "/" + workingdir);
//			}else {
//				pw.println("temp=$TMPDIR");
//				pw.println("cd $TMPDIR");
//			}
//			pw.println(command);
//			//pw.println(command + " $temp");
//			//pw.println("cd " +  workingdir);	
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}finally{
//		//	reader.close();
//			pw.close();
//		}
//		scriptfile.setExecutable(true);
//		conf.set("grid_script_file",scriptfile.getAbsolutePath());
//		return this;
//	}
	
	public JnomicsGridJobBuilder LaunchGridJob(Configuration conf) throws Exception{
		String scriptfile = new File( new File(".").getAbsolutePath() + "/safe_bin/kbasetest.sh" ).getAbsolutePath();
		String workingdir = conf.get("grid_working_dir");
		String jobname = conf.get("grid.job.name");
		SessionFactory factory = SessionFactory.getFactory();
		Session session = factory.getSession();
		JobTemplate jt = null;
		String jobId = null;
		try {
			session.init("");
			jt = session.createJobTemplate();
			jt.setRemoteCommand(scriptfile+" "+workingdir+":"+jobname);
			//jt.setArgs(Collections.singletonList("5"));
			//jt.setWorkingDirectory(workingdir);
			//jt.setErrorPath(":" + workingdir);
			//jt.setOutputPath(":" + workingdir);
			jt.setJobName(jobname);
			jobId = session.runJob(jt);
			System.out.println("Jobname is : " + jt.getJobName());
		}catch(Exception e){
			throw new Exception();
		}finally{
			session.exit();

		}
		conf.set("grid_jobId",jobId);
		return this ;
	}

	public String getjobstatus(String JobId) throws DrmaaException{
		SessionFactory factory = SessionFactory.getFactory();
		Session session = factory.getSession();
		int ret = 0;
		try {
			session.init("");
			ret  = session.getJobProgramStatus(JobId);

		} catch (DrmaaException e) {
			e.printStackTrace();
		}finally{
			session.exit();
		}
		return returnCode.get(ret);
	}

	private void ifNotSetConf(String key, String value){
		if(null == conf.get(key,null))
			conf.set(key,value);
	}

	public JnomicsGridJobBuilder setMaxSplitSize(int num){
		conf.setInt("mapred.max.split.size",num);
		return this;
	}

	/**Returns Arguments that are wanted by the mapper and/or reducer **/
	//    public JnomicsArgument[] getArgs() throws Exception{
	//        JnomicsMapper mapperInst = mapper.newInstance();
	//        JnomicsArgument[] args = mapperInst.getArgs();
	//        if(null != reducer){
	//            JnomicsReducer reducerInst = reducer.newInstance();
	//            args = ArrayUtils.addAll(args, reducerInst.getArgs());
	//        }
	//        return args;
	//    }

	public Configuration getJobConf() throws Exception {
		return this.conf;
	}


}
