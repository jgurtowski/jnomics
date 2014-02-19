package edu.cshl.schatz.jnomics.util;

import org.apache.http.HttpHost;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.kbase.auth.AuthToken;
import us.kbase.shock.client.BasicShockClient;
import us.kbase.shock.client.ShockNode;
import us.kbase.shock.client.ShockNodeId;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * User: james
 */
public class FileUtil {
    
    final static Logger logger = LoggerFactory.getLogger(FileUtil.class);

    public static InputStream getInputStreamWrapperFromExtension(InputStream inStream, String extension) throws IOException {
        if(extension.compareTo(".gz") == 0){
            return new GZIPInputStream(inStream);
        }else if(extension.compareTo(".bz2") == 0){
            return new BZip2CompressorInputStream(inStream);
        }else if(extension.compareTo(".txt") == 0){
            return inStream;
        }

        logger.warn("Unknown Extension returning original InputStream");
        return inStream;
    }
    
    public static String getExtension(String name){
        if(name.contains("."))
            return name.substring(name.lastIndexOf("."));
        return new String();
    }
    
    public static void markDeleteOnExit(File[] files){
        for(File f : files){
            f.deleteOnExit();
        }
    }
    
    public static void removeFiles(File[] files){
        for(File f: files){
            f.delete();
        }
    }

    public static void removeFilesIfExist(File [] files){
        for(File f : files){
            if(f.exists())
                f.delete();
        }
    }
    public static void makefiledirs(String filepath){
    	File dir = new File(filepath).getParentFile();
    	dir.mkdirs();
    	
    }
   
    public static boolean copyFromHdfs(FileSystem fs, List<String> files, String  dest) throws IOException{
		for(String file :  files ) {
			logger.info("file in FileUtil is " + file);
			File dir = new File(dest + "/" + file).getParentFile();
			logger.info("dir is " + dir.getAbsolutePath());
			logger.info(" file is " + file + "dir is " + dir);
			//if(!dir.exists()){
			dir.mkdirs();
			//}
//			fs.copyToLocalFile(false,new Path(fs.getHomeDirectory().toString() + "/" + file), new Path(dir.toString()));
			try{
			fs.copyToLocalFile(false,new Path(file), new Path(dir.getAbsolutePath()));
			}catch(Exception e){
				throw new IOException(e.toString());			}
		}
    	return true;
    	
    }

    public static boolean copyToHdfs(FileSystem fs, List<String> files, String  dest) throws IOException{
		for(String file :  files ) {
			fs.copyToLocalFile(false,new Path(file) , new Path(fs.getHomeDirectory().toString() + "/" + dest) );
		}
    	return true;
    	
    }
    public static String copyToShock(String url,String token,String proxy,FileSystem fs, String filename) throws IOException{
  
    	String id = null;
    	BasicShockClient base = null;
    	FSDataInputStream fsin = null;
    	String[] arr ;
    	try { 
    		logger.info("url : " + url);
    		logger.info("token :" + token);
    		logger.info("filename :" + filename);
    		logger.info("Proxy is :" + proxy);
    		Path pfilename = new Path(filename);
    		String file = pfilename.getName();
    		if(!proxy.equals("")){
    			logger.info("Getting in the if");
    	    	arr = proxy.split(":");
    	    	base = new BasicShockClient(new URL(url),new HttpHost(arr[0],Integer.parseInt(arr[1])),new AuthToken(token));
    		}else{
    			base = new BasicShockClient(new URL(url),null,new AuthToken(token));
    		}
    		logger.info("Shock java client authenticated");
    		logger.info("File name is " + file);
    		fsin = fs.open(pfilename);
    		
    		ShockNode sn = base.addNode(fsin, file, null);
    		id = sn.getId().toString();
    		logger.info("Copied file : " + filename + " to Shock"  );
    		logger.info("Node id : " + id  );
    	}catch (Exception e) {
    		logger.error(e.toString());
    		throw new IOException(e.toString());
    	}finally{
    		fsin.close();
    	}
    	return id;
    	
    }

    public static boolean copyFromShock(String url,String token,String proxy,FileSystem fs, String nodeid, String  dest) throws IOException{
    	//String id = null;
    	BasicShockClient base = null;
    	FSDataOutputStream fsout = null;
    	//OutputStream out;
    	String[] arr;
    	ShockNode sn ;
    
    	try { 
    		logger.info("url : " + url);
    		logger.info("token :" + token);
    		logger.info("filename :" + nodeid);
    		logger.info("dest :" + dest);
    		logger.info("Proxy is :" + proxy);
    		if(!proxy.equals("")){
    			logger.info("Getting in the if");
    	    	arr = proxy.split(":");
    	    	base = new BasicShockClient(new URL(url),new HttpHost(arr[0],Integer.parseInt(arr[1])),new AuthToken(token));
    		}else{
    			base = new BasicShockClient(new URL(url),null,new AuthToken(token));
    		}
    		fsout = fs.create(new Path(dest),true);
    		base.getFile(new ShockNodeId(nodeid.trim()),fsout);
    	
    	}catch (Exception e) {
    		logger.error(e.toString());
    		throw new IOException(e.toString());
    	}finally{
    		fsout.close();
    	}
    	return true;
    }
    
    public static boolean untar(FileSystem fs, String filename, String dest) throws IOException{
    	try {
			org.apache.hadoop.fs.FileUtil.unTar(new File(filename),new File(dest));
			logger.info(" Untaring File :  " + filename + "to dest : " + dest);
		} catch (Exception e) {
			throw new IOException(e.toString());
		}
		return true;
    }
}
