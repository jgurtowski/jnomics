package edu.cshl.schatz.jnomics.util;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    
    
}
