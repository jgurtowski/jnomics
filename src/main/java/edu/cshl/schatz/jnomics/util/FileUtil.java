package edu.cshl.schatz.jnomics.util;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

    
}
