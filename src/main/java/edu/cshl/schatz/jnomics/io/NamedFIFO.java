package edu.cshl.schatz.jnomics.io;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.util.Shell;


public class NamedFIFO extends File{
	
	private static final long serialVersionUID = 1L;
	
	public NamedFIFO(String pathname) {
		super(pathname);
	}

    @Override
	public boolean createNewFile() throws IOException{
		Shell.execCommand("mkfifo", this.toString());
        if(!this.exists()){
            return false;
        }
        return true;
    }

}
