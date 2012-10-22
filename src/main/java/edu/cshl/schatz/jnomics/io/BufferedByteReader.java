package edu.cshl.schatz.jnomics.io;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

public class BufferedByteReader {

	private byte[] buffer;
	private int amtInBuf;
	private int amtOut;
	private long currentPos;
	private FSDataInputStream fin;
	
	BufferedByteReader(FSDataInputStream in){
		this(in, 64 * 1024);		
	}
	
	BufferedByteReader(FSDataInputStream in, int bufsize){
		buffer = new byte[bufsize];
		fin = in;
		amtInBuf =0;
		amtOut =0;
	}
	
	private void fill() throws IOException{
		currentPos = fin.getPos();
		amtOut = 0;
		amtInBuf = fin.read(buffer);		
	}
	
	public long getPos(){
		return currentPos + amtOut;
	}
	
	public void close() throws IOException{
		fin.close();
	}
	
	public void seek(long desired) throws IOException{
		amtInBuf = 0; //invalidate previously read data
		fin.seek(desired);		
	}
	
	/*
	 * Move back one position
	 */
	public void previous() throws IOException{
		if(amtOut > 0)
			amtOut--;
		else
			seek(currentPos -1); //kills your cache but hopefully doesn't happen often
	}
	
	public byte readByte() throws IOException{
		if(amtOut >= amtInBuf)
			fill();
		if(amtInBuf == -1)
			return -1;
		return buffer[amtOut++];
	} 
	
}
