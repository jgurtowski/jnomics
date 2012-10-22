/**
 * 
 * @file	-	AlignmentSortExtractPiyush.java
 * 
 * @purpose	-	Extracts output of AlignmentSortMap to local
 * 
 * @author 	-	Piyush Kansal
 *
 * @note	-	As per the assumption of this program, all the records of one single file should belong to one single bin
 * 
 */

package edu.cshl.schatz.jnomics.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * 
 * @class	-	AlignmentSortExtractPiyush
 * @purpose	-	To define a class to implement file extraction
 * @author 	-	Piyush Kansal
 *
 */
public class AlignmentSortExtractPiyush {

	private static final String 			_SQ_TAG_			= "@SQ";
	private static final String 			_SN_TAG_			= "SN:";
	private static final String 			_RG_TAG_			= "@RG";
	private static final String 			_PG_TAG_			= "@PG";
	private static final String 			_CO_TAG_			= "@CO";
	private static final String 			_HD_TAG_			= "@HD";
	
	private static final String 			_TAB_				= "\t";
	private static final String 			_HYPHEN_			= "-";
	private static final String 			_NEW_LINE_			= "\n";
	private static final String 			_IP_FILE_PREFIX_	= "";

	private static final char 				_TAB_CHAR_			= '\t';
	private static final char 				_SPACE_CHAR_		= ' ';

	/*
	 * Make sure that this name remains same in AlignmentSortMap.java
	 */
	private static final String 	_HEADER_FILE_		= "header.sam";
	private static final String 	_UNMAPPED_			= "~~UNMAPPED";

	public static class HeaderPathFilter implements PathFilter {
		
		private final String fName;
		
		public HeaderPathFilter(String ipName ) {
		    this.fName = "/" + ipName;
		}
		
		public boolean accept( Path path ) {
			return path.toString().contains( fName );
		}
	}
	
	public static void main( String[] args ) throws IOException {

		/**
		 * Validate i/p parameters
		 */
		if( args.length != 2 ) {
			System.err.println( "Usage: " + AlignmentSortExtractPiyush.class + " <ip-dir-on-hdfs> <op-filename-on-local-fs>" );
			System.exit( 1 );
		}

		Path ip = new Path( args[0] );
		Path op = new Path( args[1] );

		Configuration conf = new Configuration();
        FileSystem ipFs = FileSystem.get( conf );

        /**
         * Check if the i/p path exists
         */
        if( !ipFs.exists( ip ) ) {
        	System.err.println( "Path: " + ip + " does not exist" );
            System.exit( 1 );
        }
        
        /**
         * Check if the i/p path is a directory
         */
        if( !ipFs.getFileStatus( ip ).isDir() ) {
        	System.err.println( "First argument to " + AlignmentSortExtractPiyush.class + " should be a directory" );
        	System.err.println( "Usage: " + AlignmentSortExtractPiyush.class + " <ip-dir-on-hdfs> <op-filename-on-local-fs>" );
            System.exit( 1 );
        }

        /**
         * Check if the o/p file already exists
         */
        LocalFileSystem opFs = FileSystem.getLocal( conf );
        if( opFs.exists( op ) ) {
            System.err.println( "File: " + op.getName()  + " already exists. Please choose a different file name" );
            System.exit( 1 );
        }

        /*
         * 1. Open header file
         * 2. Read one chromosome one by one
         * 		a) Find all the files starting with the same in the i/p dir
         * 		b) Sort them with alignment start
         * 		c) Write them to o/p file one by one
         * 3. Finally, write the unmapped chromosomes to the o/p file
         */
        FileStatus hdrFileStatus[] = ipFs.listStatus( ip, new HeaderPathFilter( _HEADER_FILE_ ) );
		if( 0 == hdrFileStatus.length ) {
			System.out.println( _HEADER_FILE_ + " not found in i/p directory. Error !!" );
			System.exit(1);
		}
		else if( 1 < hdrFileStatus.length ) {
			System.out.println( "More than one " + _HEADER_FILE_ + " found in i/p directory. Error !!" );
			System.exit(1);
		}

		Path headerFilePath = hdrFileStatus[0].getPath();
		BufferedReader br = new BufferedReader( new InputStreamReader( ipFs.open( headerFilePath ) ) );
		BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( opFs.create( op ) ) );

		/*
		 * First copy the header in the op file
		 */
		while( true ) {
			String curLine = br.readLine();
			if( null == curLine ) {
				break;
			}

			if( curLine.startsWith( _HD_TAG_ ) ) {
				continue;
			}

			bw.write( curLine + _NEW_LINE_ );
		}

		br.close();
		br = new BufferedReader( new InputStreamReader( ipFs.open( headerFilePath ) ) );

		while( true ) {
			String curLine = br.readLine();
			String allTags[] = curLine.split( _TAB_, 2 );

			/*
			 * Read a line at a time until you read all "SQ" tags
			 */
			if( allTags[0].trim().equals( _SQ_TAG_ ) && allTags[1].trim().contains( _SN_TAG_ ) ) {
				char allChars[] = allTags[1].trim().toCharArray();
				String curChr = "";
				
				int charIndex = 3;
				while( ( allChars[charIndex] != _SPACE_CHAR_ ) && ( allChars[charIndex] != _TAB_CHAR_ ) ) {
					curChr += allChars[charIndex];
					++charIndex;
				}
				
				/*
				 * Sort all the files present in i/p directory with "curChr" as name prefix using alignmentStart as key 
				 */
				FileStatus curChrFileStatus[] = ipFs.listStatus( ip, new HeaderPathFilter( curChr + _HYPHEN_ ) );
				TreeMap<Integer, Integer> sortAlgn = new TreeMap<Integer, Integer>();
				
				for( int i = 0 ; i < curChrFileStatus.length ; i++ ) {
					String curFileName = curChrFileStatus[i].getPath().getName();
					sortAlgn.put( Integer.parseInt( curFileName.substring( curFileName.indexOf( _HYPHEN_ ) + 1, curFileName.length() ) ), i );
				}
				
				/*
				 * Now using this sorted order, write down all the files one by one on local file system
				 */
				for( Entry<Integer, Integer> entry : sortAlgn.entrySet() ) {
					Path curChrPath = curChrFileStatus[entry.getValue()].getPath();
					BufferedReader curChrBr = new BufferedReader( new InputStreamReader( ipFs.open( curChrPath ) ) );
					
					while( true ) {
						String line = curChrBr.readLine();

						if( null == line )
							break;

						bw.write( line + _NEW_LINE_ );
					}
					
					curChrBr.close();
				}
			}
			else if( ( allTags[0].trim().equals(_RG_TAG_) ) || ( allTags[0].trim().equals(_PG_TAG_) ) || ( allTags[0].trim().equals(_CO_TAG_) ) ) {
				break;
			}
		}
		
		/*
		 * Finally read all the unmapped chromosomes and write it to the o/p file
		 */
		BufferedReader unmapBr = new BufferedReader( new InputStreamReader( ipFs.open( new Path( ip + "/" + _UNMAPPED_ ) ) ) );
		while( true ) {
			String line = unmapBr.readLine();

			if( null == line )
				break;

			bw.write( line + _NEW_LINE_ );
		}		
		
		unmapBr.close();
		br.close();
		bw.close();
	}
}