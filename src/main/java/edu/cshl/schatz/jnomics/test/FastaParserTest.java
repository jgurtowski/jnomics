package edu.cshl.schatz.jnomics.test;

import edu.cshl.schatz.jnomics.io.FastaParser;
import junit.framework.TestCase;

import java.io.ByteArrayInputStream;

/**
 * User: james
 */
public class FastaParserTest extends TestCase {

    private String fastaFileTest = ">line 1\n" +
            "ACACACAAAAAAAAAAA\n"  +
            ">entry 2\n" +
            "ACCCCCCCCCCCAGAGAGA\n" +
            "ACACACCCCCCCCCCCCCC\n" +
            ">entry3,empty\n" +
            ">entry4\n"+
            "hi\n";

    private String parsedFastaFileExpected = "line 1\n" +
            "ACACACAAAAAAAAAAA\n"  +
            "entry 2\n" +
            "ACCCCCCCCCCCAGAGAGA" +
            "ACACACCCCCCCCCCCCCC\n" +
            "entry3,empty\n\n" +
            "entry4\n"+
            "hi\n";

    public void testParse(){
        FastaParser parser = new FastaParser(new ByteArrayInputStream(fastaFileTest.getBytes()));
        String parsedData = "";
        for(FastaParser.FastaRecord record : parser){
            parsedData += record.getName() + "\n";
            parsedData += record.getSequence() + "\n";
        }
        assertEquals(parsedFastaFileExpected, parsedData);
    }

}
