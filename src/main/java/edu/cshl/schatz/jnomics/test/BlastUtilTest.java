package edu.cshl.schatz.jnomics.test;

import edu.cshl.schatz.jnomics.util.BlastUtil;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 * Tests some of the utility functions in PBCorrectReduce
 * User: james
 */
public class BlastUtilTest extends TestCase {
    private String test1 = "13CT3G-4-G24TC1TCAG8CT-T18T-1CG28CT7";
    private String test2 = "8-G11AC11-G-G11A-G-GC2A-34";

    private String test3 = "8-GG-C-G-T-CC12-C-GC-G-G--C";
    
    public void testBlastTracebackParse(){

        String[] test1_exp = {"13","CT","3","G-","4","-G","24","TC","1","TCAG","8","CT-T","18","T-","1","CG","28","CT","7"};
        String[] test2_exp = {"8","-G","11","AC","11","-G-G","11","A-G-GC","2","A-","34"};
        String[] test3_exp = {"8","-GG-C-G-T-CC","12","-C-GC-G-G--C"};
        
        Assert.assertArrayEquals(test1_exp, BlastUtil.splitBlastTraceback(test1));
        Assert.assertArrayEquals(test2_exp, BlastUtil.splitBlastTraceback(test2));
        Assert.assertArrayEquals(test3_exp, BlastUtil.splitBlastTraceback(test3));
    }

    public void testTracebackMutations(){
        String mutations = BlastUtil.splitBlastTraceback(test2)[7];
        String[] splitMutations = BlastUtil.splitTracebackMutations(mutations);
        String[] mergedDeletions = BlastUtil.mergeTracebackDeletions(splitMutations);
        String[] mergedDeletions_exp = {"A-G-","GC"};
        Assert.assertArrayEquals(mergedDeletions_exp,mergedDeletions);

        String[] tracebackSplit = BlastUtil.splitBlastTraceback(test3);
        String[] mergedMutations1 = BlastUtil.mergeTracebackDeletions(BlastUtil.splitTracebackMutations(tracebackSplit[1]));
        String[] mergedMutations1_exp = {"-G","G-C-G-T-","CC"};

        Assert.assertArrayEquals(mergedMutations1_exp,mergedMutations1);
        
        String[] mergedMutations2 = BlastUtil.mergeTracebackDeletions(BlastUtil.splitTracebackMutations(tracebackSplit[3]));
        String[] mergedMutations2_exp = {"-C","-G","C-G-G-","-C"};

        Assert.assertArrayEquals(mergedMutations2_exp,mergedMutations2);

    }
}
