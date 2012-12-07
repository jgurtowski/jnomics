package edu.cshl.schatz.jnomics.test;

import edu.cshl.schatz.jnomics.util.Nucleotide;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 * User: james
 */
public class NucleotideTest extends TestCase{

    public void testComplement(){
        String t1 = "ACGAGANGACACCAN";
        String t1_exp = "TGCTCTNCTGTGGTN";
        String t2 = "CCCNNNCANCACNNAC";
        String t2_exp = "GGGNNNGTNGTGNNTG";
        
        Assert.assertEquals(t1_exp, Nucleotide.complement(t1));
        Assert.assertEquals(t2_exp, Nucleotide.complement(t2));
    }

}
