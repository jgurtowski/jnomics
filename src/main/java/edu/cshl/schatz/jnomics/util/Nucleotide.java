package edu.cshl.schatz.jnomics.util;

public class Nucleotide {

    /**
    * For encoding Nucleotide letters into bits
    * A = 00000000
    * C = 00000001
    * G = 00000010
    * N = 00000011
    * T = 00000100
    * These can be or'd with an existing byte to pack them into the byte
    */
    public static final byte[] charMap = new byte[]{0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,(byte)1,
            0,0,0,(byte)2,0,0,0,0,0,0,(byte)3,0,0,0,0,0,(byte)4};

    public static final char[] reverseCharMap = new char[]{'A','C','G','N','T'};

    public static final char[] reverseComplementCharMap = new char[]{'T','G','C','N','A'};
    

    public static byte[] reverseComplement(byte[] in, int start, int end){
        byte[] n = new byte[end-start];
        for(int i =start; i< end;i++){
            n[(end-start) - 1 - (i-start)] = (byte) Nucleotide.reverseComplementCharMap[Nucleotide.charMap[(byte)in[i]]];
        }
        return n;
    }

    public static String reverseComplement(String in){
        return new String(reverseComplement(in.getBytes(),0,in.length()));
    }

    /**
     * Does not reverse, only complements
     * @param in string to comlement
     * @return complemented string
     */
    public static String complement(String in){
        StringBuilder builder = new StringBuilder();
        for(int i =0 ; i< in.length(); i++){
            builder.append(reverseComplementCharMap[Nucleotide.charMap[(byte)in.charAt(i)]]);
        }
        return builder.toString();
    }
    
    public static char complement(char in){
        return reverseComplementCharMap[Nucleotide.charMap[(byte)in]];
    }

}
