package edu.cshl.schatz.jnomics.util;

import java.util.HashMap;
import java.util.Map;

/**
 * User: james
 */
public class SequenceOpts {

    public static final Map<Character,Character> nucleotideMap = new HashMap<Character,Character>(){
        {
            put('A','T');
            put('T','A');
            put('G','C');
            put('C','G');
            put('N','N');
        }
    };

    public static String reverseComplement(String s){
        StringBuffer buffer = new StringBuffer(s).reverse();
        for(int i=0; i < buffer.length(); i++){
            buffer.setCharAt(i,nucleotideMap.get(buffer.charAt(i)));
        }
        return buffer.toString();
    }
}
