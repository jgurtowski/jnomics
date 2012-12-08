package edu.cshl.schatz.jnomics.util;

import java.util.ArrayList;

/**
 * Utility functions for dealing with blast output
 * User: james
 */
public class BlastUtil {

    /**
     * Splits blast traceback string into sections
     * of numbers and sections of letters
     * @param traceback the tracekback to be parsed
     * @return
     */
    public static String[] splitBlastTraceback(String traceback){
        // parse alignments from :
        //http://stackoverflow.com/questions/952614/help-on-a-better-way-to-parses-digits-from-a-string-in-java
        // preceded by a digit and followed by a non-digit, or
        // preceded by a non-digit and followed by a digit.
        return traceback.split("(?<=\\d)(?=\\D)|(?<=\\D)(?=\\d)");
    }

    /**
     * Splits the mutations
     * Everything is grouped by one basepair
     * @param mutations string of mutations from blast traceback
     * @return
     */
    public static String[] splitTracebackMutations(String mutations){
        return mutations.split("(?<=\\G.{2})");
    }

    /**
     * Merges Deletions (with respect to the reference) into on array element.
     * @param splitMutations mutations that have been split using splitTracebackMutations
     * @return
     */
    public static String[] mergeTracebackDeletions(String[] splitMutations){
        ArrayList<String> n = new ArrayList<String>();
        for(String s: splitMutations){
            if(s.charAt(1) == '-'){ //we have reference deletion
                if(n.size() > 0 && n.get(n.size()-1).charAt(1) == '-'){
                    //the previous base was also a reference deletion, so merge them
                    n.set(n.size()-1, n.get(n.size()-1) + s);
                    continue;
                }
            }
            n.add(s);
        }
        return n.toArray(new String[n.size()]);
    }

}
