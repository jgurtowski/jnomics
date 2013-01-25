package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.util.BlastUtil;
import edu.cshl.schatz.jnomics.util.Nucleotide;

import java.io.OutputStream;
import java.util.*;

/**
 * Given a PacBio Read and Blast Alignments to that read,
 * Try to correct it
 * User: james
 */
public class PacbioCorrector {

    /**
     * Container for incoming alignments
     */
    public static class PBBlastAlignment{
        private int alignmentStart;
        private int alignmentEnd;
        private String blastTraceback;

        public PBBlastAlignment(){

        }
        
        public PBBlastAlignment(int alignmentStart, int alignmentEnd, String blastTraceback){
            this.alignmentStart = alignmentStart;
            this.blastTraceback = blastTraceback;
            this.alignmentEnd = alignmentEnd;
        }
        
        public boolean isReverse(){
            return alignmentStart > alignmentEnd ? true: false;
        }

        public int getAlignmentEnd() {
            return alignmentEnd;
        }

        public void setAlignmentEnd(int alignmentEnd) {
            this.alignmentEnd = alignmentEnd;
        }

        public int getAlignmentStart() {
            return alignmentStart;
        }

        public void setAlignmentStart(int alignmentStart) {
            this.alignmentStart = alignmentStart;
        }

        public String getBlastTraceback() {
            return blastTraceback;
        }

        public void setBlastTraceback(String blastTraceback) {
            this.blastTraceback = blastTraceback;
        }
    }

    public static class PBPileup{
        //has pileup information
        //for each possible character at each position
        //keep a count of how many times it occurs
        private ArrayList<HashMap<Character,Integer>> pileup = new ArrayList<HashMap<Character, Integer>>();
        
        public PBPileup(int capacity){
            reset(capacity);
        }

        public HashMap<Character,Integer> get(int idx){
            return pileup.get(idx);
        }

        public int size(){
            return pileup.size();
        }
        
        /**
         * Ensure that the pileup matrix has enough elements
         * to process the read, and clear unnecessary items
         * @param capacity - number of elements minimum
         */
        public void reset(int capacity){
            //add elements if necessary
            int additional_elements = capacity - pileup.size();
            for( int i =0; i< additional_elements; i++){
                pileup.add(new HashMap<Character,Integer>());
            }

            //clear out existing pileup info
            for(int j = 0; j < capacity; j++){
                pileup.get(j).clear();
            }
        }

        public void printPileup(String read_sequence, OutputStream out) throws Exception{
            assert(pileup.size() == read_sequence.length());
            for(int j =0 ; j< pileup.size(); j++){

                ArrayList<Map.Entry<Character,Integer>> l = new ArrayList(pileup.get(j).entrySet());
                Collections.sort(l, new Comparator<Map.Entry<Character, Integer>>() {
                    @Override
                    public int compare(Map.Entry<Character, Integer> o1, Map.Entry<Character, Integer> o2) {
                        return o2.getValue() - o1.getValue();
                    }
                });
                out.write(new String((j+1) + "\t" + read_sequence.charAt(j) + "\t").getBytes());
                for(int i =0;i<l.size();i++){
                    out.write(new String(l.get(i).getKey() + ":" + l.get(i).getValue() + " ").getBytes());
                }
                out.write("\n".getBytes());
            }
        }
    }

    public static class PBCorrectionResult{

        public static class PBBaseCoverageStatistics{
            
            private final HashMap<Integer,Long> coverageHash = new HashMap<Integer, Long>();

            private int totalDatum;

            private int[] coverage_bins;
            
            public PBBaseCoverageStatistics(int []bins){
                coverage_bins = bins;
                reset();
            }
            
            /**
             * Record the coverage for a base
             * @param coverage - the current coverage
             */
            public void insertDatum(int coverage){
                totalDatum += 1;
                for(Integer k : coverageHash.keySet()){
                    if(coverage >= k)
                        coverageHash.put(k,coverageHash.get(k) + 1);
                }
            }

            public void reset(){
                coverageHash.clear();
                for(int i=0;i<coverage_bins.length;i++){
                    coverageHash.put(coverage_bins[i],new Long(0));
                }
                totalDatum = 0;
            }

            public int getTotalDatum(){
                return totalDatum;
            }
            
            public long getCountForBin(int bin){
                return coverageHash.get(bin);
            }
        }

        private PBPileup pileup = new PBPileup(0);
        //{Position: {deletion:count}}
        private HashMap<Integer,HashMap<String,Integer>> deletiondb = new HashMap<Integer, HashMap<String, Integer>>();

        private String original_read, corrected_read;

        private PBBaseCoverageStatistics coverageStats;

        /**
         * @param original_read - original read
         * @param coverage_stat_bins - array of bins that will be used to aggregate coverage
         */
        public PBCorrectionResult(String original_read, int[] coverage_stat_bins){
            coverageStats = new PBBaseCoverageStatistics(coverage_stat_bins);
            reset(original_read);
        }
        
        public PBCorrectionResult(String original_read){
            this(original_read,new int[0]);
        }

        /**
         * Reset/construct the correction result object
         * @param original_read - the original read to be corrected
         */
        public void reset(String original_read){
            pileup.reset(original_read.length());
            deletiondb.clear();
            coverageStats.reset();
            this.original_read = original_read;
        }

        public HashMap<Integer,HashMap<String,Integer>> getDeletiondb(){
            return deletiondb;
        }

        public PBPileup getPileup(){
            return pileup;
        }

        public String getOriginalRead(){
            return original_read;
        }
        
        public String getCorrectedRead(){
            return corrected_read;
        }
        
        public void setCorrectedRead(String correctedRead){
            this.corrected_read = correctedRead;
        }

        /**
         * Get the coverage statistics for this read
         * @return Coverage statistics for the read
         */
        public PBBaseCoverageStatistics getCoverageStatistics(){
            if(coverageStats.getTotalDatum() == 0){
                for(int i=0; i<pileup.size(); i++){
                    int total = 0;
                    for(Integer cnt: pileup.get(i).values()){
                        total += cnt;
                    }
                    coverageStats.insertDatum(total);
                }
            }
            return coverageStats;
        }
    }
    
    private static final Character MATCH_CHAR = ',';


    public static void correct(String read, Iterable<PBBlastAlignment> alignments, PBCorrectionResult results){
        results.reset(read);

        alignmentParse(alignments,results);
        correctRead(read,results);
    }


    /**
     * From Blast Alignments, populate a pileup table
     * As well as a deletion table
     * @param alignments alignments to use for pileups
     * @param result Results of the correction
     */
    private static void alignmentParse(Iterable<PBBlastAlignment> alignments,
                                      PBCorrectionResult result){

        //parse the blast alignment string and populate pileup matrix
        HashMap<Character,Integer> pos_hash;
        for(PBBlastAlignment balign : alignments){
            int pos = balign.getAlignmentStart() - 1;
            boolean reverse_alignment = balign.getAlignmentStart() > balign.getAlignmentEnd() ? true : false;
            for(String task: BlastUtil.splitBlastTraceback(balign.getBlastTraceback())){
                if(Character.isDigit(task.charAt(0))){
                    //digit infers match, populate with MATCH_CHAR
                    for(int i=0;i<Integer.parseInt(task);i++){
                        pos_hash = result.getPileup().get(pos);
                        if(!pos_hash.containsKey(MATCH_CHAR))
                            pos_hash.put(MATCH_CHAR,1);
                        else
                            pos_hash.put(MATCH_CHAR,pos_hash.get(MATCH_CHAR)+1);
                        pos = reverse_alignment ? pos - 1 : pos + 1;
                    }
                }else{//we have a mutation string
                    String [] mergedTasks = BlastUtil.mergeTracebackDeletions(BlastUtil.splitTracebackMutations(task));
                    for(String t: mergedTasks){
                        if(t.charAt(1) == '-'){//reference deletion
                            //populate deletion hash and if there is enough
                            //evidence we'll add the missing base at the end.
                            //the position of the deletion(s) is right before
                            //the 'pos' found in the deletion db
                            String deletion = t.replaceAll("-","");
                            deletion = reverse_alignment ? Nucleotide.complement(deletion) : deletion;
                            //if we're on the reverse strand, we want the deletion to be registered
                            //in one spot ahead of where it should go relative to the first strand
                            int dpos = reverse_alignment ? pos + 1 : pos;
                            if(!result.getDeletiondb().containsKey(dpos))
                                result.getDeletiondb().put(dpos, new HashMap<String, Integer>());
                            HashMap<String,Integer> hash_pos = result.getDeletiondb().get(dpos);
                            if(!hash_pos.containsKey(deletion))
                                hash_pos.put(deletion,1);
                            else
                                hash_pos.put(deletion, hash_pos.get(deletion) + 1);
                        }else{ //point mutation
                            char c = t.charAt(0);
                            if(c != '-')
                                c = reverse_alignment ? Nucleotide.complement(t.charAt(0)) : t.charAt(0);
                            pos_hash = result.getPileup().get(pos);
                            if(!pos_hash.containsKey(c))
                                pos_hash.put(c,1);
                            else
                                pos_hash.put(c,pos_hash.get(c)+1);
                            pos = reverse_alignment ? pos - 1 : pos + 1;
                        }
                    }
                }
            }
        }
    }


    private static void correctRead(String sequence,
                                     PBCorrectionResult result){

        StringBuilder correctedRead = new StringBuilder();

        HashMap<Character,Integer> pup_hash;

        int[] maxs = new int[sequence.length()];
        for(int z=0; z< sequence.length(); z++){
            if(!result.getPileup().get(z).isEmpty())
                maxs[z] = Collections.max(result.getPileup().get(z).values());
            else
                maxs[z] = 0;
        }

        double max_sum = 0;
        int max_max = 0, max_min = Integer.MAX_VALUE;
        for(int l = 0; l< maxs.length;l++){
            max_sum += maxs[l];
            if(maxs[l] > max_max)
                max_max = maxs[l];
            if(maxs[l] < max_min)
                max_min = maxs[l];
        }

        double max_avg = max_sum / maxs.length;
        double max_std = (max_max - max_min) / 4.0;
        double max_lstd = max_avg - max_std;
        double max_rstd = max_avg + max_std;

        //iterate through each pileup and determine the best candidate for correction
        for(int i=0; i< sequence.length(); i++){ //for base i
            pup_hash = result.getPileup().get(i);

            int current_max = maxs[i];

            //check if we need to re-insert some deletions
            if(result.getDeletiondb().containsKey(i)){
                HashMap<String,Integer> dels_at_pos = result.getDeletiondb().get(i);
                int max_del_occur = Collections.max(dels_at_pos.values());
                if(max_del_occur > max_lstd && max_del_occur < max_rstd){
                    //we're in 1 standard deviation of the other maxs
                    //find the max deletion
                    for(Map.Entry<String,Integer> e: dels_at_pos.entrySet()){
                        if(e.getValue() == max_del_occur){
                            //found our guy, insert into the corrected read
                            correctedRead.append(e.getKey());
                            break;
                        }
                    }
                }
            }

            //if there is no coverage on this part of the read, leave it alone
            if(pup_hash.entrySet().isEmpty()){
                correctedRead.append(sequence.charAt(i));
                continue;
            }

            //Just fix the other stuff by choosing the max value
            for(Map.Entry<Character,Integer> entry : pup_hash.entrySet()){
                if(entry.getValue() == current_max){ //perform this operation
                    if(entry.getKey() == MATCH_CHAR){ //same as reference
                        correctedRead.append(sequence.charAt(i));
                    }else if(entry.getKey() != '-'){//Base mutation, just change it
                        correctedRead.append(entry.getKey());
                    }
                    //if we have a '-' we want to delete that base, so just
                    //don't add it to the corrected read
                    break; // found the max, we're done
                }
            }
        }

        //if deletion at the end
        if(result.getDeletiondb().containsKey(sequence.length())){
            HashMap<String,Integer> dels_at_pos = result.getDeletiondb().get(sequence.length());
            int max_del_occur = Collections.max(dels_at_pos.values());
            if(max_del_occur > max_lstd && max_del_occur < max_rstd){
                //we're in 1 standard deviation of the other maxs
                //find the max deletion
                for(Map.Entry<String,Integer> e: dels_at_pos.entrySet()){
                    if(e.getValue() == max_del_occur){
                        //found our guy, insert into the corrected read
                        correctedRead.append(e.getKey());
                        break;
                    }
                }
            }
        }
        
        result.setCorrectedRead(correctedRead.toString());
    }

}
