package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.FastaParser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * User: james
 */
public class PacbioCorrectorMain {
    
    public static void main(String []args) throws Exception {
        if(args.length != 2){
            System.out.println("<read.fa> <tab_blast.alignments>");
            System.exit(1);
        }

        FastaParser fastaParser = new FastaParser(new FileInputStream(args[0]));
        Iterator<FastaParser.FastaRecord> it = fastaParser.iterator();
        FastaParser.FastaRecord record = it.hasNext() ? it.next() : null;
        assert(record != null);

        String read_sequence = record.getSequence();
        
        final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[1])));

        PacbioCorrector.PBCorrectionResult result = new PacbioCorrector.PBCorrectionResult(read_sequence);

        PacbioCorrector.correct(read_sequence,new Iterable<PacbioCorrector.PBBlastAlignment>() {
            @Override
            public Iterator<PacbioCorrector.PBBlastAlignment> iterator() {
                List<PacbioCorrector.PBBlastAlignment> l = new ArrayList<PacbioCorrector.PBBlastAlignment>();
                String line;

                try {
                    while(null != (line = reader.readLine())){
                        String []arr = line.split("\t");
                        l.add(new PacbioCorrector.PBBlastAlignment(
                                Integer.parseInt(arr[8]),
                                Integer.parseInt(arr[9]),
                                arr[12]
                        ));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                return l.iterator();
            }
        },result);
        
        result.getPileup().printPileup(read_sequence, System.out);

    }
    
}
