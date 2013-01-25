package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.util.BlastUtil;
import edu.cshl.schatz.jnomics.util.Nucleotide;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * Parsers Btop and gives the value at a position
 */
public class ValueAtBtop {
    
    public static void main(String []args) throws IOException {

        if(args.length != 4){
            System.out.println("ValueAtBtop sstart_col send_col btop_col position");
            System.exit(-1);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        int sstart_col = Integer.parseInt(args[0]);
        int send_col = Integer.parseInt(args[1]);
        int btop_col = Integer.parseInt(args[2]);
        int qpos = Integer.parseInt(args[3]);

        while(null != (line = reader.readLine())){
            String []arr = line.split("\t");
            PacbioCorrector.PBBlastAlignment alignment = new PacbioCorrector.PBBlastAlignment(Integer.parseInt(arr[sstart_col]),
                    Integer.parseInt(arr[send_col]),
                    arr[btop_col]);

            String []bt = BlastUtil.splitBlastTraceback(alignment.getBlastTraceback());
            int pos = alignment.isReverse() ? alignment.getAlignmentStart()+1 : alignment.getAlignmentStart()-1;
            //System.out.print(arr[0] + " ");
            //System.out.println(Arrays.toString(bt));
            //System.out.print(pos + ",,");
            if(pos <= qpos && alignment.isReverse()){
                continue;
            }else if(pos >= qpos && !alignment.isReverse()){
                continue;
            }

            task_loop:for(String task : bt){
                if(Character.isDigit(task.charAt(0))){
                    pos += alignment.isReverse() ? Integer.parseInt(task)*-1 :Integer.parseInt(task);
                    if(pos >= qpos && !alignment.isReverse()){
                        System.out.println(arr[0] + " M");
                        break task_loop;
                    }else if(pos <= qpos && alignment.isReverse()){
                        System.out.println(arr[0] + " M");
                        break task_loop;
                    }
                }else{//mismatch
                    mismatch_loop: for(int i =0 ; i < task.length();i+=2){
              //          System.out.println("pos:" + pos);
                        if(task.charAt(i+1) == '-')
                            continue mismatch_loop;//continue on insertions
                        pos += alignment.isReverse() ? -1 : 1;
                        if(pos == qpos){
                            char nuc = alignment.isReverse() ? Nucleotide.complement(task.charAt(i)) : task.charAt(i);
                            char nuc2 = alignment.isReverse() ? Nucleotide.complement(task.charAt(i+1)) : task.charAt(i+1);
                            System.out.println(arr[0]+" q:"+nuc+" s:"+nuc2);
                            break task_loop;
                        }
                    }
                }
                //System.out.print(pos + ",");
            }
        }
    }
}
