package edu.cshl.schatz.jnomics.test;

import edu.cshl.schatz.jnomics.io.FastaParser;
import edu.cshl.schatz.jnomics.tools.PacbioCorrector;
import junit.framework.TestCase;
import org.junit.Assert;

import java.io.*;
import java.util.*;

/**
 * User: james
 */
public class PacbioCorrectorTest extends TestCase {

    public void testAlignmentParse() throws Exception {
        InputStream reads_in = new ByteArrayInputStream(read.getBytes());
        FastaParser parser = new FastaParser(reads_in);
        Iterator<FastaParser.FastaRecord> it = parser.iterator();
        FastaParser.FastaRecord read = it.hasNext() ? it.next() : null; 
        Assert.assertNotEquals("Collecting read from parser", read, null);

        String read_sequence = read.getSequence();

        PacbioCorrector.PBCorrectionResult correctionResult = new PacbioCorrector.PBCorrectionResult(read_sequence);

        PacbioCorrector.correct(read_sequence,
                new Iterable<PacbioCorrector.PBBlastAlignment>() {
                    @Override
                    public Iterator<PacbioCorrector.PBBlastAlignment> iterator() {
                        List<PacbioCorrector.PBBlastAlignment> l = new ArrayList<PacbioCorrector.PBBlastAlignment>();
                        BufferedReader b = new BufferedReader(new InputStreamReader(
                                new ByteArrayInputStream(alignments.getBytes()))
                        );
                        String line;
                        try {
                            while(null != (line = b.readLine())){
                                String[] arr = line.split("\\s+");
                                l.add(new PacbioCorrector.PBBlastAlignment(Integer.parseInt(arr[8]),
                                        Integer.parseInt(arr[9]),
                                        arr[12]
                                ));
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return l.iterator();
                    }
                },
                correctionResult);
        correctionResult.getPileup().printPileup(read_sequence, System.out);
    }

    String read = ">gnl|PBCORRECT|25041 m111210_003511_42137_c100242842555500000315045003121280_s1_p0/40832/0_1121\n" +
            "GGCTTACGGTTTTCATGTACTTTTTAGCTTTACACAATTAAATAGTTGCAGCTAACCTTCATTTTTTGGTCTATGACCCT\n" +
            "CATCTTACCGTTTAGCCAAGAATGTCCAGCAATAGTCGACTAGCTCCTCCTTTGCTTTACCGACTTTTTATAATCATGAT\n" +
            "ACAAGACCATCCGTCCGTTAAGGTTCTCTTAGCCTTTTGCTCCCTCAGGGCCAGCCATCGTTCCTCTTTTTCTGCCCTCT\n" +
            "TACCTTTGCTTCCCACTGCCCACGAAAATACCACTCCATCTACTACTGTCTCATTAGTTGGTTGCCCAACGCGACAATTT\n" +
            "GGTAACCCAAGTGGAAAATGATTTCATGATTCCACGTTGGTTCCACGTGGCCATGTGGAATCTAAATGTTTCTTGTATTT\n" +
            "TTGCAAATTATTAACTTCACAGGATGACGATAATGGTACGGCTGCAAAGATTTTTTACGCAATTATTTTTTTATTTTTAC\n" +
            "TGTCCTAATTTGATGCTGGATTTTACAAGGAATTCATGAAAATGAGCCGTATACTTGATTCAACATGGAATCCACTCCCG\n" +
            "GCGGGTCGATAAAAAAACTGTGATACACCACGCCAATTCAGCACATTTTTAGGAAATTACAAGCCCCCATAGTATTCATT\n" +
            "ATTACATTATATGTTTTTAAGGTTAAAATTAACTATCTTAAAGCTAAAATTACAACTCGCAAATAACGACACTGTCCTCT\n" +
            "TGTGTCAAGAATAGACAAAGTAGAAGTGCAAATAAAAGACGCTTCGTCGCTCTGTTTTTAAACATAAAATTAATTTTCCT\n" +
            "GCCCTTATATTGTGCACTGTTGTCTTGGTTCTCAGAGACGCCTTGAAAGATAAATGCGCCTCTTGTTTATGCTATTTAAG\n" +
            "CACCTGTAACAATTTTAGTAGATCTAGCCTATAGCAATAACTTTATCAGGCCATACCTCCATGAGATTAGAAAATTAGAT\n" +
            "AATCAGTATTTGTTATCATAGATAGGCCGGTAGCAGCTACGCTTCTTGGTAAAATCTATGATAAATTTATTTAGGCCCTA\n" +
            "GGCATCAAATTTCTAGAAATCCCATGGCCATGTCCATAGTTGGCTACCTCATTTTTTTAACTAAATAGACTTTACAGAAG\n" +
            "T";
    String alignments = "HWI-ST985:95:C0KV6ACXX:1:1107:17168:75383       gnl|PBCORRECT|25041     92.24   116     3       6       36      148     1114    1002    3e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:21120:75258       gnl|PBCORRECT|25041     93.10   116     2       6       51      163     1114    1002    8e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:11105:66717       gnl|PBCORRECT|25041     88.97   145     2       10      27      160     1114    973     2e-39    167    4G-8A-14-G8T-32GT2GT5-G32-G3-C-G2-G-C1-G-C2-C-C16       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:6672:91315        gnl|PBCORRECT|25041     87.57   169     4       13      3       157     1110    945     2e-43    180    8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C2AC13-C2-A8T-9-T6       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1201:21178:55508       gnl|PBCORRECT|25041     92.24   116     3       6       69      181     1114    1002    4e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1101:20424:10584       gnl|PBCORRECT|25041     94.29   70      0       4       123     191     1114    1048    2e-20    104    4G-8A-14-G8T-32 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1101:16650:39503       gnl|PBCORRECT|25041     88.89   189     2       15      1       174     909     1093    7e-54    215    5G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32A-8-C5       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1101:10729:188549      gnl|PBCORRECT|25041     92.71   96      2       5       18      111     1114    1022    1e-29    134    4G-8A-14-G8T-32GT2GT7-G14       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1102:12298:71430       gnl|PBCORRECT|25041     93.10   116     2       6       39      151     1114    1002    7e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1102:18337:133911      gnl|PBCORRECT|25041     92.24   116     3       6       38      150     1114    1002    3e-37    159    4G-8A-14-G8T-32CT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1103:7300:5490 gnl|PBCORRECT|25041     88.62   167     2       13      23      176     1114    952     2e-45    187    4G-8A-14-G8T-32GT2GT5-G32-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1103:14909:30027       gnl|PBCORRECT|25041     88.34   163     3       12      1       149     917     1077    6e-44    182    5-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA30    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1103:5533:60797        gnl|PBCORRECT|25041     88.02   167     3       13      1       154     952     1114    7e-44    182    9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8C-4  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1103:6748:67491        gnl|PBCORRECT|25041     91.11   90      4       4       1       87      1101    1013    3e-25    119    3-A2GA6-G8T-32CT2GT7-G18GA4     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1103:3703:181484       gnl|PBCORRECT|25041     88.83   179     2       14      13      177     1114    940     2e-50    204    4G-8A-14-G8T-32GT2GT5-G32-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T11  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1103:8687:186687       gnl|PBCORRECT|25041     92.86   98      2       5       77      172     1114    1020    2e-30    137    4G-8A-14-G8T-32GT2GT7-G16       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:13604:43103       gnl|PBCORRECT|25041     87.43   167     4       13      3       156     952     1114    4e-42    176    9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C11GA2T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:2143:92955        gnl|PBCORRECT|25041     88.78   205     4       15      1       190     865     1065    2e-59    233    6G-15-G3AC23G-8-T13G-15-A1GA7A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C30-C7CA2CA18       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:16340:95755       gnl|PBCORRECT|25041     90.24   123     5       7       10      128     1002    1121    2e-35    154    3-C11CT18-C7CA2CA32A-8-C14TA1T-6C-4GC1-G4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:11272:138369      gnl|PBCORRECT|25041     87.66   154     3       12      1       141     964     1114    7e-39    165    5-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8C-4     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:8099:143914       gnl|PBCORRECT|25041     92.47   93      2       5       94      184     1114    1025    1e-27    128    4G-8A-14-G8T-32CT2GT7-G11       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:2478:165917       gnl|PBCORRECT|25041     93.00   100     2       5       15      112     1114    1018    7e-32    141    4G-8A-14-G8T-32GT2GT7-G18       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:6118:166038       gnl|PBCORRECT|25041     88.37   172     3       13      1       158     927     1095    1e-46    191    9G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C7  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:12024:171548      gnl|PBCORRECT|25041     92.39   92      2       5       82      171     1114    1026    3e-27    126    4G-8A-14-G8T-32GT2GT7-G10       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:5813:185196       gnl|PBCORRECT|25041     91.92   99      2       6       1       97      1020    1114    2e-29    134    16-C7CA2CA32A-8-C10T-4T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:14543:188744      gnl|PBCORRECT|25041     92.55   94      2       5       1       92      1024    1114    3e-28    130    12-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:14598:198938      gnl|PBCORRECT|25041     92.50   80      2       4       99      177     1114    1038    1e-22    111    4G-8A-14-G8T-32GT2GT6   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1105:2342:8423 gnl|PBCORRECT|25041     92.05   88      2       5       6       90      410     325     6e-25    119    13T-2-C36CA10C-5-A7TC4-T4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1105:1424:17354        gnl|PBCORRECT|25041     93.10   116     2       6       29      141     1114    1002    6e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1105:17778:24926       gnl|PBCORRECT|25041     87.50   144     3       11      1       131     953     1094    4e-35    152    8A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2GA32A-8-C6 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1105:4018:34520        gnl|PBCORRECT|25041     88.52   183     3       14      1       169     909     1087    4e-51    206    5G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1105:20738:55312       gnl|PBCORRECT|25041     92.93   99      3       4       5       100     1002    1099    7e-32    141    3-C11CT18-C7CA2CA32A-8-C11      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1105:1937:106350       gnl|PBCORRECT|25041     91.86   86      2       5       37      120     1114    1032    5e-24    115    4G-8A-14-G8T-32GT2GT7-G4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1106:14197:4708        gnl|PBCORRECT|25041     94.29   70      0       4       3       71      1048    1114    2e-20    104    32A-8-C14T-8C-4 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1106:10220:40515       gnl|PBCORRECT|25041     88.46   182     3       14      1       168     933     1110    2e-50    204    3G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1106:15087:74094       gnl|PBCORRECT|25041     89.74   78      3       5       113     188     1114    1040    1e-17   95.3    4G-8A-4-A2CA6-G8T-32CT2GT4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1106:3702:87008        gnl|PBCORRECT|25041     89.47   190     2       14      1       175     1101    915     1e-56    224    13-G8T-32GT2GT5-G32-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A7 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1106:4209:96476        gnl|PBCORRECT|25041     92.55   94      2       5       1       92      1024    1114    3e-28    130    12-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1106:14652:132612      gnl|PBCORRECT|25041     93.69   111     2       5       4       111     1110    1002    8e-38    161    8A-14-G8T-32GT2GT5-G32-G3       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1106:2529:151554       gnl|PBCORRECT|25041     92.50   80      2       4       110     188     1114    1038    1e-22    111    4G-8A-14-G8T-32GT2GT6   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:11505:18634       gnl|PBCORRECT|25041     92.24   116     3       6       8       120     1002    1114    4e-37    159    3-C11CT18-C7CA2CA32A-8-C14T-8C-4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:2424:50570        gnl|PBCORRECT|25041     88.89   198     3       15      1       183     904     1097    4e-57    226    10G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C9  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:15781:73377       gnl|PBCORRECT|25041     89.27   177     2       13      1       163     906     1079    4e-51    206    8G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:11230:78962       gnl|PBCORRECT|25041     88.95   190     3       14      1       176     925     1110    6e-55    219    11G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:10107:98323       gnl|PBCORRECT|25041     92.66   109     3       5       84      190     1114    1009    6e-35    152    4G-8A-14-G8T-32GT2GT7-G18GA8    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:16017:126508      gnl|PBCORRECT|25041     86.67   120     2       10      1       107     1066    948     1e-25    121    19GT2GT7-G30-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:7614:128216       gnl|PBCORRECT|25041     92.93   99      2       5       87      183     1114    1019    5e-31    139    4G-8A-14-G8T-32GT2GT7-G17       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:3425:171632       gnl|PBCORRECT|25041     92.21   77      2       4       1       76      1041    1114    5e-21    106    3CA2CA32A-8-C14T-8C-4   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1107:10009:179165      gnl|PBCORRECT|25041     93.00   100     2       5       1       98      1018    1114    1e-31    141    18-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1108:11858:3484        gnl|PBCORRECT|25041     87.95   166     3       13      21      173     1114    953     3e-43    180    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-8  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1108:19336:29014       gnl|PBCORRECT|25041     92.31   104     3       5       72      173     1114    1014    3e-32    143    4G-8A-14-G8T-32GT2GT7-G18GA3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1108:20897:33874       gnl|PBCORRECT|25041     92.24   116     3       6       2       114     1002    1114    3e-37    159    3-C11CT18-C7CA2CA32A-8-C14T-8C-4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1108:10420:82417       gnl|PBCORRECT|25041     92.93   99      2       5       1       97      1019    1114    3e-31    139    17-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1108:3668:85084        gnl|PBCORRECT|25041     92.93   99      2       5       39      135     1114    1019    3e-31    139    4G-8A-14-G8T-32GT2GT7-G17       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1108:6461:90335        gnl|PBCORRECT|25041     89.37   207     3       15      1       192     864     1066    4e-62    243    7G-15-G3AC23G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C30-C7CA2CA19  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1108:12873:143040      gnl|PBCORRECT|25041     87.90   157     3       12      42      185     1114    961     2e-40    171    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1108:14641:176816      gnl|PBCORRECT|25041     92.31   104     3       5       1       102     1014    1114    3e-32    143    3CT18-C7CA2CA32A-8-C14T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1101:4872:37761        gnl|PBCORRECT|25041     92.45   106     3       5       1       104     1012    1114    2e-33    147    5CT18-C7CA2CA32A-8-C14T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1101:6156:73454        gnl|PBCORRECT|25041     89.50   200     2       15      1       185     904     1099    6e-60    235    10G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32A-8-C11     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1101:14593:148435      gnl|PBCORRECT|25041     93.10   116     2       6       69      181     1114    1002    8e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1101:20303:178355      gnl|PBCORRECT|25041     88.46   182     3       14      1       168     1113    936     2e-50    204    3G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1101:4496:183979       gnl|PBCORRECT|25041     93.10   116     2       6       2       114     1002    1114    7e-39    165    3-C32-C5CA2CA32A-8-C14T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1101:17539:197777      gnl|PBCORRECT|25041     92.24   116     3       6       4       116     1002    1114    4e-37    159    3-C11CT18-C7CA2GA32A-8-C14T-8C-4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:4584:23540        gnl|PBCORRECT|25041     93.10   116     2       6       2       114     1114    1002    5e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:7477:31323        gnl|PBCORRECT|25041     93.10   116     2       6       46      158     1114    1002    7e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:9957:60808        gnl|PBCORRECT|25041     94.29   70      0       4       3       71      1048    1114    1e-20    104    32A-8-C14T-8C-4 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:16948:85998       gnl|PBCORRECT|25041     92.66   109     3       5       1       107     1009    1114    6e-35    152    8CT18-C7CA2CA32A-8-C14T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:13801:86172       gnl|PBCORRECT|25041     90.52   116     4       7       60      171     1114    1002    3e-33    147    4G-8A-4-A2CA6-G8T-32CT2GT7-G18GA11-G3   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:15541:88183       gnl|PBCORRECT|25041     89.34   197     2       15      1       183     923     1114    3e-58    230    13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32A-8-C14T-8C-4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:18499:106217      gnl|PBCORRECT|25041     89.36   188     2       14      1       174     925     1108    2e-55    220    11G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32A-8-C14T-6 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:3696:109837       gnl|PBCORRECT|25041     86.67   120     2       10      1       107     1066    948     1e-25    121    19GT2GT7-G30-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:5032:146695       gnl|PBCORRECT|25041     88.24   187     3       15      5       177     1114    933     4e-51    206    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-3   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:14458:170297      gnl|PBCORRECT|25041     92.47   93      2       5       1       91      1025    1114    7e-28    128    11-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1102:12675:178845      gnl|PBCORRECT|25041     88.71   186     3       14      1       171     915     1097    9e-53    211    7-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2GA32A-8-C9      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1103:15844:74674       gnl|PBCORRECT|25041     92.73   110     3       5       20      127     1114    1008    1e-35    154    4G-8A-14-G8T-32GT2GT7-G18GA9    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1103:12650:79699       gnl|PBCORRECT|25041     92.24   116     3       6       55      167     1114    1002    4e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1103:7973:133221       gnl|PBCORRECT|25041     94.29   70      0       4       104     172     1114    1048    2e-20    104    4G-8A-14-G8T-32 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1103:14576:138804      gnl|PBCORRECT|25041     93.90   82      2       3       1       80      1082    1002    9e-26    121    3T-32GT2GT5-G32-G3      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1103:13879:147592      gnl|PBCORRECT|25041     93.10   116     2       6       57      169     1114    1002    8e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1103:14454:168552      gnl|PBCORRECT|25041     88.41   207     5       15      1       192     880     1082    8e-59    231    6-G3AC12CT10G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-3   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1103:11474:175426      gnl|PBCORRECT|25041     92.22   90      2       5       1       88      1028    1114    5e-26    122    8-C7CA2CA32A-8-C14T-8C-4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1104:21097:42202       gnl|PBCORRECT|25041     89.13   184     2       14      1       170     1106    927     3e-53    213    4A-14-G8T-32GT2GT5-G32-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-9  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1104:9923:46113        gnl|PBCORRECT|25041     92.86   112     3       5       2       111     1006    1114    1e-36    158    11CT18-C7CA2CA32A-8-C14T-8C-4   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1104:14492:47241       gnl|PBCORRECT|25041     87.69   130     2       10      1       117     937     1065    3e-31    139    14-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C30-C7CA2CA18       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1104:14087:50675       gnl|PBCORRECT|25041     87.98   183     3       15      20      188     1114    937     8e-49    198    4G-8A-4A-10-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T14   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1104:21451:79056       gnl|PBCORRECT|25041     92.24   116     3       6       49      161     1114    1002    3e-37    159    4G-8A-14-G8T-32CT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1104:16175:90734       gnl|PBCORRECT|25041     92.24   116     2       6       4       115     1002    1114    1e-36    158    3-C31-C-C5CA2CA32A-8-C14T-8C-4  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1104:4853:150606       gnl|PBCORRECT|25041     92.24   116     3       6       68      180     1114    1002    4e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1104:12678:189093      gnl|PBCORRECT|25041     92.13   89      2       5       33      119     1114    1029    1e-25    121    4G-8A-14-G8T-32GT2GT7-G7        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1105:6901:3402 gnl|PBCORRECT|25041     92.24   116     3       6       42      154     1114    1002    3e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1105:3923:3839 gnl|PBCORRECT|25041     94.29   70      0       4       104     172     1114    1048    2e-20    104    4G-8A-14-G8T-32 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1105:12961:16839       gnl|PBCORRECT|25041     87.86   140     3       10      1       127     1077    939     4e-35    152    30GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T12   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1105:8475:95736        gnl|PBCORRECT|25041     91.36   81      2       5       84      163     1114    1038    4e-21    106    4G-8A-4A-10-G8T-32GT2GT6        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1105:6766:193915       gnl|PBCORRECT|25041     88.30   171     3       13      1       157     941     1108    4e-46    189    10-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-6 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1106:18477:128476      gnl|PBCORRECT|25041     88.89   189     2       15      18      192     1114    931     8e-54    215    4G-8A-14-G8T-32GT2GT5-G32-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-5       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:6003:73808        gnl|PBCORRECT|25041     87.37   190     5       15      1       175     1108    923     2e-49    200    6A-4-A2CA6-G8T-32CT2GT7-G18GA11-G3-C2-A-G-C1-G-C2-C-C11TG4-C2-A8T-9-T15C-13     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:8886:114397       gnl|PBCORRECT|25041     91.36   81      3       4       99      178     1114    1037    1e-21    108    4G-2TC5A-14-G8T-32GT2GT7        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:19454:114956      gnl|PBCORRECT|25041     92.59   81      2       4       3       82      1037    1114    2e-23    113    7CA2CA32A-8-C14T-8C-4   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:9372:151853       gnl|PBCORRECT|25041     88.50   200     4       15      1       185     1095    900     1e-56    224    7-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A8C-10GA3       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:11644:163432      gnl|PBCORRECT|25041     89.19   185     2       14      1       171     930     1110    7e-54    215    6G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32A-8-C14T-8  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:10270:193264      gnl|PBCORRECT|25041     93.64   110     2       5       1       108     1008    1114    4e-37    159    30-C5CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:11174:3897        gnl|PBCORRECT|25041     88.12   160     3       12      1       146     937     1094    3e-42    176    14-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C6     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:19316:42774       gnl|PBCORRECT|25041     94.29   70      0       4       6       74      1048    1114    2e-20    104    32A-8-C14T-8C-4 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:10357:54237       gnl|PBCORRECT|25041     88.73   204     3       16      1       189     909     1107    8e-59    231    5G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-5       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:17182:103996      gnl|PBCORRECT|25041     92.93   99      2       5       1       97      1019    1114    4e-31    139    17-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:4317:111499       gnl|PBCORRECT|25041     91.45   117     3       7       63      176     1114    1002    2e-35    154    4G-8A-4A-10-G8T-32GT2GT7-G18GA11-G3     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:5236:141817       gnl|PBCORRECT|25041     92.59   108     3       5       2       107     1114    1010    1e-34    150    4G-8A-14-G8T-32GT2GT7-G18GA7    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:17490:157689      gnl|PBCORRECT|25041     92.86   70      1       4       4       72      1048    1114    8e-19   99.0    32A-2CG5-C14T-8C-4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:20088:177630      gnl|PBCORRECT|25041     92.17   115     3       6       1       112     1002    1113    7e-37    158    3-C11CT18-C7CA2CA32A-8-C14T-8C-3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:6056:179065       gnl|PBCORRECT|25041     90.00   80      3       5       1       78      1038    1114    5e-19   99.0    6CA2GA32A-8-C6GT2-T4T-8C-4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:9027:180483       gnl|PBCORRECT|25041     93.00   100     2       5       66      163     1114    1018    1e-31    141    4G-8A-14-G8T-32GT2GT7-G18       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:5857:181832       gnl|PBCORRECT|25041     88.28   145     3       10      43      176     1114    973     1e-37    161    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:20098:199484      gnl|PBCORRECT|25041     92.24   116     3       6       49      161     1114    1002    4e-37    159    4G-8A-14-G8T-32CT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1101:5792:71774        gnl|PBCORRECT|25041     91.43   70      1       5       118     185     1114    1048    1e-16   91.6    4G-8A-4-A2CA6-G8T-32    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1102:4385:113086       gnl|PBCORRECT|25041     92.93   99      2       5       1       97      1019    1114    4e-31    139    17-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1102:12166:133469      gnl|PBCORRECT|25041     93.10   116     2       6       59      171     1114    1002    8e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1102:11735:157573      gnl|PBCORRECT|25041     87.36   182     3       15      3       170     1114    939     1e-46    191    4G-8A-14-G2GA5T-32GT2GT5-G32-G3-C-G2-G-C1-G-C2-C-C7A-T-9-C2-A8T-9-T12   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1103:5379:27257        gnl|PBCORRECT|25041     92.31   78      2       4       37      113     1114    1040    8e-22    108    4G-8A-14-G8T-32GT2GT4   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1103:3625:68226        gnl|PBCORRECT|25041     88.83   188     3       14      1       173     1101    917     7e-54    215    13-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A5     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1103:17691:74602       gnl|PBCORRECT|25041     87.40   127     3       9       1       115     1079    954     1e-29    134    32CT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-7       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1103:9176:79026        gnl|PBCORRECT|25041     87.79   172     2       14      2       159     1037    871     2e-44    183    32-G3-C-G2-G-C1-G-C2-C-C7A-T-9-C2-A8T-9-T15C-13-A8C-10GA12TG3-C15       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:3704:71792        gnl|PBCORRECT|25041     91.46   82      2       5       1       81      1037    1114    1e-21    108    7CA2CA32A-8-C10T-4T-8C-4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:19897:82730       gnl|PBCORRECT|25041     92.73   110     3       5       1       108     1008    1114    2e-35    154    9CT18-C7CA2CA32A-8-C14T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:8401:91557        gnl|PBCORRECT|25041     92.24   116     3       6       24      136     1114    1002    3e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:14770:133249      gnl|PBCORRECT|25041     88.42   190     3       15      2       177     1114    930     1e-52    211    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-6   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:8038:152222       gnl|PBCORRECT|25041     87.90   157     3       12      41      184     1114    961     2e-40    171    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:14104:157355      gnl|PBCORRECT|25041     86.71   173     4       15      20      177     1114    946     1e-41    174    4G-8A-4-A2CA6-G8T-32CT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T5  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1105:3115:9509 gnl|PBCORRECT|25041     87.82   156     3       12      1       143     962     1114    6e-40    169    7-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8C-4     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1105:16935:50115       gnl|PBCORRECT|25041     93.10   116     2       6       1       113     1002    1114    7e-39    165    3-C32-C5CA2CA32A-8-C14T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1105:6244:171356       gnl|PBCORRECT|25041     87.88   165     3       13      1       152     954     1114    9e-43    178    7A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8C-4  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1105:18621:172577      gnl|PBCORRECT|25041     92.86   98      2       5       88      183     1114    1020    2e-30    137    4G-8A-14-G8T-32GT2GT7-G16       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:6762:21407        gnl|PBCORRECT|25041     92.41   79      2       4       1       78      1039    1114    4e-22    110    5CA2CA32A-8-C14T-8C-4   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:14764:36535       gnl|PBCORRECT|25041     88.48   165     2       13      1       152     954     1114    2e-44    183    7A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32A-8-C14T-8C-4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:14347:48060       gnl|PBCORRECT|25041     92.47   93      2       5       1       91      1025    1114    8e-28    128    11-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:8886:114397       gnl|PBCORRECT|25041     91.36   81      3       4       99      178     1114    1037    1e-21    108    4G-2TC5A-14-G8T-32GT2GT7        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:19454:114956      gnl|PBCORRECT|25041     92.59   81      2       4       3       82      1037    1114    2e-23    113    7CA2CA32A-8-C14T-8C-4   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:9372:151853       gnl|PBCORRECT|25041     88.50   200     4       15      1       185     1095    900     1e-56    224    7-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A8C-10GA3       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:11644:163432      gnl|PBCORRECT|25041     89.19   185     2       14      1       171     930     1110    7e-54    215    6G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32A-8-C14T-8  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1107:10270:193264      gnl|PBCORRECT|25041     93.64   110     2       5       1       108     1008    1114    4e-37    159    30-C5CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:11174:3897        gnl|PBCORRECT|25041     88.12   160     3       12      1       146     937     1094    3e-42    176    14-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C6     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:19316:42774       gnl|PBCORRECT|25041     94.29   70      0       4       6       74      1048    1114    2e-20    104    32A-8-C14T-8C-4 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:10357:54237       gnl|PBCORRECT|25041     88.73   204     3       16      1       189     909     1107    8e-59    231    5G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-5       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:17182:103996      gnl|PBCORRECT|25041     92.93   99      2       5       1       97      1019    1114    4e-31    139    17-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:4317:111499       gnl|PBCORRECT|25041     91.45   117     3       7       63      176     1114    1002    2e-35    154    4G-8A-4A-10-G8T-32GT2GT7-G18GA11-G3     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:5236:141817       gnl|PBCORRECT|25041     92.59   108     3       5       2       107     1114    1010    1e-34    150    4G-8A-14-G8T-32GT2GT7-G18GA7    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:17490:157689      gnl|PBCORRECT|25041     92.86   70      1       4       4       72      1048    1114    8e-19   99.0    32A-2CG5-C14T-8C-4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:20088:177630      gnl|PBCORRECT|25041     92.17   115     3       6       1       112     1002    1113    7e-37    158    3-C11CT18-C7CA2CA32A-8-C14T-8C-3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:6056:179065       gnl|PBCORRECT|25041     90.00   80      3       5       1       78      1038    1114    5e-19   99.0    6CA2GA32A-8-C6GT2-T4T-8C-4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:9027:180483       gnl|PBCORRECT|25041     93.00   100     2       5       66      163     1114    1018    1e-31    141    4G-8A-14-G8T-32GT2GT7-G18       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:5857:181832       gnl|PBCORRECT|25041     88.28   145     3       10      43      176     1114    973     1e-37    161    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:20098:199484      gnl|PBCORRECT|25041     92.24   116     3       6       49      161     1114    1002    4e-37    159    4G-8A-14-G8T-32CT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1101:5792:71774        gnl|PBCORRECT|25041     91.43   70      1       5       118     185     1114    1048    1e-16   91.6    4G-8A-4-A2CA6-G8T-32    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1102:4385:113086       gnl|PBCORRECT|25041     92.93   99      2       5       1       97      1019    1114    4e-31    139    17-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1102:12166:133469      gnl|PBCORRECT|25041     93.10   116     2       6       59      171     1114    1002    8e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1102:11735:157573      gnl|PBCORRECT|25041     87.36   182     3       15      3       170     1114    939     1e-46    191    4G-8A-14-G2GA5T-32GT2GT5-G32-G3-C-G2-G-C1-G-C2-C-C7A-T-9-C2-A8T-9-T12   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1103:5379:27257        gnl|PBCORRECT|25041     92.31   78      2       4       37      113     1114    1040    8e-22    108    4G-8A-14-G8T-32GT2GT4   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1103:3625:68226        gnl|PBCORRECT|25041     88.83   188     3       14      1       173     1101    917     7e-54    215    13-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A5     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1103:17691:74602       gnl|PBCORRECT|25041     87.40   127     3       9       1       115     1079    954     1e-29    134    32CT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-7       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1103:9176:79026        gnl|PBCORRECT|25041     87.79   172     2       14      2       159     1037    871     2e-44    183    32-G3-C-G2-G-C1-G-C2-C-C7A-T-9-C2-A8T-9-T15C-13-A8C-10GA12TG3-C15       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:3704:71792        gnl|PBCORRECT|25041     91.46   82      2       5       1       81      1037    1114    1e-21    108    7CA2CA32A-8-C10T-4T-8C-4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:19897:82730       gnl|PBCORRECT|25041     92.73   110     3       5       1       108     1008    1114    2e-35    154    9CT18-C7CA2CA32A-8-C14T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:8401:91557        gnl|PBCORRECT|25041     92.24   116     3       6       24      136     1114    1002    3e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:14770:133249      gnl|PBCORRECT|25041     88.42   190     3       15      2       177     1114    930     1e-52    211    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-6   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:8038:152222       gnl|PBCORRECT|25041     87.90   157     3       12      41      184     1114    961     2e-40    171    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1104:14104:157355      gnl|PBCORRECT|25041     86.71   173     4       15      20      177     1114    946     1e-41    174    4G-8A-4-A2CA6-G8T-32CT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T5  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1105:3115:9509 gnl|PBCORRECT|25041     87.82   156     3       12      1       143     962     1114    6e-40    169    7-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8C-4     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1105:16935:50115       gnl|PBCORRECT|25041     93.10   116     2       6       1       113     1002    1114    7e-39    165    3-C32-C5CA2CA32A-8-C14T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1105:6244:171356       gnl|PBCORRECT|25041     87.88   165     3       13      1       152     954     1114    9e-43    178    7A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8C-4  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1105:18621:172577      gnl|PBCORRECT|25041     92.86   98      2       5       88      183     1114    1020    2e-30    137    4G-8A-14-G8T-32GT2GT7-G16       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:6762:21407        gnl|PBCORRECT|25041     92.41   79      2       4       1       78      1039    1114    4e-22    110    5CA2CA32A-8-C14T-8C-4   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:14764:36535       gnl|PBCORRECT|25041     88.48   165     2       13      1       152     954     1114    2e-44    183    7A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32A-8-C14T-8C-4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:14347:48060       gnl|PBCORRECT|25041     92.47   93      2       5       1       91      1025    1114    8e-28    128    11-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:3776:66017        gnl|PBCORRECT|25041     92.24   116     3       6       38      150     1114    1002    3e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:10118:77246       gnl|PBCORRECT|25041     92.93   99      2       5       1       97      1019    1114    3e-31    139    17-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:4628:95337        gnl|PBCORRECT|25041     89.13   184     1       15      7       175     862     1041    1e-52    211    9G-15-G3AC23G-8-T13G-15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C3 (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:7784:138779       gnl|PBCORRECT|25041     88.83   179     2       14      1       165     940     1114    2e-50    204    11-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C32-C5CA2CA32A-8-C14T-8C-4  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:15330:143710      gnl|PBCORRECT|25041     87.93   174     3       14      1       160     945     1114    5e-46    189    6-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8C-4       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1106:18693:176732      gnl|PBCORRECT|25041     92.24   116     3       6       7       119     1002    1114    3e-37    159    3-C11CT18-C7CA2CA32A-8-C14T-8C-4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1107:11720:32625       gnl|PBCORRECT|25041     93.00   100     2       5       46      143     1114    1018    1e-31    141    4G-8A-14-G8T-32CT2GT7-G18       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1107:14542:51628       gnl|PBCORRECT|25041     92.41   79      2       4       1       78      1039    1114    3e-22    110    5CA2CA32A-8-C14T-8C-4   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1107:16657:132465      gnl|PBCORRECT|25041     92.24   116     3       6       63      175     1114    1002    3e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1107:16664:132483      gnl|PBCORRECT|25041     92.24   116     3       6       62      174     1114    1002    3e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1107:18409:133019      gnl|PBCORRECT|25041     88.61   202     4       15      1       187     1097    900     1e-57    228    9-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A8C-10GA3       (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1107:2781:134334       gnl|PBCORRECT|25041     88.83   197     3       15      1       182     1096    904     1e-56    224    8-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A8C-10  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1107:7691:193734       gnl|PBCORRECT|25041     91.86   86      2       5       39      122     1114    1032    5e-24    115    4G-8A-14-G8T-32GT2GT7-G4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1107:6708:198427       gnl|PBCORRECT|25041     88.24   170     3       13      1       156     1110    944     1e-45    187    8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T7  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1108:15093:34103       gnl|PBCORRECT|25041     92.13   89      2       5       1       87      1029    1114    9e-26    121    7-C7CA2CA32A-8-C14T-8C-4        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1108:4088:70334        gnl|PBCORRECT|25041     92.13   89      2       5       99      185     1114    1029    2e-25    121    4G-8A-14-G8T-32CT2GT7-G7        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1108:17921:120435      gnl|PBCORRECT|25041     92.24   116     3       6       72      184     1114    1002    4e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1108:12408:163303      gnl|PBCORRECT|25041     88.34   163     3       12      3       151     936     1096    6e-44    182    15-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C8     (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1108:13470:165280      gnl|PBCORRECT|25041     88.59   184     3       14      1       170     1087    908     1e-51    207    8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A8C-6      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1108:15237:196949      gnl|PBCORRECT|25041     92.65   68      0       5       1       67      1051    1114    3e-17   93.5    29A-8-C10T-4T-8C-4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1106:5272:67077        gnl|PBCORRECT|25041     92.24   116     3       6       60      172     1114    1002    4e-37    159    4G-8A-14-G8T-32GT2GT7-G18GA11-G3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1103:20531:171409      gnl|PBCORRECT|25041     91.58   95      2       6       66      158     1114    1024    3e-27    126    4G-8A-4A-10-G8T-32GT2GT7-G12    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1106:20083:192801      gnl|PBCORRECT|25041     88.12   202     6       14      1       188     1084    887     1e-56    224    5T-3CT28CT2GT5-G20GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A8C-10GA12TG3   (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:1:1104:10024:12051       gnl|PBCORRECT|25041     88.20   178     3       14      1       164     941     1114    3e-48    196    10-A9A-8-T2-G16-G-G2-G-C1-G-C2-C-G3-C11CT18-C7CA2CA32A-8-C14T-8C-4      (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1103:5722:80536        gnl|PBCORRECT|25041     87.68   203     3       18      2       186     1043    845     2e-54    217    5-G32-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T15C-13-A8C-10GA12TG3-C14T-10-A1-G1AG-C11        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:3:1102:17543:163749      gnl|PBCORRECT|25041     87.65   162     3       13      36      184     1114    957     5e-41    172    4G-8A-14-G8T-32GT2GT7-G18GA11-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-4  (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1106:3019:142786       gnl|PBCORRECT|25041     86.55   119     2       10      1       106     1065    948     4e-25    119    18GT2GT7-G30-G3-C-G2-G-C1-G-C2-C-C16-C2-A8T-9-T3        (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1101:11751:96338       gnl|PBCORRECT|25041     93.10   116     2       6       4       116     1002    1114    8e-39    165    3-C32-C5CA2CA32A-8-C14T-8C-4    (null)\n" +
            "HWI-ST985:95:C0KV6ACXX:2:1108:10181:141109      gnl|PBCORRECT|25041     93.10   116     2       6       67      179     1114    1002    8e-39    165    4G-8A-14-G8T-32GT2GT5-G32-G3    (null)\n";




}
