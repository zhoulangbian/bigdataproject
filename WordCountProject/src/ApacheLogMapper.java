import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class ApacheLogMapper extends InCombiningMapper {
    
    
    private static final String a = "64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1 401 12846\r\n" +
        "64.242.88.10 - - [07/Mar/2004:16:06:51 -0800] GET /twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1.3&rev2=1.2 HTTP/1.1 200 4523\r\n"; 
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String paragraph = value.toString();
        StringTokenizer st = new StringTokenizer(paragraph, "\r\n");
        while (st.hasMoreTokens()) {
            String line = st.nextToken();
            StringTokenizer words = new StringTokenizer(line, " ");
            int count = words.countTokens();
            int i = 0;
            String ipadress = null;
            int lastquantity = -1;
            while(words.hasMoreTokens()) {
                String token = words.nextToken();
                if(i == 0)
                    ipadress = token;
                else if(i == count-1) {
                    try {
                        lastquantity = Integer.valueOf(token);
                    } catch(Exception ex) {
                        break;
                    }
                }
                i++;
            }
            if(lastquantity != -1)
               this.getMap().put(ipadress, lastquantity); 
        }
    }
    
    public static void testAnalysis(String a) {
        String[] lines = a.split("\r\n");
        for(String line : lines) {
            String[] words = line.split("\\s+");
            System.out.println(words[0] + " " + words[words.length - 1]);
        }
    }
    
    public static void main(String args[]) {
        //Scanner in = new Scanner(System.in);
        //int n = in.nextInt();
        //System.out.println(calc_fib(n));
        //System.out.println(calcfib1(n));
        testAnalysis(a);
      }
}
