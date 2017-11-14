import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ApacheLogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
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
            	context.write(new Text(ipadress), new IntWritable(lastquantity));
        }
    }
}
