import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InCombiningMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private Map<String, Integer> map = null;

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws java.io.IOException, java.lang.InterruptedException {
        map = new HashMap<String, Integer>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if (map.containsKey(token)) {
                int total = map.get(token) + 1;
                map.put(token, total);
            } else {
                map.put(token, 1);
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, Integer> map = getMap();
        Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = it.next();
            String sKey = entry.getKey();
            int total = entry.getValue().intValue();
            context.write(new Text(sKey), new IntWritable(total));
        }
    }

    public Map<String, Integer> getMap() {
        // if(null == map) {//lazy loading
        // map = new HashMap<String, Integer>();
        // }
        return map;
    }
}
