package com.javacodegeeks.examples.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ApacheLogInMapperCombining extends Mapper<LongWritable, Text, Text, Pair<Integer,Integer>> {
    
    
    private static final String a = "64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1 401 12846\r\n" +
        "64.242.88.10 - - [07/Mar/2004:16:06:51 -0800] GET /twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1.3&rev2=1.2 HTTP/1.1 200 4523\r\n"; 
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String paragraph = value.toString();
        StringTokenizer st = new StringTokenizer(paragraph, "\n");
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
            if(lastquantity != -1) {
                if(map.containsKey(ipadress)) {
                    Pair<Integer,Integer> pair = map.get(ipadress);
                    pair.setValue(pair.getValue()+1);
                    map.put(ipadress, pair);
                }else {
                    map.put(ipadress, new Pair<Integer, Integer>(lastquantity,1));
                }
            }
        }
    }
    
    private Map<String, Pair<Integer, Integer>> map = null;
    
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Pair<Integer,Integer>>.Context context) 
            throws java.io.IOException, java.lang.InterruptedException {
        map = new HashMap<String, Pair<Integer, Integer>>();
    }
    
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        Map<String, Pair<Integer, Integer>> map = getMap();
        Iterator<Map.Entry<String, Pair<Integer, Integer>>> it = map.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Pair<Integer, Integer>> entry = it.next();
            String sKey = entry.getKey();
            Pair<Integer, Integer> pair = entry.getValue();
            context.write(new Text(sKey), pair);
        }
    }
    
    public Map<String, Pair<Integer, Integer>> getMap() {
//        if(null == map) {//lazy loading
//            map = new HashMap<String, Integer>();
//        }
        return map;
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
