import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Given in input the price for each food product 
 * it produce for each month revenue for each product.
 * @author francescotangari
 *
 */
public class TotalRevenuePerProduct {

	
  
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text monthKey = new Text();
    private static final HashMap<String, Integer> priceFood;
    static
    {
        priceFood = new HashMap<String, Integer>();
        priceFood.put("pesce", (int) 10.0);
        priceFood.put("latte", (int) 2.0);
        priceFood.put("uova", (int) 2.0);
        priceFood.put("formaggio", (int) 2.0);
        priceFood.put("dolce", (int) 2.0);
        priceFood.put("vino", (int) 2.0);
        priceFood.put("pane", (int) 2.0);
        priceFood.put("insalata", (int) 2.0);

    }
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String line = value.toString();
    	String[] split = line.split(",");
    	
    	String[] getMonth = split[0].split("-");
		String month = getMonth[0] + "-" + getMonth[1];
		

        for(int i=1;i<split.length;i++){
    	    int price = priceFood.get(split[i]);
    	    one.set(price);
    		monthKey.set(month + "-" +split[i]);
    	    context.write(monthKey, one);
	    }
      
    }
  }
  
  public static class SumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	
	  
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "count");
    job.setJarByClass(MarketBasketMostSoldPerMonth.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
