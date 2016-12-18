import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Charsets;

/**
 * Calculate the support and confidence given in input the file, this is the
 * first analysis for the Apriori algorithm. Please set correctly the
 * NumberOfLinesInFile variable, for a rule of this kind p1 -> p2 it calculate
 * X->Y   =>   frequency ( x and y ) / NumberOfLinesInFile   ;   frequency ( x and y ) / frequency ( x) 
 * the support and the confidence of the rule
 * 
 * @author francescotangari
 *
 */
public class MarketBasketSupportConfidence {
	public final static float NumberOfLinesInFile = 18;

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {


		private static Text one = new Text("1");
		private Text KeyPair = new Text();
		private Text FreqItem = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] split = line.split(",");

			// String[] getMonth = split[0].split("-");
			// String month = getMonth[0] + "-" + getMonth[1];

			for (int i = 1; i < split.length; i++) {
				for (int j = i + 1; j < split.length; j++) {
					KeyPair.set("rule:" +split[i] + "," + split[j]);
					context.write(KeyPair, one);
				}
				FreqItem.set("fq:" + split[i]);
				context.write(FreqItem , one);
			}
			

		}
	}

	private static class Combine extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (Text value : values) {
				count += Integer.parseInt(value.toString());
			}
			context.write(key, new Text(String.valueOf(count)));
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		private Text resultfq = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String keyStr = key.toString();
			if(keyStr.contains("fq:")){
				int sum = 0;
				for (Text val : values) {
					sum = sum + Integer.parseInt(val.toString());
				}
				resultfq = new Text(String.valueOf(sum));
				context.write(key, resultfq);
			}
			else {
				float sum = 0;
				float sum2 = 0;
				float freqRule = 0;
				for (Text val : values) {
					sum = sum + Integer.parseInt(val.toString());
					sum2 = sum2 + Integer.parseInt(val.toString());
				}
				sum = (float) ((float)sum / NumberOfLinesInFile)*100;
				freqRule = (float) ((float)sum2);
				result = new Text(String.valueOf(sum) + "-" +freqRule);
				context.write(key, result);
			}
		}
	}
	
	  private static void readAndCalcConf(Path path, Configuration conf)
		      throws IOException {
		    FileSystem fs = FileSystem.get(conf);
		    Path file = new Path(path, "part-r-00000");
		    HashMap<String,Float> h = new HashMap<String,Float>();
		    if (!fs.exists(file))
		      throw new IOException("Output not found!");

		    BufferedReader br = null;

		    // average = total sum / number of elements;
		    try {
		      br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));

		      float frequencyItem = 0;
		      float supportRule = 0;
		      long confidence = 0;
		      String line;
		      while ((line = br.readLine()) != null) {
		        StringTokenizer st = new StringTokenizer(line);

		        // grab type
		        String type = st.nextToken();

		        // differentiate
		        if (type.equals("rule:".toString())) {
		          String rule = st.nextToken();
		          supportRule = Long.parseLong(rule);
		          float freq = (float) h.get(rule.split(",")[0].replace("rule:","")); 
		          confidence = (long) (((supportRule/100) * NumberOfLinesInFile) / freq);
			      System.out.println("Confidence: " + confidence);
		        } else {
		        	String item = st.nextToken();
		        	frequencyItem = Long.parseLong(item);
		        	h.put(item.split("fq:")[1], (float) frequencyItem);
		        }
		      }

		    } finally {
		      if (br != null) {
		        br.close();
		      }
		    }
		  }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage:  <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "count");
		job.setJarByClass(MarketBasketMostSoldPerMonth.class);
		job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(Combine.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
	    Path outputpath = new Path(args[1]);
		readAndCalcConf(outputpath,conf);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
