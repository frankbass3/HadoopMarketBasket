import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.shell.Count;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SparkAnalysis {
	public static class MarketBasket implements Serializable {
		private String support;
		private String confidence;
		private String rule;
		public String getRule() {
			return rule;
		}
		public void setRule(String rule) {
			this.rule = rule;
		}
		public String getConfidence() {
			return confidence;
		}
		public void setConfidence(String confidence) {
			this.confidence = confidence;
		}
		public String getSupport() {
			return support;
		}
		public void setSupport(String support) {
			this.support = support;
		}
	}
	public static class ItemPair implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String FristItem;
		private String SecondItem;
		private String frequencyOfPair;
		public String getFristItem() {
			return FristItem;
		}

		public void setFristItem(String fristItem) {
			FristItem = fristItem;
		}

		public String getSecondItem() {
			return SecondItem;
		}

		public void setSecondItem(String secondItem) {
			SecondItem = secondItem;
		}

		public String getFrequencyOfPair() {
			return frequencyOfPair;
		}

		public void setFrequencyOfPair(String frequencyOfPair) {
			this.frequencyOfPair = frequencyOfPair;
		}

		 
	}
	
	public static class ItemPair2 implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String FristItem2;
		private String SecondItem2;
		public String getFristItem2() {
			return FristItem2;
		}
		public void setFristItem2(String fristItem2) {
			FristItem2 = fristItem2;
		}
		public String getSecondItem2() {
			return SecondItem2;
		}
		public void setSecondItem2(String secondItem2) {
			SecondItem2 = secondItem2;
		}
 
		 

	}

	public static class Item implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String name;
		private String month;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getMonth() {
			return month;
		}

		public void setMonth(String month) {
			this.month = month;
		}
	}

	static long numberOfElements = 0;

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("MarketBasket").config("spark.master", "local")
				.getOrCreate();
		/**
		 * Showing a normal map reduce with spark and map reduce mapped with an
		 * own class
		 */
	    runFirstJobInSpark(spark);
	    runFirstJobInSparkUsingSQLSchemaObj(spark);
		/**
		 * Calculating support and confidence
		 */
		runJobCacluateSupport(spark);
	}

	@SuppressWarnings("serial")
	private static void runJobCacluateSupport(SparkSession spark) {
		// TODO Auto-generated method stub
		JavaRDD<String> frequencyOfItem = spark.read().textFile("sample.txt").javaRDD()
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String s) {
						ArrayList<String> li = new ArrayList<String>();
						String[] split = s.split(",");

						for (int i = 1; i < split.length; i++) {
							li.add(split[i]);
						}
						return li.iterator();
					}
				});

		numberOfElements = frequencyOfItem.count();

		JavaPairRDD<String, Integer> ones = frequencyOfItem.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		/// In this way i have calculated the frequency of ItemX
		/// Now lets calculate the frequency of pair (x and y)
		JavaRDD<String> ItemsRDD = spark.read().textFile("sample.txt").javaRDD()
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String s) {
						ArrayList<String> li = new ArrayList<String>();
						String[] split = s.split(",");

						for (int i = 1; i < split.length; i++) {
							for (int j = i + 1; j < split.length; j++) {
								li.add(split[i] + "-" + split[j]);
							}

						}
						return li.iterator();
					}
				});

		JavaPairRDD<String, Integer> onesPair = ItemsRDD.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> countsPair = onesPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		// calcolate support
		JavaPairRDD<String, Double> mapToPairWithSupport = countsPair
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Double>() {
					@Override
					public Tuple2<String, Double> call(Tuple2<String, Integer> arg) throws Exception {
						// TODO Auto-generated method stub
						Tuple2<String, Double> t = new Tuple2<String, Double>(arg._1 + "-" + arg._2.toString(),
								(double) (((((double) arg._2 / (double) numberOfElements)) * 100.0)));
						return t;
					}
				});

		/// convert the countsPair to ItemX , value , needed for calculate
		/// confidence
		JavaPairRDD<String, String> mapToItemXValue = mapToPairWithSupport
				.mapToPair(new PairFunction<Tuple2<String, Double>, String, String>() {

					@Override
					public Tuple2<String, String> call(Tuple2<String, Double> arg) throws Exception {
						// TODO Auto-generated method stub
						Tuple2<String, String> t = new Tuple2<String, String>(arg._1.split("-")[0],
								arg._1 + "-" + arg._2.toString());
						return t;
					}

				});

		JavaRDD<ItemPair> map1 = mapToItemXValue.map(new Function<Tuple2<String, String>, ItemPair>() {

			@Override
			public ItemPair call(Tuple2<String, String> arg) throws Exception {
				// TODO Auto-generated method stub
				ItemPair t = new ItemPair();
				t.setFristItem(arg._1);
				t.setSecondItem(arg._2);
				t.setFrequencyOfPair(arg._2.split("-")[2]);
				return t;
			}

		});

		JavaRDD<ItemPair2> map2 = counts.map(new Function<Tuple2<String, Integer>, ItemPair2>() {

			@Override
			public ItemPair2 call(Tuple2<String, Integer> arg) throws Exception {
				// TODO Auto-generated method stub
				ItemPair2 t = new ItemPair2();
				t.setFristItem2(arg._1);
				t.setSecondItem2(arg._2.toString());
				return t;
			}

		});
		Dataset<Row> df1 = spark.createDataFrame(map1, ItemPair.class);
		// Register the DataFrame as a temporary view
		df1.createOrReplaceTempView("df1");
		Dataset<Row> df2 = spark.createDataFrame(map2, ItemPair2.class);
		df2.createOrReplaceTempView("df2");

		org.apache.spark.sql.Column c = new Column("fristItem").equalTo( new Column("fristItem2"));

		Dataset<Row> join2 = df1.join(df2, c, "inner");
		
		JavaRDD<Row> javaRDD = join2.toJavaRDD();
		JavaRDD<MarketBasket> mapTot = javaRDD.map(new Function<Row, MarketBasket>() {

			@Override
			public MarketBasket call(Row row) throws Exception {
				// TODO Auto-generated method stub
				MarketBasket it = new MarketBasket();
				it.setRule(row.getString(2));
        		Double d = Double.valueOf(row.get(0).toString()) / Double.valueOf(row.get(4).toString()) * 100.0;
        		it.setConfidence(d.toString());
        		it.setSupport(row.getString(2).split("-")[3]);
				return it;
			}
			
		});
		Dataset<Row> dfT = spark.createDataFrame(mapTot, MarketBasket.class);

		dfT.show();

//		List<Tuple2<String, Integer>> output = counts.collect();
//		for (Tuple2<?, ?> tuple : output) {
//			System.out.println(tuple._1() + ": " + tuple._2());
//		}
//
//		output = countsPair.collect();
//		for (Tuple2<?, ?> tuple : output) {
//			System.out.println(tuple._1() + ": " + tuple._2());
//		}
//
//		List<Tuple2<String, Double>> output3 = mapToPairWithSupport.collect();
//		for (Tuple2<?, ?> tuple : output3) {
//			System.out.println(tuple._1() + ": " + tuple._2());
//		}
//
//		List<Tuple2<String, String>> output2 = mapToItemXValue.collect();
//		for (Tuple2<?, ?> tuple : output2) {
//			System.out.println(tuple._1() + ": " + tuple._2());
//		}

		//join2.show();

		// output2 = flatMapToPairJoin.collect();
		// for (Tuple2<?, ?> tuple : output) {
		// System.out.println(tuple._1() + ": " + tuple._2());
		// }

	}

	private static void runFirstJobInSparkUsingSQLSchemaObj(SparkSession spark) {
		// TODO Auto-generated method stub
		JavaRDD<Item> ItemsRDD = spark.read().textFile("sample.txt").javaRDD()
				.flatMap(new FlatMapFunction<String, Item>() {
					@Override
					public Iterator<Item> call(String s) {
						ArrayList<Item> li = new ArrayList<Item>();
						String[] product = s.split(",");
						for (int i = 1; i < product.length; i++) {
							Item item = new Item();
							item.setName(product[i]);
							item.setMonth(product[0].split("-")[0] + "-" + product[0].split("-")[1]);
							li.add(item);
						}
						return li.iterator();
					}
				});

		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> ItemsDf = spark.createDataFrame(ItemsRDD, Item.class);
		// Register the DataFrame as a temporary view
		ItemsDf.createOrReplaceTempView("item");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> selectItems = spark.sql("SELECT name FROM item");

		Dataset<Row> groupByItemsPerMonth = ItemsDf.groupBy("month", "name")
				.agg(org.apache.spark.sql.functions.count("name"));

		groupByItemsPerMonth.show();

	}

	public static void runFirstJobInSpark(SparkSession spark) {

		JavaRDD<String> lines = spark.read().textFile("sample.txt").javaRDD();
		// mette in memory gli Item per mese
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) {
				ArrayList<String> li = new ArrayList<>();
				String[] product = s.split(",");
				for (int i = 1; i < product.length; i++) {
					String itemMonth = product[0].split("-")[0] + "-" + product[0].split("-")[1] + "-" + product[i];
					li.add(itemMonth);
				}
				return li.iterator();
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		spark.stop();

	}
}
