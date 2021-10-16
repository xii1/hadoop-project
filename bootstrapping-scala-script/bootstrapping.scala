val FILENAME = "mtcars.csv";
val N_TIMES = 500;
val PERCENTAGE = 0.25;


val roundingDouble = (d: Double) => BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;


// Define lambda function to compute the mean and variance
val computeMeanAndVariance = (rdd: org.apache.spark.rdd.RDD[(String, Double)]) => rdd.mapValues(v => (v, Math.pow(v, 2), 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)).mapValues(v => (v._1 / v._3, v._2 / v._3)).mapValues(v => (v._1, v._2 - Math.pow(v._1, 2))).sortByKey();


// Step 1: Read CSV file and display data
println("----- %s -----".format("Read CSV file and display data"))
val data = spark.read.option("header", true).csv(FILENAME).cache();
data.printSchema();
data.show();


// Step 2: Create key-value pair RDD for a categorical variable and a numeric variable
println("----- %s -----".format("Create key-value pair RDD for a categorical variable and a numeric variable"))
val p = data.select("cyl", "mpg").rdd.map(r => (r.getString(0), r.getString(1).toDouble)).cache();
p.count();


// Step 3: Compute the mean and variance for each category and display the result
println("----- %s -----".format("Compute the mean and variance for each category and display the result"))
val p_result = computeMeanAndVariance(p).cache();
p_result.map(x => (x._1, roundingDouble(x._2._1), roundingDouble(x._2._2))).toDF("Category", "Mean", "Variance").show();


// Step 4: Create a sample for bootstrapping
println("----- %s -----".format("Create a sample for bootstrapping"))
val sampleData = p.sample(false, PERCENTAGE).cache();
sampleData.collect().foreach(print);
println();


// Step 5: Do loop N_TIMES
println("----- %s -----".format("Do loop %d times".format(N_TIMES)))
var sum = sc.emptyRDD[(String, (Double, Double))];
for (i <- 1 to N_TIMES) {
  // Step 5a: Create a resampled data
  val resampledData = sampleData.sample(true, 1.0);

  // Step 5b: Compute and display the mean and variance for each category
  val r = computeMeanAndVariance(resampledData);

  // Step 5c: Keep adding the values in some running sum
  sum = sum.union(r).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).sortByKey().cache();
  sum.collect();
}


// Step 6: Divide each quantity by N_TIMES to get the average and display the result
println("----- %s -----".format("Divide each quantity by %d to get the average and display the result".format(N_TIMES)))
val result = sum.mapValues(v => (v._1 / N_TIMES, v._2 / N_TIMES)).cache();
result.map(x => (x._1, roundingDouble(x._2._1), roundingDouble(x._2._2))).toDF("Category", "Mean", "Variance").show();


// Step 7: Determine the absolute error percentage for each of the values being estimated
println("----- %s -----".format("Compute absolute error for percentage %s".format(PERCENTAGE)));
val actual_result = p_result.mapValues(v => List(v._1, v._2));
val estimate_result = result.mapValues(v => List(v._1, v._2));
val percent_error = (actual_result union estimate_result).reduceByKey(_ ++ _).mapValues(v => (100 * Math.abs(v(0) - v(2)) / v(0), 100 * Math.abs(v(1) - v(3)) / v(1)));
percent_error.sortByKey().map(x => (x._1, roundingDouble(x._2._1), roundingDouble(x._2._2))).toDF("Category", "Error Mean", "Error Variance").show();
