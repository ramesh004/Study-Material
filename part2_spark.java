//Spark API to load a csv file in Scala

val df = spark.read
                  .format("csv")
                  .option("header","true")
                  .option("inferSchema","true") 
                  .option("nullValue","NA")
                  .option("timestampFormat","yyyy-MM-dd'T'HH:mm​:ss")
                  .option("mode","failfast")
                  .option("path","/home/prashant/spark-data/survey.csv")
                  .load()  

We are using a format method to specify the data source type. There are many formats available to us, 
and the community is working to add a lot more. Some of my favorites are CSV, JSON, parquet, JDBC, Kafka, and Cassandra.
In the earlier API call, we created a MAP of all the options and passed them at once. You can do that in this API call as well. However, 
I prefer to supply each option individually. It is just a personal choice.
You might notice that I provided the file location as an option. However,
the load method can also take the path as an argument. Which one to use is again a personal preference.

//Where is DataFrame documentation?
Surprisingly, there is no Scala Class for Data Frame. 
You might find it in Python documentation, but in Scala, Data Frame is not a class. A Scala Data Frame is a data set of the following type. 
type DataFrame = Dataset[Row] 
So, we have to look into the DataSet class.
All the methods available to a DataSet is also available to the Data Frames. Well, A hell lot of methods, 
But don't worry about them because 80% of your requirement can be satisfied by just a handful of these APIs, and I will cover the most critical ones.

df.rdd.getNumPartitions                                  
 val df5= df.repartition(5).toDF                                 
 df5.rdd.getNumPartitions    
df.select("Timestamp", "Age","remote_work","leave").filter("Age > 30").show 
 df5.printSchema

 
 
 
=>Transformations
=>Actions

lines.count() 

															//Itversity

/ RDD from files in HDFS
val orders = sc.textFile("/public/retail_db/orders")

// RDD from files in local file system
val productsRaw = scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines.toList
val products = sc.parallelize(productsRaw)


val orders = sc.textFile("/public/retail_db/orders")

// Previewing data
orders.first
orders.take(10).foreach(println)
orders.count

// Use collect with care. 
// As it creates single threaded list from distributed RDD, 
// using collect on larger datasets can cause out of memory issues.
orders.collect.foreach(println)



//Reading different file formats

// JSON files are under this location on the lab
// You can download from github as well and copy to HDFS
hadoop fs -ls /public/retail_db_json/orders

sqlContext.read.json("/public/retail_db_json/orders").show
sqlContext.load("/public/retail_db_json/orders", "json").show

2.

//String Manipulation
val str = orders.first
val a = str.split(",")
val orderId = a(0).toInt
a(1).contains("2013")

val orderDate = a(1)
orderDate.substring(0, 10)
orderDate.substring(5, 7)
orderDate.substring(11)
orderDate.replace('-', '/')
orderDate.replace("07", "July")
orderDate.indexOf("2")
orderDate.indexOf("2", 2)
orderDate.length

//map

// Row level transformations using map

val orders = sc.textFile("/public/retail_db/orders")
// 21,2013-07-25 00:00:00.0,11599,CLOSED -> 20130725 as Int
val str = orders.first
str.split(",")(1).substring(0, 10).replace("-", "").toInt

val orderDates = orders.map((str: String) => {
  str.split(",")(1).substring(0, 10).replace("-", "").toInt
})

val ordersPairedRDD = orders.map(order => {
  val o = order.split(",")
  (o(0).toInt, o(1).substring(0, 10).replace("-", "").toInt)
})

val orderItems = sc.textFile("/public/retail_db/order_items")
val orderItemsPairedRDD = orderItems.map(orderItem => {
  (orderItem.split(",")(1).toInt, orderItem)
})

								//flatMap


// Row level transformations using flatMap

val l = List("Hello", "How are you doing", "Let us perform word count", "As part of the word count program", "we will see how many times each word repeat")
val l_rdd = sc.parallelize(l)
val l_map = l_rdd.map(ele => ele.split(" "))
val l_flatMap = l_rdd.flatMap(ele => ele.split(" "))
val wordcount = l_flatMap.map(word => (word, "")).countByKey


							// Filtering data
orders.filter(order => order.split(",")(3) == "COMPLETE")
orders.count
orders.filter(order => order.split(",")(3) == "COMPLETE").count
// Get all the orders from 2013-09 which are in closed or complete
orders.map(order => order.split(",")(3)).distinct.collect.foreach(println)
val ordersFiltered = orders.filter(order => {
  val o = order.split(",")
  (o(3) == "COMPLETE" || o(3) == "CLOSED") && (o(1).contains("2013-09"))
})
ordersFiltered.take(10).foreach(println)
ordersFiltered.count


							//Joining data sets
// Joining orders and order_items
val orders = sc.textFile("/public/retail_db/orders")
val orderItems = sc.textFile("/public/retail_db/order_items")
val ordersMap = orders.map(order => {
  (order.split(",")(0).toInt, order.split(",")(1).substring(0, 10))
})
val orderItemsMap = orderItems.map(orderItem => {
  val oi = orderItem.split(",")
  (oi(1).toInt, oi(4).toFloat)
})
val ordersJoin = ordersMap.join(orderItemsMap)


							//Outer Join
// Get all the orders which do not have corresponding entries in order items
val orders = sc.textFile("/public/retail_db/orders")
val orderItems = sc.textFile("/public/retail_db/order_items")
val ordersMap = orders.map(order => {
  (order.split(",")(0).toInt, order)
})
val orderItemsMap = orderItems.map(orderItem => {
  val oi = orderItem.split(",")
  (oi(1).toInt, orderItem)
})
val ordersLeftOuterJoin = ordersMap.leftOuterJoin(orderItemsMap)
val ordersLeftOuterJoinFilter = ordersLeftOuterJoin.filter(order => order._2._2 == None)
val ordersWithNoOrderItem = ordersLeftOuterJoinFilter.map(order => order._2._1)
ordersWithNoOrderItem.take(10).foreach(println)
val ordersRightOuterJoin = orderItemsMap.rightOuterJoin(ordersMap)
val ordersWithNoOrderItem = ordersRightOuterJoin.
  filter(order => order._2._1 == None).
  map(order => order._2._2)
ordersWithNoOrderItem.take(10).foreach(println)

					// Aggregations - using actions
val orders = sc.textFile("/public/retail_db/orders")
orders.map(order => (order.split(",")(3), "")).countByKey.foreach(println)
val orderItems = sc.textFile("/public/retail_db/order_items")
val orderItemsRevenue = orderItems.map(oi => oi.split(",")(4).toFloat)
orderItemsRevenue.reduce((total, revenue) => total + revenue)
val orderItemsMaxRevenue = orderItemsRevenue.reduce((max, revenue) => {
  if(max < revenue) revenue else max
})

					//Role of Combiner

=>Computing intermediate values and then using intermediate values to compute final values is called combiner
=>Aggregations such as sum, min, max, average etc can perform better using combiner

					// Aggregations - groupByKey
//1, (1 to 1000) - sum(1 to 1000) => 1 + 2+ 3+ .....1000
//1, (1 to 1000) - sum(sum(1, 250), sum(251, 500), sum(501, 750), sum(751, 1000))
val orderItems = sc.textFile("/public/retail_db/order_items")
val orderItemsMap = orderItems.
  map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
val orderItemsGBK = orderItemsMap.groupByKey
//Get revenue per order_id
orderItemsGBK.map(rec => (rec._1, rec._2.toList.sum)).take(10).foreach(println)
//Get data in descending order by order_item_subtotal for each order_id
val ordersSortedByRevenue = orderItemsGBK.
  flatMap(rec => {
    rec._2.toList.sortBy(o => -o).map(k => (rec._1, k))
  })

				// Aggregations - reduceByKey
val orderItems = sc.textFile("/public/retail_db/order_items")
val orderItemsMap = orderItems.
  map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))

val revenuePerOrderId = orderItemsMap.
  reduceByKey((total, revenue) => total + revenue)

val minRevenuePerOrderId = orderItemsMap.
  reduceByKey((min, revenue) => if(min > revenue) revenue else min)
  
  
				// Aggregations - aggregateByKey
val orderItems = sc.textFile("/public/retail_db/order_items")
val orderItemsMap = orderItems.
  map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))

//(order_id, order_item_subtotal)
val revenueAndMaxPerProductId = orderItemsMap.
  aggregateByKey((0.0f, 0.0f))(
    (inter, subtotal) => (inter._1 + subtotal, if(subtotal > inter._2) subtotal else inter._2),
    (total, inter) => (total._1 + inter._1, if(total._2 > inter._2) total._2 else inter._2)
  )
//(order_id, (order_revenue, max_order_item_subtotal))
  
  
								// Sorting - sortByKey
val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  map(product => (product.split(",")(1).toInt, product))
val productsSortedByCategoryId = productsMap.sortByKey(false)

val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => ((product.split(",")(1).toInt, -product.split(",")(4).toFloat), product))

val productsSortedByCategoryId = productsMap.sortByKey().map(rec => rec._2)
  
					// Ranking - Global (details of top 10 products)
val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(4).toFloat, product))
val productsSortedByPrice = productsMap.sortByKey(false)
productsSortedByPrice.take(10).foreach(println)
val products = sc.textFile("/public/retail_db/products")
products.
  filter(product => product.split(",")(4) != "").
  takeOrdered(10)(Ordering[Float].reverse.on(product => product.split(",")(4).toFloat)).
  foreach(println)
  
				// Ranking - Get top N priced products with in each product category
val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(1).toInt, product))
val productsGroupByCategory = productsMap.groupByKey
 
				//Scala APIs to get top N Prices

val productsIterable = productsGroupByCategory.first._2
val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
val topNPrices = productPrices.toList.sortBy(p => -p).take(5)

// Function to get top n priced products using Scala collections API

val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(1).toInt, product))
val productsGroupByCategory = productsMap.groupByKey

def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): Iterable[String] = {
  val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
  val topNPrices = productPrices.toList.sortBy(p => -p).take(topN)

  val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
  val minOfTopNPrices = topNPrices.min

  val topNPricedProducts = productsSorted.takeWhile(product => product.split(",")(4).toFloat >= minOfTopNPrices)

  topNPricedProducts
}

val productsIterable = productsGroupByCategory.first._2
getTopNPricedProducts(productsIterable, 3).foreach(println)



					// Ranking - Get top N priced products with in each product category

val products = sc.textFile("/public/retail_db/products")
val productsMap = products.filter(product => product.split(",")(4) != "").
map(product => (product.split(",")(1).toInt, product))
val productsGroupByCategory = productsMap.groupByKey

def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): Iterable[String] = {
  val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
  val topNPrices = productPrices.toList.sortBy(p => -p).take(topN)

  val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
  val minOfTopNPrices = topNPrices.min

  val topNPricedProducts = productsSorted.takeWhile(product => product.split(",")(4).toFloat >= minOfTopNPrices)

  topNPricedProducts
}

val top3PricedProductsPerCategory = productsGroupByCategory.flatMap(rec => getTopNPricedProducts(rec._2, 3))
				
										// Set operations

val orders = sc.textFile("/public/retail_db/orders")
val customers_201308 = orders.filter(order => order.split(",")(1).contains("2013-08")).
  map(order => order.split(",")(2).toInt)

val customers_201309 = orders.filter(order => order.split(",")(1).contains("2013-09")).
  map(order => order.split(",")(2).toInt)

// Get all the customers who placed orders in 2013 August and 2013 September
val customers_201308_and_201309 = customers_201308.intersection(customers_201309)

// Get all unique customers who placed orders in 2013 August or 2013 September
val customers_201308_union_201309 = customers_201308.union(customers_201309).distinct

// Get all customers who placed orders in 2013 August but not in 2013 September
val customer_201308_minus_201309 = customers_201308.map(c => (c, 1)).
  leftOuterJoin(customers_201309.map(c => (c, 1))).
  filter(rec => rec._2._2 == None).map(rec => rec._1).distinct
  
  
//Saving RDD – Text file format

RDD have below APIs to save data into different file formats

saveAsTextFile (most important and covered here)
saveAsSequenceFile
saveAsNewAPIHadoopFile
saveAsObjectFile
  
//Saving RDD – Compression

  
//Saving data in other formats
json (as shown below)
parquet
orc
text
csv (3rd party plugin)
avro (3rd party plugin, but cloudera clusters are well integrated with avro)

There are 2 APIs which can be used to save data frames

save – takes 2 arguments, path and file format
write – provides interfaces such as json to save data in the path specified



//SPARK-SQL

create database dgadiraju_retail_db_txt;
use dgadiraju_retail_db_txt;

create table orders (
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
) row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/orders' into table orders;

create table order_items (
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
) row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/order_items' into table order_items;



//Creating tables and insert data Hive supports other file formats(avro,orc,parquet,sequencefile,)
create database dgadiraju_retail_db_orc;
use dgadiraju_retail_db_orc;

create table orders (
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
) stored as orc;

insert into table orders select * from dgadiraju_retail_db_txt.orders;

create table order_items (
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
) stored as orc;

insert into table order_items select * from dgadiraju_retail_db_txt.order_items;

					//Spark SQL – Analytics and Windowing Functions

select * from (
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal, 
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where order_revenue >= 1000
order by order_date, order_revenue desc, rank_revenue;

select * from (
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal, 
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue,
rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
percent_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
lead(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) lead_order_item_subtotal,
lag(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) lag_order_item_subtotal,
first_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) first_order_item_subtotal,
last_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) last_order_item_subtotal,
row_number() over (partition by o.order_id) rn_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where order_revenue >= 1000
order by order_date, order_revenue desc, rnk_revenue;

//Create Data Frame and Register as temp table

val ordersRDD = sc.textFile("/public/retail_db/orders")
val ordersDF = ordersRDD.map(order => {
  (order.split(",")(0).toInt, order.split(",")(1), order.split(",")(2).toInt, order.split(",")(3))
}).toDF("order_id", "order_date", "order_customer_id", "order_status")

ordersDF.registerTempTable("orders")
sqlContext.sql("select order_status, count(1) count_by_status from orders group by order_status").show()

sqlContext.sql("use dgadiraju_retail_db_orc")
val productsRaw = scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines.toList
val productsRDD = sc.parallelize(productsRaw)
val productsDF = productsRDD.map(product => {
  (product.split(",")(0).toInt, product.split(",")(2))
}).
toDF("product_id", "product_name")

productsDF.registerTempTable("products")

sqlContext.sql("select * from products").show

----------------------------------------------
sqlContext.sql("use dgadiraju_retail_db_orc")
sqlContext.setConf("spark.sql.shuffle.partitions", "2")
sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product " +
"FROM orders o JOIN order_items oi " +
"ON o.order_id = oi.order_item_order_id " +
"JOIN products p ON p.product_id = oi.order_item_product_id " +
"WHERE o.order_status IN ('COMPLETE', 'CLOSED') " +
"GROUP BY o.order_date, p.product_name " +
"ORDER BY o.order_date, daily_revenue_per_product desc").
show
-------------------------------------------------------------------------------------
//Saving Data Frame

sqlContext.sql("CREATE DATABASE dgadiraju_daily_revenue")
sqlContext.sql("CREATE TABLE dgadiraju_daily_revenue.daily_revenue " +
"(order_date string, product_name string, daily_revenue_per_product float) " +
"STORED AS orc")

val daily_revenue_per_product = sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product " +
"FROM orders o JOIN order_items oi " +
"ON o.order_id = oi.order_item_order_id " +
"JOIN products p ON p.product_id = oi.order_item_product_id " +
"WHERE o.order_status IN ('COMPLETE', 'CLOSED') " +
"GROUP BY o.order_date, p.product_name " +
"ORDER BY o.order_date, daily_revenue_per_product desc")

daily_revenue_per_product.insertInto("dgadiraju_daily_revenue.daily_revenue")
-------------------------------------------------------------------------------------
daily_revenue_per_product.save("/user/dgadiraju/daily_revenue_save", "json")
daily_revenue_per_product.write.json("/user/dgadiraju/daily_revenue_write")
daily_revenue_per_product.select("order_date", "daily_revenue_per_product")
daily_revenue_per_product.filter(daily_revenue_per_product("order_date") === "2013-07-25 00:00:00.0").count


//---------------------------Itversity END--------------------------------------------------------------------------

->Typed Transformations
->Untyped Transformations

Both of these are available to data frames. The untyped transformations might return you a dataset. 
But you can convert a dataset to a data frame using the toDF function. However, you don't need to worry too much about 
it because Spark can take care of that automatically. 
Transformations are the basic building blocks for Spark developers.
They are the key tools for the developers because we use transformations to express our business logic. 
Let's come back to our example. What do we want to do? If you check the dataset . 
You will see a field for treatment. This field records the response to the following question. 

val df = spark.read
                  .format("csv")
                  .option("header","true")
                  .option("inferSchema","true") 
                  .option("nullValue","NA")
                  .option("timestampFormat","yyyy-MM-dd'T'HH:mm​:ss")
                  .option("mode","failfast")
                  .option("path","/home/prashant/spark-data/mental-health-in-tech-survey/survey.csv")
                  .load()       
				  
				
  val df1 = df.select( $"Gender",$"treatment")
               df1.show                 
			   
			   
val df2 = df1.select($"Gender",
                (when($"treatment" === "Yes", 1).otherwise(0)).alias("All-Yes"),
                (when($"treatment" === "No", 1).otherwise(0)).alias("All-Nos")
                         )                       
 val df2 = df.select($"Gender",
                       (when($"treatment" === "Yes", 1).otherwise(0)).alias("All-Yes"),
                       (when($"treatment" === "No", 1).otherwise(0)).alias("All-Nos")
                        )                  
        df2.collect                                             



Now you will see one job in Spark UI. 
If you jump to the SQL tab in the Spark UI and click on the collect job, you will get a lot of details. 
You will see four plans.

1.Parsed Logical Plan
2.Analyzed Logical Plan
3.Optimized Logical Plan
4.Physical Plan
When we execute an action, Spark takes the user code. In our case, It takes those two select transformations. 
It will then parse the user code and generate a parsed logical plan. 
The second step is to analyze the initial plan and resolve the column names and their data types.
 The output of the second step is an analyzed logical plan. 
 Apache Spark maintains a catalog of all the tables and data frame information.
 The analyzer makes a call to the catalog and resolves the initial plan. 
 The analyzed plan clearly shows the column names and the datatypes. 
The analyzed logical plan goes to an optimizer. As of now, the optimizer mainly performs two types of optimizations.

1.Pipelining
2.Predicate pushdown

Pipelining is as simple as combining multiple transformations together. We created two transformations.
 Both were the select operations. Spark realizes that it can combine them together into a single transformation. So, it simply does that. 
You can cross check it by looking at the optimized plan. 
The pipelining optimization doesn't only apply to a select transformation.
Spark will look for all such opportunities and apply the pipelining where ever it is applicable. 
The other type of optimization is the predicate pushdown. 
That simply means pushing down the filter conditions to the early stage instead of applying it at the end. 
The optimized logical plan goes to the Spark compiler that generates a bunch of physical execution plans.
 The physical execution plan is nothing but a series of RDD transformations. 
 The Spark engine generates multiple physical plans based on various considerations. 
 Those considerations might be a different approach to perform a join operation. 
 It may be the physical attributes of the underlying data file. 
 It may be something else. However, Spark settles down to a single physical plan that it evaluates to be the best among others. 
 Finally, The best plan goes for the execution on the cluster. 
Great. Let's come back to our example. We loaded data. We applied one transformation. 
Then we applied another transformation. But we haven't reached the desired output.
 I think if group by the gender and compute a sum over the second and third column, we should get the desired output. 
Let's try that.
                              
    val df3 = df2.groupBy("Gender").agg( sum($"All-Yes"),sum($"All-Nos"))                                     
                         
//Writing the function 
                                
    def parseGender(g: String) = {  
                g.toLowerCase match {
                    case "male" | "m" | "male-ish" | "maile" |
                         "mal" | "male (cis)" | "make" | "male " |
                         "man" | "msle" | "mail" | "malr" |
                         "cis man" | "cis male" => "Male"
                    case "cis female" | "f" | "female" |
                         "woman" |  "femake" | "female " |
                         "cis-female/femme" | "female (cis)" |
                         "femail" => "Female"
                    case _ => "Transgender"
                }
    }                                             
                         
//Registering the UDF
                                
   val parseGenderUDF = udf( parseGender _ )                                             
                         
Spark will serialize the function on the driver and transfer it over the network to all executor processes. So, now we can use the parseGenderUDF in our data frames. 
Let's create another transformation to fix our data quality problem.
 //using the function                               
    val df3 = df2.select((parseGenderUDF($"Gender")).alias("Gender"),
                            $"All-Yes",
                            $"All-Nos"
                        )                                             
                         

I am using the data frame df2 and applying another select transformation. This time, we apply the parseGenderUDF to the gender field. 
We also take All-Yes and All-Nos fields that we created earlier. Now, we can do a group by on df3.

                                
    val df4 = df3.groupBy("Gender").agg( sum($"All-Yes"),sum($"All-Nos"))                                           
                         
// =>  O/P                            
        df4.show                                           
                         
// =>All the Code at Once
                                
    spark.conf.set("spark.sql.shuffle.partitions", 2)
    val df = spark.read
                  .format("csv")
                  .option("header","true")
                  .option("inferSchema","true") 
                  .option("nullValue","NA")
                  .option("timestampFormat","yyyy-MM-dd'T'HH:mm?:ss")
                  .option("mode","failfast")
                  .option("path","/home/prashant/spark-data/mental-health-in-tech-survey/survey.csv")
                  .load()
    val df1 = df.select( $"Gender",$"treatment")
    val df2 = df.select($"Gender",
                         (when($"treatment" === "Yes", 1).otherwise(0)).alias("All-Yes"),
                         (when($"treatment" === "No", 1).otherwise(0)).alias("All-Nos")
                       )
    def parseGender(g: String) = {  
      g.toLowerCase match {
        case "male" | "m" | "male-ish" | "maile" |
             "mal" | "male (cis)" | "make" | "male " |
             "man" | "msle" | "mail" | "malr" |
             "cis man" | "cis male" => "Male"
        case "cis female" | "f" | "female" |
             "woman" |  "femake" | "female " |
             "cis-female/femme" | "female (cis)" |
             "femail" => "Female"
        case _ => "Transgender"
       }
       }
    val parseGenderUDF = udf(parseGender _)
    val df3 = df2.select((parseGenderUDF($"Gender")).alias("Gender"),
                          $"All-Yes",
                          $"All-Nos"
                        )
    val df4 = df3.groupBy("Gender").agg( sum($"All-Yes"),sum($"All-Nos"))
    val df5 = df4.filter($"Gender" =!= "Transgender")
	
    df5.collect                                           

//Amazing SQL

val df = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true") 
    .option("nullValue","NA")
    .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
    .option("mode","failfast")
    .load("/home/prashant/spark-data/survey.csv")
                                    
    //Then we applied a select transformation and a filter condition.
    val sel = df.select("Timestamp", "Age","remote_work","leave").filter("Age > 30")     
	
	
	 select timestamp, age,remote_work,leave
    from survey_tbl
    where age > 30;         
	
// --Same UDF	
	
	 select gender, sum(all_yes), sum(all_nos) 
    from (select case when lower(trim(gender)) in ('male','m','male-ish','maile','mal',
                                                   'male (cis)','make','male','man','msle',
                                                   'mail', 'malr','cis man', 'cis male') 
                      then 'Male' 
                      when lower(trim(gender)) in ('cis female','f','female','woman',
                                                 'femake','female ','cis-female/femme',
                                                 'female (cis)','femail') 
                      then 'Female'
                      else 'Transgender' 
                 end as gender,
                 case when treatment == 'Yes' then 1 else 0 end as all_yes,
                 case when treatment == 'No'  then 1 else 0 end as all_nos
          from survey_tbl)
          where gender != 'Transgender'
          group by gender                                      
                        
//How to define a Spark Schema

//You can create a Schema for survey data set using below code
    import org.apache.spark.sql.types._
    val surveySchema = StructType(Array(StructField("timestamp",TimestampType,true), 
                                        StructField("age",LongType,true), 
                                        StructField("gender",StringType,true), 
                                        StructField("country",StringType,true), 
                                        StructField("state",StringType,true), 
                                        StructField("self_employed",StringType,true), 
                                        StructField("family_history",StringType,true), 
                                        StructField("treatment",StringType,true), 
                                        StructField("work_interfere",StringType,true), 
                                        StructField("no_employees",StringType,true), 
                                        StructField("remote_work",StringType,true), 
                                        StructField("tech_company",StringType,true), 
                                        StructField("benefits",StringType,true), 
                                        StructField("care_options",StringType,true), 
                                        StructField("wellness_program",StringType,true), 
                                        StructField("seek_help",StringType,true), 
                                        StructField("anonymity",StringType,true), 
                                        StructField("leave",StringType,true), 
                                        StructField("mental_health_consequence",StringType,true),
                                        StructField("phys_health_consequence",StringType,true), 
                                        StructField("coworkers",StringType,true), 
                                        StructField("supervisor",StringType,true), 
                                        StructField("mental_health_interview",StringType,true),
                                        StructField("phys_health_interview",StringType,true),
                                        StructField("mental_vs_physical",StringType,true), 
                                        StructField("obs_consequence",StringType,true), 
                                        StructField("comments",StringType,true))
                                )

    //You can load the data using above schema
    val df = spark.read
                 .format("csv")
                 .schema(surveySchema)
                 .option("header","true")
                 .option("nullValue","NA")
                 .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
                 .option("mode","failfast")
                 .load("/home/prashant/spark-data/survey.csv")   
				 
				 
//Spark Temporary View

createGlobalTempView
createOrReplaceGlobalTempView
createOrReplaceTempView
createTempView

   df.createOrReplaceTempView("survey_tbl")                                                

   spark.catalog.listTables.show                                                
   df.createOrReplaceGlobalTempView("survey_gtbl")                                                
   spark.catalog.listTables("global_temp").show                                               

   spark.sql("""select timestamp, age,remote_work,leave
    from survey_tbl
    where age > 30""")      


spark.sql("""select gender, sum(yes), sum(no) 
            from (select case when lower(trim(gender)) in ('male','m','male-ish','maile','mal',
                                                           'male (cis)','make','male ','man','msle',
                                                           'mail','malr','cis man','cis male') 
                              then 'Male' 
                              when lower(trim(gender)) in ('cis female','f','female','woman',
                                                           'female','female ','cis-female/femme',
                                                           'female (cis)','femail') 
                              then 'Female'
                              else 'Transgender' 
                              end as gender,
                              case when treatment == 'Yes' then 1 else 0 end as yes,
                              case when treatment == 'No' then 1 else 0 end as no
                  from survey_tbl) 
         where gender != 'Transgender'
         group by gender""").show
                                                        
														//Spark Streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

// Create a local StreamingContext with two working thread and batch interval of 1 second.
// The master requires 2 cores to prevent from a starvation scenario.

val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
val ssc = new StreamingContext(conf, Seconds(1))


// Create a DStream that will connect to hostname:port, like localhost:9999
val lines = ssc.socketTextStream("localhost", 9999)
// Split each line into words
val words = lines.flatMap(_.split(" "))
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
// Count each word in each batch
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)

// Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.print()
ssc.start()             // Start the computation
ssc.awaitTermination()  // Wait for the computation to terminate

$ nc -lk 9999


$ ./bin/run-example streaming.NetworkWordCount localhost 9999


//Initializing StreamingContext

import org.apache.spark._
import org.apache.spark.streaming._

val conf = new SparkConf().setAppName(appName).setMaster(master)
val ssc = new StreamingContext(conf, Seconds(1))


import org.apache.spark.streaming._
val sc = ...                // existing SparkContext
val ssc = new StreamingContext(sc, Seconds(1))

//Input DStreams and Receivers

  streamingContext.fileStream[KeyClass, ValueClass, InputFormatClass](dataDirectory)

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = ...  // add the new values with the previous running count to get the new count
    Some(newCount)
}
val runningCounts = pairs.updateStateByKey[Int](updateFunction _)

val spamInfoRDD = ssc.sparkContext.newAPIHadoopRDD(...) // RDD containing spam information

val cleanedDStream = wordCounts.transform { rdd =>
  rdd.join(spamInfoRDD).filter(...) // join data stream with spam information to do data cleaning
  ...
}

// Reduce last 30 seconds of data, every 10 seconds
val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))
//Join Operations
val stream1: DStream[String, String] = ...
val stream2: DStream[String, String] = ...
val joinedStream = stream1.join(stream2)

val windowedStream1 = stream1.window(Seconds(20))
val windowedStream2 = stream2.window(Minutes(1))
val joinedStream = windowedStream1.join(windowedStream2)

//Stream-dataset joins
val dataset: RDD[String, String] = ...
val windowedStream = stream.window(Seconds(20))...
val joinedStream = windowedStream.transform { rdd => rdd.join(dataset) }

print()	
saveAsTextFiles(prefix, [suffix])	
saveAsObjectFiles(prefix, [suffix])	
saveAsHadoopFiles(prefix, [suffix])	
foreachRDD(func)	

//Design Patterns for using foreachRDD

dstream.foreachRDD { rdd =>
  val connection = createNewConnection()  // executed at the driver
  rdd.foreach { record =>
    connection.send(record) // executed at the worker
  }
}

dstream.foreachRDD { rdd =>
  rdd.foreach { record =>
    val connection = createNewConnection()
    connection.send(record)
    connection.close()
  }
}

dstream.foreachRDD { rdd =>
  rdd.foreachPartition { partitionOfRecords =>
    val connection = createNewConnection()
    partitionOfRecords.foreach(record => connection.send(record))
    connection.close()
  }
}


dstream.foreachRDD { rdd =>
  rdd.foreachPartition { partitionOfRecords =>
    // ConnectionPool is a static, lazily initialized pool of connections
    val connection = ConnectionPool.getConnection()
    partitionOfRecords.foreach(record => connection.send(record))
    ConnectionPool.returnConnection(connection)  // return to the pool for future reuse
  }
}

//DataFrame and SQL Operations


/** DataFrame operations inside your streaming program */

val words: DStream[String] = ...

words.foreachRDD { rdd =>

  // Get the singleton instance of SparkSession
  val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
  import spark.implicits._

  // Convert RDD[String] to DataFrame
  val wordsDataFrame = rdd.toDF("word")

  // Create a temporary view
  wordsDataFrame.createOrReplaceTempView("words")

  // Do word count on DataFrame using SQL and print it
  val wordCountsDataFrame = 
    spark.sql("select word, count(*) as total from words group by word")
  wordCountsDataFrame.show()
}




// Function to create and setup a new StreamingContext

def functionToCreateContext(): StreamingContext = {
  val ssc = new StreamingContext(...)   // new context
  val lines = ssc.socketTextStream(...) // create DStreams
  ...
  ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
  ssc
}

// Get StreamingContext from checkpoint data or create a new one
val context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

// Do additional setup on context that needs to be done,
// irrespective of whether it is being started or restarted
context. ...

// Start the context
context.start()
context.awaitTermination()

//Accumulators, Broadcast Variables, and Checkpoints

object WordBlacklist {

  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

object DroppedWordsCounter {

  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}

wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
  // Get or register the blacklist Broadcast
  val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
  // Get or register the droppedWordsCounter Accumulator
  val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
  // Use blacklist to drop words and use droppedWordsCounter to count them
  val counts = rdd.filter { case (word, count) =>
    if (blacklist.value.contains(word)) {
      droppedWordsCounter.add(count)
      false
    } else {
      true
    }
  }.collect().mkString("[", ", ", "]")
  val output = "Counts at time " + time + " " + counts
})

-----------------------------------------------------------------------------------------------------------------------------
//Setting up Partition size
scala> val inputrdd = sc.parallelize(Seq( ("key1", 1), ("key2", 2), ("key1", 3)))
inputrdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[80] at parallelize at :21

scala> val noPartition = inputrdd.reduceByKey((x, y) => x + y)
noPartition: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[81] at reduceByKey at :23

scala> noPartition.partitions.length
res50: Int = 8

scala> //Here Partition size is given as a second argument
scala> val withPartition = inputrdd.reduceByKey((x, y) => x + y, 11)
withPartition: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[82] at reduceByKey at :23

scala> withPartition.partitions.length
res51: Int = 11

scala> val repartitioned = withPartition.repartition(16)
repartitioned: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[86] at repartition at :25

scala> repartitioned.partitions.length
res52: Int = 16

scala> val coalesced = if(4 < repartitioned.partitions.length) {
     |    //Note : Use coalesce() only when the new partition size is
     |    //       less than the current partition size of the RDD
     |    repartitioned.coalesce(4)
     | }else {
     |    repartitioned
     | }
coalesced: org.apache.spark.rdd.RDD[(String, Int)] = CoalescedRDD[87] at coalesce at :30

scala> coalesced.partitions.length
res53: Int = 4

-----------------------------------------------------------------------------------------------------------------------------
//Combiner in Pair RDDs : combineByKey()
1st Argument : createCombiner
2nd Argument : mergeValue is called when the key already has an accumulator
3rd Argument : mergeCombiners is called when more that one partition has accumulator for the same key

scala> val inputrdd = sc.parallelize(Seq(("maths", 50), ("maths", 60),("english", 65),("physics", 66), ("physics", 61), ("physics", 87)), 1)
inputrdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[41] at parallelize at <console>:27

scala> inputrdd.getNumPartitions                      
res55: Int = 1

scala> val reduced = inputrdd.combineByKey((mark) => { println(s"Create combiner -> ${mark}")(mark, 1)},
     |     (acc: (Int, Int), v) => {println(s"""Merge value : (${acc._1} + ${v}, ${acc._2} + 1)""")(acc._1 + v, acc._2 + 1)},
     |     (acc1: (Int, Int), acc2: (Int, Int)) => {println(s"""Merge Combiner : (${acc1._1} + ${acc2._1}, ${acc1._2} + ${acc2._2})""")
     |       (acc1._1 + acc2._1, acc1._2 + acc2._2)})
	 
reduced: org.apache.spark.rdd.RDD[(String, (Int, Int))] = ShuffledRDD[42] at combineByKey at <console>:29

scala> reduced.collect()
Create combiner -> 50
Merge value : (50 + 60, 1 + 1)
Create combiner -> 65
Merge value : (66 + 61, 1 + 1)
Merge value : (127 + 87, 2 + 1)
res56: Array[(String, (Int, Int))] = Array((maths,(110,2)), (physics,(214,3)), (english,(65,1)))

scala> val result = reduced.mapValues(x => x._1 / x._2.toFloat)
result: org.apache.spark.rdd.RDD[(String, Float)] = MapPartitionsRDD[43] at mapValues at <console>:31

scala> result.collect()
res57: Array[(String, Float)] = Array((maths,55.0), (physics,71.333336), (english,65.0))

Note:The map side aggregation done using combineByKey() 
can also be disabled(which is the case with methods like groupByKey() where the functionality of the combiner is not needed)
------------------------------------------
//Executors
Runs computation & Stores Application Data
Are launched at the beginning of an Application & runs for the entire life time of an Application
Each Application gets it own Executors
An Application can have multiple Executors
An Executor is not shared by Multiple Applications
Provides in-memory storage for RDDs
For an Application, No >1 Executors run in the same Node

//Task
Represents a unit of work in Spark
Gets executed in Executor

------------------------------------------------------------------------------------
scala> import org.apache.spark.storage.StorageLevel
...
scala> lines.persist(StorageLevel.MEMORY_ONLY) // We can also use cache() method if we need MEMORY_ONLY storage level

We use unpersist() to unpersist RDD. When the cached data exceeds the Memory capacity, 
Spark automatically evicts the old partitions(it will be recalculated when needed).
 This is called Last Recently used Cache(LRU) policy
...
scala> lines.count() (1)

scala> lines.partitions.size


//Spark Submit

object WordCount {
   def main(args: Array[String]) {
 
      println("In main : " + args(0) + "," + args(1))
 
      //Create Spark Context
      val conf = new SparkConf().setAppName("WordCountApp")
      val sc = new SparkContext(conf)
 
      //Load Data from File
      val input = sc.textFile(args(0))
 
      //Split into words
      val words = input.flatMap(line => line.split(" "))
 
      //Assign unit to each word
      val units = words.map ( word => (word, 1) )
 
      //Reduce each key
      val counts = units.reduceByKey ( (x, y) => x + y )
 
      //Write output to Disk
      counts.saveAsTextFile(args(1))
 
      //Shutdown spark. System.exit(0) or sys.exit() can also be used
      sc.stop();
   }
}


							//Countbyvalue
							
scala> val inputrdd = sc.parallelize{ Seq(10, 4, 3, 3) }
inputrdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[28] at parallelize at :47

scala> inputrdd.countByValue()
res34: scala.collection.Map[Int,Long] = Map(10 -> 1, 3 -> 2, 4 -> 1)
							//Reduce
scala> val rdd1 = sc.parallelize(List(1, 2, 5)) 
scala> val sum = rdd1.reduce{ (x, y) => x + y}
sum: Int = 8

val seq = Seq(3,9,2,3,5,4)
val rdd = sc.parallelize(seq,2)
rdd.takeOrdered(2)(Ordering[Int].reverse)
=>Result will be Array(9,5)

// top
 sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
// returns Array(12)

                        //collectAsMap

scala> val rdd = sc.parallelize(Seq(("math",    55), ("math",    56),("english", 57), ("english", 58),("science", 59),("science", 54)))
scala> val reslt2 = rdd.collectAsMap()
reslt2: scala.collection.Map[String,Int] = Map(math -> 56, science -> 54, english -> 58)
						//lookup
scala> //Example : lookup()
scala> val result3 = rdd.lookup("math")
result3: Seq[Int] = WrappedArray(55, 56)



scala> //Reference : Learning Spark (Page 38)

scala> val rdd1 = sc.parallelize(List("lion", "tiger", "tiger", "peacock", "horse"))
rdd1: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[33] at parallelize at :21
scala> val rdd2 = sc.parallelize(List("lion", "tiger"))
rdd2: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[34] at parallelize at :21

										// distinct()
scala> // distinct(): Returns distinct element in the RDD
scala> // Warning   :Involves shuffling of data over N/W
scala> rdd1.distinct().collect()
res20: Array[String] = Array(peacock, lion, horse, tiger)

										//union
scala> // union() : Returns an RDD containing data from both sources
scala> // Note    : Unlike the Mathematical Union, duplicates are
scala> //           not removed. Also type should be same in both the RDD
scala> rdd1.union(rdd2).collect()
res22: Array[String] = Array(lion, tiger, tiger, peacock, horse, lion, tiger)

									//intersection
scala> // intersection() :  Returns elements that are common b/w both
scala> //                   RDDs. Also removed Duplicates
scala> // Warning        :  Involves shuffling & has worst performance
scala> rdd1.intersection(rdd2).collect();
res24: Array[String] = Array(lion, tiger)

										//subtract
scala> // subtract() : Returns only elements that are present in the
scala> //              first RDD
scala> rdd1.subtract(rdd2).collect()
res26: Array[String] = Array(peacock, horse)

										//cartesian
scala> // cartesian(): Provides cartesian product b/w 2 RDDs
scala> // Warning    : Is very expensive for large RDDs
scala> rdd1.cartesian(rdd2).collect();
res28: Array[(String, String)] = Array((lion,lion), (lion,tiger), (tiger,lion), (tiger,tiger), (tiger,lion), (tiger,tiger), (peacock,lion), (peacock,tiger), (horse,lion), (horse,tiger))

