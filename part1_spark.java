object MainObject {  
   def main(args: Array[String]) {  
        var result = search ("Hello")  
        print(result)  
    }  
    def search (a:Any):Any = a match{  
        case 1  => println("One")  
        case "Two" => println("Two")  
        case "Hello" => println("Hello")  
        case _ => println("No")  
              
        }  
}  


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName(appName).setMaster(master)
new SparkContext(conf)

$ ./bin/spark-shell --master local[4] --jars code.jar
$ ./bin/spark-shell --master local[4] --packages "org.example:example:0.1"

val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)

scala> val distFile = sc.textFile("data.txt")
distFile: org.apache.spark.rdd.RDD[String] = data.txt MapPartitionsRDD[10] at textFile at <console>:26

val lines = sc.textFile("data.txt")
val lineLengths = lines.map(s => s.length)
val totalLength = lineLengths.reduce((a, b) => a + b)

lineLengths.persist()

class MyClass {
  val field = "Hello"
  def doStuff(rdd: RDD[String]): RDD[String] = { rdd.map(x => field + x) }
}

def doStuff(rdd: RDD[String]): RDD[String] = {
  val field_ = this.field
  rdd.map(x => field_ + x)
}


var counter = 0
var rdd = sc.parallelize(data)

									// Wrong: Don't do this!!
rdd.foreach(x => counter += x)

println("Counter value: " + counter)


val counts = pairs.reduceByKey((a, b) => a + b)

										//RDD Persistence
										
MEMORY_ONLY	=> 
Store RDD as deserialized Java objects in the JVM. 
If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. 
This is the default level.

										//broadcast variable 
										
scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)

scala> broadcastVar.value
res0: Array[Int] = Array(1, 2, 3)

										//Accumulators
										
scala> val accum = sc.longAccumulator("My Accumulator")
accum: org.apache.spark.util.LongAccumulator = LongAccumulator(id: 0, name: Some(My Accumulator), value: 0)

scala> sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
...
10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

scala> accum.value
res2: Long = 10

//Example of Accumulator 
class VectorAccumulatorV2 extends AccumulatorV2[MyVector, MyVector] {

  private val myVector: MyVector = MyVector.createZeroVector

  def reset(): Unit = {
    myVector.reset()
  }

  def add(v: MyVector): Unit = {
    myVector.add(v)
  }
  ...
}

									// Then, create an Accumulator of this type:
val myVectorAcc = new VectorAccumulatorV2
// Then, register it into spark context:
sc.register(myVectorAcc, "MyVectorAcc1")

val accum = sc.longAccumulator
data.map { x => accum.add(x); x }
// Here, accum is still 0 because no actions have caused the map operation to be computed.


										//Dataframes and datasets 

import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._


val df = spark.read.json("examples/src/main/resources/people.json")

// Displays the content of the DataFrame to stdout
df.show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+


// This import is needed to use the $-notation
import spark.implicits._
// Print the schema in a tree format
df.printSchema()
// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
df.select("name").show()
// +-------+
// |   name|
// +-------+
// |Michael|
// |   Andy|
// | Justin|
// +-------+

// Select everybody, but increment the age by 1
df.select($"name", $"age" + 1).show()
// +-------+---------+
// |   name|(age + 1)|
// +-------+---------+
// |Michael|     null|
// |   Andy|       31|
// | Justin|       20|
// +-------+---------+

// Select people older than 21
df.filter($"age" > 21).show()
// +---+----+
// |age|name|
// +---+----+
// | 30|Andy|
// +---+----+

// Count people by age
df.groupBy("age").count().show()
// +----+-----+
// | age|count|
// +----+-----+
// |  19|    1|
// |null|    1|
// |  30|    1|
// +----+-----+


										// Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

val sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+


									// Register the DataFrame as a global temporary view
df.createGlobalTempView("people")

// Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+

// Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+


										//Creating Datasets


// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface
case class Person(name: String, age: Long)

// Encoders are created for case classes
val caseClassDS = Seq(Person("Andy", 32)).toDS()
caseClassDS.show()
// +----+---+
// |name|age|
// +----+---+
// |Andy| 32|
// +----+---+

// Encoders for most common types are automatically provided by importing spark.implicits._
val primitiveDS = Seq(1, 2, 3).toDS()
primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

// DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
val path = "examples/src/main/resources/people.json"
val peopleDS = spark.read.json(path).as[Person]
peopleDS.show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+

// For implicit conversions from RDDs to DataFrames
import spark.implicits._

// Create an RDD of Person objects from a text file, convert it to a Dataframe
val peopleDF = spark.sparkContext
  .textFile("examples/src/main/resources/people.txt")
  .map(_.split(","))
  .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
  .toDF()
// Register the DataFrame as a temporary view
peopleDF.createOrReplaceTempView("people")

// SQL statements can be run by using the sql methods provided by Spark
val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

// The columns of a row in the result can be accessed by field index
teenagersDF.map(teenager => "Name: " + teenager(0)).show()
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+

// or by field name
teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+

// No pre-defined encoders for Dataset[Map[K,V]], define explicitly
implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
// Primitive types and case classes can be also defined as
// implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

// row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
// Array(Map("name" -> "Justin", "age" -> 19))


import org.apache.spark.sql.types._

// Create an RDD
val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

// The schema is encoded in a string
val schemaString = "name age"

// Generate the schema based on the string of schema
val fields = schemaString.split(" ")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)

// Convert records of the RDD (people) to Rows
val rowRDD = peopleRDD
  .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))

// Apply the schema to the RDD
val peopleDF = spark.createDataFrame(rowRDD, schema)

// Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

// SQL can be run over a temporary view created using DataFrames
val results = spark.sql("SELECT name FROM people")

// The results of SQL queries are DataFrames and support all the normal RDD operations
// The columns of a row in the result can be accessed by field index or by field name
results.map(attributes => "Name: " + attributes(0)).show()
// +-------------+
// |        value|
// +-------------+
// |Name: Michael|
// |   Name: Andy|
// | Name: Justin|
// +-------------+


																	//Aggregations


import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object MyAverage extends UserDefinedAggregateFunction {
  // Data types of input arguments of this aggregate function
  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
  // Data types of values in the aggregation buffer
  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }
  // The data type of the returned value
  def dataType: DataType = DoubleType
  // Whether this function always returns the same output on the identical input
  def deterministic: Boolean = true
  // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
  // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
  // the opportunity to update its values. Note that arrays and maps inside the buffer are still
  // immutable.
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }
  // Updates the given aggregation buffer `buffer` with new input data from `input`
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }
  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
  // Calculates the final result
  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
}

// Register the function to access it
spark.udf.register("myAverage", MyAverage)

val df = spark.read.json("examples/src/main/resources/employees.json")
df.createOrReplaceTempView("employees")
df.show()
// +-------+------+
// |   name|salary|
// +-------+------+
// |Michael|  3000|
// |   Andy|  4500|
// | Justin|  3500|
// |  Berta|  4000|
// +-------+------+

val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
result.show()
// +--------------+
// |average_salary|
// +--------------+
// |        3750.0|
// +--------------+


												//Type-Safe User-Defined Aggregate Functions

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession

case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)

object MyAverage extends Aggregator[Employee, Average, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }
  // Merge two intermediate values
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  // Transform the output of the reduction
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Average] = Encoders.product
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

val ds = spark.read.json("examples/src/main/resources/employees.json").as[Employee]
ds.show()
// +-------+------+
// |   name|salary|
// +-------+------+
// |Michael|  3000|
// |   Andy|  4500|
// | Justin|  3500|
// |  Berta|  4000|
// +-------+------+

// Convert the function to a `TypedColumn` and give it a name
val averageSalary = MyAverage.toColumn.name("average_salary")
val result = ds.select(averageSalary)
result.show()
// +--------------+
// |average_salary|
// +--------------+
// |        3750.0|
// +--------------+


//Generic Load/Save Functions


val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

val sqlDF = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")

																	//Save Modes

Save operations can optionally take a SaveMode, that specifies how to handle existing data if present. 
It is important to realize that these save modes do not utilize any locking and are not atomic
 Additionally, when performing an Overwrite, the data will be deleted before writing out the new data.

Scala/Java							Any Language					Meaning
SaveMode.ErrorIfExists (default)	"error" (default)	When saving a DataFrame to a data source, if data already exists, an exception is expected to be thrown.
SaveMode.Append						"append"			When saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
SaveMode.Overwrite					"overwrite"			Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.
SaveMode.Ignore						"ignore"			Ignore mode means that when saving a DataFrame to a data source, if data already exists, the save operation is expected to not save the contents of the DataFrame and to not change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.


//Saving to Persistent Tables

peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")

usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

peopleDF
  .write
  .partitionBy("favorite_color")
  .bucketBy(42, "name")
  .saveAsTable("people_partitioned_bucketed")
  
															//Parquet Files
 
 // Encoders for most common types are automatically provided by importing spark.implicits._
import spark.implicits._

val peopleDF = spark.read.json("examples/src/main/resources/people.json")

// DataFrames can be saved as Parquet files, maintaining the schema information
peopleDF.write.parquet("people.parquet")

// Read in the parquet file created above
// Parquet files are self-describing so the schema is preserved
// The result of loading a Parquet file is also a DataFrame
val parquetFileDF = spark.read.parquet("people.parquet")

// Parquet files can also be used to create a temporary view and then used in SQL statements
parquetFileDF.createOrReplaceTempView("parquetFile")
val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
namesDF.map(attributes => "Name: " + attributes(0)).show()
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+


//Partition Discovery

path
└── to
    └── table
        ├── gender=male
        │   ├── ...
        │   │
        │   ├── country=US
        │   │   └── data.parquet
        │   ├── country=CN
        │   │   └── data.parquet
        │   └── ...
        └── gender=female
            ├── ...
            │
            ├── country=US
            │   └── data.parquet
            ├── country=CN
            │   └── data.parquet
            └── ...
root
|-- name: string (nullable = true)
|-- age: long (nullable = true)
|-- gender: string (nullable = true)
|-- country: string (nullable = true)


															//Schema Merging

// This is used to implicitly convert an RDD to a DataFrame.
import spark.implicits._

// Create a simple DataFrame, store into a partition directory
val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
squaresDF.write.parquet("data/test_table/key=1")

// Create another DataFrame in a new partition directory,
// adding a new column and dropping an existing column
val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
cubesDF.write.parquet("data/test_table/key=2")

// Read the partitioned table
val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
mergedDF.printSchema()

// The final schema consists of all 3 columns in the Parquet files together
// with the partitioning column appeared in the partition directory paths
// root
//  |-- value: int (nullable = true)
//  |-- square: int (nullable = true)
//  |-- cube: int (nullable = true)
//  |-- key: int (nullable = true)


//Hive metastore Parquet table conversion
//Hive/Parquet Schema Reconciliation
//Metadata Refreshing

// spark is an existing SparkSession
spark.catalog.refreshTable("my_table")


Configuration
Configuration of Parquet can be done using the setConf method on SparkSession or by running SET key=value commands using SQL.

Property_Name						Default								Meaning
spark.sql.parquet.binaryAsString	false		Some other Parquet-producing systems, in particular Impala, Hive, and older versions of Spark
												SQL, do not differentiate between binary data and strings when writing out the Parquet schema.
												This flag tells Spark SQL to interpret binary data as a string to provide compatibility with these systems.
spark.sql.parquet.int96AsTimestamp	true		Some Parquet-producing systems, in particular Impala and Hive, store Timestamp into INT96. 
												This flag tells Spark SQL to interpret INT96 data as a timestamp to provide compatibility with these systems.
spark.sql.parquet.cacheMetadata		true		Turns on caching of Parquet schema metadata. Can speed up querying of static data.
spark.sql.parquet.compression.codec	snappy		Sets the compression codec use when writing Parquet files. Acceptable values include: uncompressed, snappy, gzip, lzo.
spark.sql.parquet.filterPushdown	true		Enables Parquet filter push-down optimization when set to true.
spark.sql.hive.convertMetastoreParquet	true	When set to false, Spark SQL will use the Hive SerDe for parquet tables instead of the built in support.
spark.sql.parquet.mergeSchema		false			When true, the Parquet data source merges schemas collected from all data files,
												otherwise the schema is picked from the summary file or a random data file if no summary file is available.

spark.sql.optimizer.metadataOnly	true 		when true, enable the metadata-only query optimization that use the table's metadata to produce
												the partition columns instead of table scans. It applies when all the columns scanned are partition columns and 
												the query has an aggregate operator that satisfies distinct semantics.
												
														
														
														//JSON Datasets

// Primitive types (Int, String, etc) and Product types (case classes) encoders are
// supported by importing this when creating a Dataset.
import spark.implicits._

// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files
val path = "examples/src/main/resources/people.json"
val peopleDF = spark.read.json(path)

// The inferred schema can be visualized using the printSchema() method
peopleDF.printSchema()
// root
//  |-- age: long (nullable = true)
//  |-- name: string (nullable = true)

// Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

// SQL statements can be run by using the sql methods provided by spark
val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
teenagerNamesDF.show()
// +------+
// |  name|
// +------+
// |Justin|
// +------+

// Alternatively, a DataFrame can be created for a JSON dataset represented by
// a Dataset[String] storing one JSON object per string
val otherPeopleDataset = spark.createDataset(
  """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
val otherPeople = spark.read.json(otherPeopleDataset)
otherPeople.show()
// +---------------+----+
// |        address|name|
// +---------------+----+
// |[Columbus,Ohio]| Yin|
// +---------------+----+


																	//Hive Tables

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

case class Record(key: Int, value: String)

// warehouseLocation points to the default location for managed databases and tables
val warehouseLocation = new File("spark-warehouse").getAbsolutePath

val spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._
import spark.sql

sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

														// Queries are expressed in HiveQL
sql("SELECT * FROM src").show()
// +---+-------+
// |key|  value|
// +---+-------+
// |238|val_238|
// | 86| val_86|
// |311|val_311|
// ...

// Aggregation queries are also supported.
sql("SELECT COUNT(*) FROM src").show()
// +--------+
// |count(1)|
// +--------+
// |    500 |
// +--------+

// The results of SQL queries are themselves DataFrames and support all normal functions.
val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

// The items in DataFrames are of type Row, which allows you to access each column by ordinal.
val stringsDS = sqlDF.map {
  case Row(key: Int, value: String) => s"Key: $key, Value: $value"
}
stringsDS.show()
// +--------------------+
// |               value|
// +--------------------+
// |Key: 0, Value: val_0|
// |Key: 0, Value: val_0|
// |Key: 0, Value: val_0|
// ...

// You can also use DataFrames to create temporary views within a SparkSession.
val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
recordsDF.createOrReplaceTempView("records")

// Queries can then join DataFrame data with data stored in Hive.
sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
// +---+------+---+------+
// |key| value|key| value|
// +---+------+---+------+
// |  2| val_2|  2| val_2|
// |  4| val_4|  4| val_4|
// |  5| val_5|  5| val_5|
// ...


											//specifying storage format for Hive tables
fileFormat
inputFormat, outputFormat
serde
fieldDelim, escapeDelim, collectionDelim, mapkeyDelim, lineDelim

//Interacting with Different Versions of Hive Metastore


												//JDBC To Other Databases

bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar



// Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
// Loading data from a JDBC source
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql:dbserver")
  .option("dbtable", "schema.tablename")
  .option("user", "username")
  .option("password", "password")
  .load()

val connectionProperties = new Properties()
connectionProperties.put("user", "username")
connectionProperties.put("password", "password")
val jdbcDF2 = spark.read
  .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

// Saving data to a JDBC source
jdbcDF.write
  .format("jdbc")
  .option("url", "jdbc:postgresql:dbserver")
  .option("dbtable", "schema.tablename")
  .option("user", "username")
  .option("password", "password")
  .save()

jdbcDF2.write
  .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

// Specifying create table column data types on write
jdbcDF.write
  .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
  .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
  
  
  
															//Performance Tuning

 //Caching Data In Memory

 Property Name									Default		Meaning
=>spark.sql.inMemoryColumnarStorage.compressed	true		When set to true Spark SQL will automatically select a compression codec for each column based on 
															statistics of the data.
=>spark.sql.inMemoryColumnarStorage.batchSize		10000	Controls the size of batches for columnar caching. Larger batch sizes can improve memory utilization and 
															compression, but risk OOMs when caching data.

=>spark.sql.files.maxPartitionBytes	134217728 (128 MB)	
=>spark.sql.files.openCostInBytes	4194304 (4 MB)
=>spark.sql.broadcastTimeout	300
=>spark.sql.autoBroadcastJoinThreshold	10485760 (10 MB)							
=>spark.sql.shuffle.partitions	200


//Running the Spark SQL CLI

./bin/spark-sql

//DataFrame.groupBy retains grouping columns

// In 1.3.x, in order for the grouping column "department" to show up,
// it must be included explicitly as part of the agg function call.
df.groupBy("department").agg($"department", max("age"), sum("expense"))

// In 1.4+, grouping column "department" is included automatically.
df.groupBy("department").agg(max("age"), sum("expense"))

// Revert to 1.3 behavior (not retaining grouping column) by:
sqlContext.setConf("spark.sql.retainGroupColumns", "false")


//UDF Registration Moved to sqlContext.udf (Java & Scala)

sqlContext.udf.register("strLen", (s: String) => s.length())


															//Data Types
Spark SQL and DataFrames support the following data types:

import org.apache.spark.sql.types._


															//Numeric types
ByteType: 		Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
ShortType: 		Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
IntegerType: 	Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
LongType: 		Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
FloatType: 		Represents 4-byte single-precision floating point numbers.
DoubleType: 	Represents 8-byte double-precision floating point numbers.
DecimalType: 	Represents arbitrary-precision signed decimal numbers. Backed internally by java.math.BigDecimal.
				A BigDecimal consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.
				
String type
Binary type
Boolean type
Datetime type
Complex types

StructType	org.apache.spark.sql.Row
StructField	The value type in Scala of the data type of this field (For example, Int for a StructField with the data type IntegerType)



		   ---------------------------------------------//Learning Journals----------------------------------------------------------

Driver
Executors
Client mode
Cluster mode
Local mode.



 //Submit a Spark Job in client mode                                   
    spark-submit --class org.apache.spark.examples.SparkPi spark-home/spark-2.2.0-bin-hadoop2.6/examples/jars/spark-examples_2.11-2.2.0.jar 1000
    //Start an SSh tunnel
    gcloud compute ssh --zone=us-east1-c --ssh-flag="-D" --ssh-flag="10000" --ssh-flag="-N" "spark22-notebook-m"
    //Start the chrome browser using the SSH tunnel
    cd C:\Program Files (x86)\Google\Chrome\Application
    chrome.exe  "http://spark4-m:4040" --proxy-server="socks5://localhost:10000" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=/tmp/spark22-notebook
    //Start a Spark sell with three executors
    spark-shell --num-executors 3
    //Submit a Spark Job in cluster mode
    spark-submit --class org.apache.spark.examples.SparkPi --deploy-mode cluster file:///usr/lib/spark/examples/jars/spark-examples.jar 1000                                            
                             
							 

//Shell command
    find \ -name * > flist.txt
    //Start Spark Shell
    spark-shell
    // Load the data file
    val flistRDD = sc.textFile("flist.txt")
    //Check the number of partitions
    flistRDD.getNumPartitions
    // Redefine the partitions
    val flistRDD = sc.textFile("flist.txt", 5)
    // The second parameter in the above API is the number of partitions. 
    // Verify the new partitons
    flistRDD.getNumPartitions
    // You can iterate to all partitions and count the number of elements in each partition. 
    flistRDD.foreachPartition(p =>println("No of Items in partition-" + p.count(y=>true)) )            


										//----Spark RDD Example-----

1.Spark Job
2.Job Stages
3.Spark Tasks
We loaded the file and asked for the count, and hence Spark started one Job. The job is to calculate the count. 
Spark breaks that job into five tasks because we had five partitions. And it starts one counting task per partition. A task is the smallest unit of work, and it is performed by an executor. 
We talk about stages in the next example. 
For this example, I am executing it in local mode, and I have a single executor. Hence all these tasks are executed by the same executor. 
You can try the same example on a real multi-node cluster and see the difference. 
Great. I think by the end of the video, you will have a fair idea about the parallel processing in Apache Spark. 
There are two main variables to control the degree of parallelism in Apache Spark.

The number of partitions.
The number of executors.
If you have ten partitions, you can achieve ten parallel processes at the most. However, if you have just two executors, all those ten partitions will be queued to those two executors. 
So far so good. Let's do something little more complicated and take our understanding to the next level.

Spark RDD Example
Here is an example in Scala as well as in Python. Let me quickly explain the code.
                                          
    //-------------------Scala Code---------------------
    val flistRDD = sc.textFile("/home/prashant/flist.txt", 5)
    val arrayRDD = flistRDD.map(x=>x.split("/"))
    val kvRDD = arrayRDD.map(a => (a(1),1))
    val fcountRDD= kvRDD.reduceByKey((x,y)=>x+y)
    fcountRDD.collect()

                                                                    
Line one loads a text file into an RDD. The file is quite small. If you keep it in HDFS, it may have one or two blocks in HDFS, So, it is likely that you get one or two partitions by default. However, we want to make five partitions of this data file. 
The line two executes a map method on the first RDD and returns a new RDD. We already know that RDDs are immutable. So, we can't modify the first RDD. Instead, we take the first RDD, perform a map operation and create a new RDD. The map operation splits each line into an array of words. Hence, the new RDD is a collection of Arrays. The third line executes another Map method over the arrayRDD. This time, we generate a tuple. A key value pair. I am taking the second element of the array as my key. And the value is a hardcoded numeric one. 
What am I trying to do? 
Well, I am trying to count the number of files in each different directory. That's why I am taking the directory name as a key and one as a value. Once I have the kvRDD, I can easily count the number of files. All I have to do is to group all the values by the key and sum up the 1s. That's what the fourth line is doing. The ReduceByKey means, group by key and sum those 1s. 
Finally, I collect all the data back from the executors to the driver. 
That's it. 
I am assuming that you already know the mechanics of the map and reduce methods. I have covered all these fundamentals in my Scala tutorials. I have also included a lot of content about functional programming. If you are not familiar with those fundamentals, I strongly recommend that you first go through my Scala training to take full advantage of the Spark tutorials. 
Let's execute it and see what's happening behind the scenes. 
This time I want to use a mult-inode cluster. 
Please watch the video. The video shows following things.

=>Start a six-node Spark cluster.
=>Create the data file on the master node.
=>Copy the data file to HDFS.
=>Start a Spark Shell.
=>Paste the first four lines in the shell.
=>Check the Spark UI.
=>At this stage, you won't see any Spark Jobs. There is a reason for that. 
=>All these functions that we executed on various RDDs are lazy. 
They don't perform anything until you run a Function that is not lazy. We call the lazy Functions as transformations. The non-lazy functions are Actions.                           	
	

	//-------------------------------RDDs offer two types of operations.

//Transformations
//Actions
The transformation operations create a new distributed dataset from an existing distributed dataset. So, they create a new RDD from an existing RDD. 
The Actions are mainly performed to send results back to the driver, and hence they produce a non-distributed dataset. 
The map and the reduceByKey are transformations whereas collect is an action. 
All transformations in Spark are lazy. That means, they don't compute results until an action requires them to provide results. That's why you won't see any jobs in your Spark UI. 
Now, you can execute the action on the final RDD and check the Spark UI once again. 
The video shows you that there is one job, two stages, and ten tasks. 
Apache Spark has completed the Job in two Stages. It couldn't do it in a single Stage, and we will look at the reason in a minute. But for now, just realize that the Spark has completed that Job in two stages. We had five partitions. So, I expected five tasks. Since the job went into two Stages, you will see ten Tasks. Five Task for each stage. 
You will also see a DAG. The Spark DAG shows the whole process in a nice visualization. 
The DAG shows that the Spark was able to complete first three activities in a single stage. But for the ReduceByKey function, it took a new Stage. The question is Why? 
The video shows you a logical diagram to explain the reason and shows the shuffle and sort activity. The Shuffle is the main reason behind a new stage. So, whenever there is a need to move data across the executors, Spark needs a new Stage. Spark engine will identify such needs and break the Job into two Stages. 
While learning Apache Spark, whenever you do something that needs moving data, for example, a group by operation or a join operation, you will notice a new Stage. 
Once we have these key based partitions, it is simple for each executor to calculate the count for the keys that they own. Finally, they send these results to the driver because we executed the collect method. 
This video gives you a fair Idea about the following things.

//RDDs – RDDs are the core of Spark. They represent a partitioned and distributed data set.
//Transformations and Actions – We can perform transformations and actions over the RDDs.
//Spark Job – An action on an RDD triggers a job.
//Stages – Spark breaks the job into stages.Shuffle and Sort – A shuffle activity is a reason to break the job into two stages.
//Shuffle and Sort – A shuffle activity is a reason to break the job into two stages.
//Tasks – Each stage is executed in parallel tasks. The number of parallel tasks is directly dependent on the number of partitions.
//Executors – Apart from the tasks, the number of available executors is also a constraint on the degree of parallelism.



//-------------------------------------------------------------DataFrames--------------------------------------------------------------

1. quote -> Quote is the character used to enclose the string values. Quoting your string value is critical if you have a field that contains a comma.
 The default value is the double quote character, and hence we can rely on the default value for our example.

2. inferSchema -> Infer schema will automatically guess the data types for each field. If we set this option to TRUE, 
the API will read some sample records from the file to infer the schema. If we want to set this value to false, we must specify a schema explicitly.

3. nullValue -> The null value is used to define the string that represents a null.

4. timestampFormat -> This one is to declare the timestamp format used in your CSV file.

5. mode -> This one is crucial. It defines the method for dealing with a corrupt record. There are three supported modes. 
PERMISSIVE, DROPMALFORMED, and FAILFAST. 
The first two options allow you to continue loading even if some rows are corrupt. 
The last one throws an exception when it meets a corrupted record. We will be using the last 
one in our example because we do not want to proceed in case of data errors. 
We can use the following code to load the data from the CSV file.

//Spark Example to load a csv file in Scala
 val df = spark.read.options( Map("header" -> "true",
                                     "inferSchema" -> "true", 
                                     "nullValue" -> "NA",
                                     "timestampFormat" -> "yyyy-MM-dd'T'HH:mm​:ss",
                                     "mode" -> "failfast")
                                ).csv("/home/prashant/spark-data/survey.csv")                                        
                            
							
//Spark Example to load a csv file in Python
df = spark.read.options(header="true", \
                            inferSchema="true", \
                            nullValue = "NA", \
                            timestampFormat= "yyyy-MM-dd'T'HH:mm​:ss", \
                            mode = "failfast").csv("/home/prashant/spark-data/survey.csv")                                      
 
