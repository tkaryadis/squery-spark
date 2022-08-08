(ns squery-spark.spark_definitive_book.ch05
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql SparkSession Dataset)
           (org.apache.spark.api.java JavaSparkContext)
           (org.apache.spark SparkContext)
           (org.apache.spark.sql.types ByteType Metadata)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/spark-definitive-book/")   ;;CHANGE THIS!!!

;;// in Scala
;val df = spark.read.format("json")
;  .load("/data/flight-data/json/2015-summary.json")
;df.printSchema()
;spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema

(def df (-> spark
            .read
            (.format "json")
            (.load (str data-path "/flight-data/2015-summary.json"))))

(.printSchema df)
(.schema df)

;;val myManualSchema = StructType(Array(
;  StructField("DEST_COUNTRY_NAME", StringType, true),
;  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
;  StructField("count", LongType, false,
;    Metadata.fromJson("{\"hello\":\"world\"}"))
;))

(def my-manual-schema
  (build-schema
    [:DEST_COUNTRY_NAME
     :ORIGIN_COUNTRY_NAME
     [:count :long false (Metadata/fromJson "{\"hello\":\"world\"}")]]))

;;val df = spark.read.format("json").schema(myManualSchema)
;  .load("/data/flight-data/json/2015-summary.json")

(def df (-> spark .read (.format "json") (.schema my-manual-schema)
            (.load (str data-path "/flight-data/2015-summary.json"))))

;;(((col("someCol") + 5) * 200) - 6) < col("otherCol")

(sq (< :otherCol (- (* (+ :someCol 5) 200) 6)))

;;spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
;  .columns

(def columns (-> spark .read (.format "json")
                 (.load (str data-path "/flight-data/2015-summary.json"))
                 header))
(println columns)

;;df.first()

(print-rows (.first df))

;;val myRow = Row("Hello", null, 1, false)
;myRow(0) // type Any
;myRow(0).asInstanceOf[String] // String
;myRow.getString(0) // String
;myRow.getInt(2) // Int

(def my-row (row "Hello" nil (int 1)  false))
(println (get my-row 0))
(println (.getString my-row 0))
(println (.getInt my-row 2))

;;val myManualSchema = new StructType(Array(
;  new StructField("some", StringType, true),
;  new StructField("col", StringType, true),
;  new StructField("names", LongType, false)))
;val myRows = Seq(Row("Hello", null, 1L))
;val myRDD = spark.sparkContext.parallelize(myRows)
;val myDf = spark.createDataFrame(myRDD, myManualSchema)
;myDf.show()

(def my-manual-schema (build-schema [:some :col [:names :long false]]))
(def myRDD (seq->rdd spark [["Hello" nil 1]]))
(def mydf (rdd->df spark myRDD my-manual-schema))
(.show mydf)

;;val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")
(def mydf (seq->df spark [["Hello" nil 1]] my-manual-schema))
(.show mydf)

;df.select("DEST_COUNTRY_NAME").show(2)

(q df [:DEST_COUNTRY_NAME] (.show 2))

;;df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

(q df [{:destination :DEST_COUNTRY_NAME}] (.show 2))

;;df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

(q df
   (group nil
          {:avg (avg :count)
           :count-d (count-distinct :DEST_COUNTRY_NAME)})
   (.show 2))

;;;df.select(expr("*"), lit(1).as("One")).show(2)

;;TODO support * on select

;;df.withColumn("numberOne", lit(1)).show(2)

(q df {:number-one 1} (.show 2))

;;df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
;  .show(2)

(q df
   {:withinCountry (= :ORIGIN_COUNTRY_NAME :DEST_COUNTRY_NAME)}
   (.show 2))

;;df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

;;TODO add suport for rename

;;val dfWithLongColName = df.withColumn(
;  "This Long Column-Name",
;  expr("ORIGIN_COUNTRY_NAME"))

(def dfWithLongColName
  (q df
     {"This Long Column-Name" :ORIGIN_COUNTRY_NAME}))

(q df
   {(keyword "This Long Column-Name") :ORIGIN_COUNTRY_NAME}
   .show)

;;dfWithLongColName.select(col("This Long Column-Name")).columns

(q dfWithLongColName
   [(keyword "This Long Column-Name")]
   header
   println)

;;df.drop("ORIGIN_COUNTRY_NAME").columns

(q df (unset :ORIGIN_COUNTRY_NAME) header println)

;dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")

(q df (unset :ORIGIN_COUNTRY_NAME :DEST_COUNTRY_NAME) header println)

;df.withColumn("count2", col("count").cast("long"))

(q df {:count2 (cast :count :long)} .show)

;df.filter(col("count") < 2).show(2)
;df.where("count < 2").show(2)

(q df ((< :count 2)) (.show 2))


;;df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
;  .show(2)

(q df ((< :count 2) (not= :ORIGIN_COUNTRY_NAME "Croatia")) (.show 2))

;// in Scala
;df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

(q df
   (group :ORIGIN_COUNTRY_NAME :DEST_COUNTRY_NAME)
   (count-s)
   println)

;// in Scala
;df.select("ORIGIN_COUNTRY_NAME").distinct().count()

(q df (group :ORIGIN_COUNTRY_NAME) (count-s) println)

;;val seed = 5
;val withReplacement = false
;val fraction = 0.5
;df.sample(withReplacement, fraction, seed).count()

(def seed 5)
(def with-replacement false)
(def fraction 0.5)

(-> df (.sample with-replacement fraction seed) (count-s) println)

;// in Scala
;val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
;dataFrames(0).count() > dataFrames(1).count() // False

(def dataframes (-> df (.randomSplit (double-array [0.25 0.75]) seed)))
(-> (aget dataframes 0) count-s println)

;;import org.apache.spark.sql.Row
;val schema = df.schema
;val newRows = Seq(
;  Row("New Country", "Other Country", 5L),
;  Row("New Country 2", "Other Country 3", 1L)
;)
;val parallelizedRows = spark.sparkContext.parallelize(newRows)
;val newDF = spark.createDataFrame(parallelizedRows, schema)
;df.union(newDF)
;  .where("count = 1")
;  .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
;  .show() // get all of them and we'll see our new rows at the end

(def newDF (seq->df spark
                    [["New Country" "Other Country" 5] ["New Country 2" "Other Country 3" 1]]
                    (.schema df)))

(q df
   (union-with newDF)
   ((= :count 1) (not= :ORIGIN_COUNTRY_NAME "United States"))
   .show)

;;df.sort("count").show(5)
;df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
;df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)


(q df (sort :count) (.show 5))
(q df (sort :count :DEST_COUNTRY_NAME) (show 5))

;;df.orderBy(expr("count desc")).show(2)
;df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

(q df (sort :!count) (show 2))
(q df (sort :!count :DEST_COUNTRY_NAME) (show 2))

;;df.limit(5).show()

(q df (limit 5) show)

;;df.orderBy(expr("count desc")).limit(6).show()

(q df (sort :!count) (limit 6) show)

;df.rdd.getNumPartitions // 1

(-> df (.rdd) (.getNumPartitions) println)

;df.repartition(5)
;df.repartition(col("DEST_COUNTRY_NAME"))
;df.repartition(5, col("DEST_COUNTRY_NAME"))
;df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

(-> df (repartition 5 :DEST_COUNTRY_NAME) .rdd .getNumPartitions println)

;;val collectDF = df.limit(10)
;collectDF.take(5) // take works with an Integer count
;collectDF.show() // this prints it out nicely
;collectDF.show(5, false)
;collectDF.collect()
;collectDF.toLocalIterator()

(def collectDF (-> df (limit 10)))
(show collectDF)
(show collectDF 5 false)
(print-rows (take-rows collectDF 5))
(-> collectDF .collectAsList print-rows)
(print-rows (-> df .toLocalIterator .next))