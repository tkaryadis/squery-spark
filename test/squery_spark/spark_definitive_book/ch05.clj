(ns squery-spark.spark_definitive_book.ch05
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.query :refer :all]
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

(println (q dfWithLongColName
            [(keyword "This Long Column-Name")]
            header))

;;df.drop("ORIGIN_COUNTRY_NAME").columns

(println (q df (unset :ORIGIN_COUNTRY_NAME) header))

;dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")

(println (q df (unset :ORIGIN_COUNTRY_NAME :DEST_COUNTRY_NAME) header))

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

(println (q df
            (group :ORIGIN_COUNTRY_NAME :DEST_COUNTRY_NAME)
            (count-s)))

;// in Scala
;df.select("ORIGIN_COUNTRY_NAME").distinct().count()

(println (q df (group :ORIGIN_COUNTRY_NAME) (count-s)))

;;TODO NOT FINISHED