(ns squery-spark.spark-definitive-book.ch06
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.delta-lake.queries :refer :all]
            [squery-spark.delta-lake.schema :refer :all]
            [squery-spark.datasets.udf :refer :all])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql functions Column RelationalGroupedDataset Dataset)
           (org.apache.spark.sql.expressions Window WindowSpec)
           (org.apache.spark.sql.types DataTypes ArrayType)
           (java.util Arrays)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/data-used/spark-definitive-book/")   ;;CHANGE THIS!!!

;;val df = spark.read.format("csv")
;  .option("header", "true")
;  .option("inferSchema", "true")
;  .load("/data/retail-data/by-day/2010-12-01.csv")
;df.printSchema()
;df.createOrReplaceTempView("dfTable")

(def df (-> spark .read (.format "csv")
            (.option "header" "true")
            (.option "inferSchema" "true")
            (.load (str data-path "/retail-data/by-day/2010-12-01.csv"))))

(.printSchema df)
(show df)

;import org.apache.spark.sql.functions.lit
;df.select(lit(5), lit("five"), lit(5.0))

(q df [5 "five" 5.0] show)

;df.where(col("InvoiceNo").equalTo(536365))
;  .select("InvoiceNo", "Description")
;  .show(5, false)

(q df
   ((= :InvoiceNo 536365))
   [:InvoiceNo :Description]
   (show 5 false))

;;val priceFilter = col("UnitPrice") > 600
;val descripFilter = col("Description").contains("POSTAGE")
;df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))
;  .show()

(q df
   ((contains? ["DOT"] :StockCode)
    (or (> :UnitPrice 600) (substring? "POSTAGE" :description)))
   show)

;;val DOTCodeFilter = col("StockCode") === "DOT"
;val priceFilter = col("UnitPrice") > 600
;val descripFilter = col("Description").contains("POSTAGE")
;df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
;  .where("isExpensive")
;  .select("unitPrice", "isExpensive").show(5)


(q df
   {:isExpensive (and (= :StockCode "DOT")
                      (or (> :UnitPrice 600)
                          (substring? "POSTAGE" :description)))}
   ((true? :isExpensive))
   [:UnitPrice :isExpensive]
   (show 5))


;;df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
;  .filter("isExpensive")
;  .select("Description", "UnitPrice").show(5)

(q df
   {:isExpensive (not (<= :UnitPrice 250))}
   ((true? :isExpensive))
   [:Description :UnitPrice]
   (show 5))

;;df.where(col("Description").eqNullSafe("hello")).show()
(q df
   ((=safe :Description "hello"))
   show)

;;val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
;df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

(q df
   [:CustomerId {:realQuantity (+ (pow (* :Quantity :UnitPrice) 2) 5)}]
   (show 2))

;;df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

(q df
   [{:rounded (round :UnitPrice 1)} :UnitPrice]
   (show 5))


;;df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

(q df
   [(round "2.5") (bround "2.5")]
   (show 2))

;df.stat.corr("Quantity", "UnitPrice")

(q df
   .stat
   (corr :Quantity :UnitPrice)
   prn)

;;;df.select(corr("Quantity", "UnitPrice")).show()
(q df
   [(corr :Quantity :UnitPrice)]
   show)

;;df.describe().show()

(q df
   describe
   show)

;;val colName = "UnitPrice"
;val quantileProbs = Array(0.5)
;val relError = 0.05
;df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

(q df
   stat
   (approx-quantile (c/double-array [0.5]) 0.05 :UnitPrice)
   Arrays/toString
   println)

;;skipped
;df.stat.crosstab("StockCode", "Quantity").show()
;df.stat.freqItems(Seq("StockCode", "Quantity")).show()
;df.select(monotonically_increasing_id()).show(2)

;;df.select(initcap(col("Description"))).show(2, false)

(q df
   [(capitalize :Description)]
   (show 2 false))

;;df.select(col("Description"),
;  lower(col("Description")),
;  upper(lower(col("Description")))).show(2)

(q df
   [:Description (upper-case :Description) (lower-case :Description)]
   (show 2))

;df.select(
;    ltrim(lit("    HELLO    ")).as("ltrim"),
;    rtrim(lit("    HELLO    ")).as("rtrim"),
;    trim(lit("    HELLO    ")).as("trim"),
;    lpad(lit("HELLO"), 3, " ").as("lp"),
;    rpad(lit("HELLO"), 10, " ").as("rp")).show(2)

(q df
   (limit 1)
   [{:triml (triml "    HELLO    ")}
    {:trimr (trimr "    HELLO    ")}
    {:trim (trim "    HELLO    ")}
    {:padl (padl "HELLO" 8 "#")}
    {:padr (padr "HELLO" 10 "#")}
    ]
   (show 2))

;;val simpleColors = Seq("black", "white", "red", "green", "blue")
;val regexString = simpleColors.map(_.toUpperCase).mkString("|")
;// the | signifies `OR` in regular expression syntax
;df.select(
;  regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
;  col("Description")).show(2)

(def simple-colors ["black", "white", "red", "green", "blue"])

(let [regex-string (clojure.string/join "|" (map clojure.string/upper-case simple-colors))]
  (q df
     [:Description {:color-clean (replace :Description regex-string "COLOR")}]
     (show false)))

;;alt (use of clojure.core c, inside the q scope)
(q df
   [:Description
    {:color-clean (replace :Description
                           (clojure.string/join "|" (c/map clojure.string/upper-case simple-colors))
                           "COLOR")}]
   (show false))


;df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
;  .show(2)

(q df
   [(translate :Description "LEET" "1337") :Description]
   (show 2))

;val regexString = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
;// the | signifies OR in regular expression syntax
;df.select(
;     regexp_extract(col("Description"), regexString, 1).alias("color_clean"),
;     col("Description")).show(2)

(let [regex-string (str "(" (clojure.string/join "|" (map clojure.string/upper-case simple-colors)) ")")]
  (q df
     [{:color-clean (re-find regex-string :Description 1)} :Description]
     (show 2)))


;val containsBlack = col("Description").contains("BLACK")
;val containsWhite = col("DESCRIPTION").contains("WHITE")
;df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
;  .where("hasSimpleColor")
;  .select("Description").show(3, false)

(q df
   {:hasSimpleColor (or (substring? "BLACK" :Description)
                        (substring? "White" :Description))}
   ((true? :hasSimpleColor))
   [:Description]
   (show 3 false))

;val simpleColors = Seq("black", "white", "red", "green", "blue")
;val selectedColumns = simpleColors.map(color => {
;   col("Description").contains(color.toUpperCase).alias(s"is_$color")
;}):+expr("*") // could also append this value
;df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
;  .select("Description").show(3, false)

(let [color-map (reduce (fn [color-map color]
                          (merge color-map
                                 (sq {(c/keyword (c/str "is-" color))
                                      (substring? (clojure.string/upper-case color) :Description)})))
                        {}
                        simple-colors)]
  (q df
     (add-columns color-map)
     ((or :is-white :is-red))
     [:Description]
     (show 3 false)))

;;df.printSchema()

(.printSchema df)

;;-------------------------------------------Dates---------------------------------------------------------------

;;val dateDF = spark.range(10)
;  .withColumn("today", current_date())
;  .withColumn("now", current_timestamp())
;dateDF.createOrReplaceTempView("dateTable")

(def date-df
  (q spark
     (.range 10)
     {:today (current-date)
      :now (current-timestamp)}))

(.printSchema date-df)
(show date-df)

;;dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

(q date-df
   [(sub-days :today 5) :today (add-days :today 5)]
   (show 1))


;;dateDF.withColumn("week_ago", date_sub(col("today"), 7))
;  .select(datediff(col("week_ago"), col("today"))).show(1)

(q date-df
   {:week-ago (sub-days :today 7)}
   [(days-diff :week-ago :today)]
   (show 1))

;;dateDF.select(
;;    to_date(lit("2016-01-01")).alias("start"),
;;    to_date(lit("2017-05-22")).alias("end"))
;;  .select(months_between(col("start"), col("end"))).show(1)

(q date-df
   [{:start (date "2016-01-01")} {:end (date "2017-05-22")}]
   [(months-between :start :end)]
   (show 1))

;;spark.range(5).withColumn("date", lit("2017-01-01"))
;  .select(to_date(col("date"))).show(1)

(q spark
   (.range 5)
   {:date "2017-01-01"}
   [(date :date)]
   (show 1))


;dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

(q date-df
   [(date "2016-20-12") (date "2017-12-11")]
   (show 1))

;val dateFormat = "yyyy-dd-MM"
;val cleanDateDF = spark.range(1).select(
;    to_date(lit("2017-12-11"), dateFormat).alias("date"),
;    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
;cleanDateDF.createOrReplaceTempView("dateTable2")

(def clean-date-df
  (q spark (.range 1)
     [{:date (date "2017-12-11" "yyyy-dd-MM")}
      {:date2 (date "2017-20-12" "yyyy-dd-MM")}]))

(show clean-date-df)

;;cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
;cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()

(q clean-date-df [(timestamp :date "yyyy-dd-MM")] show)
(q clean-date-df ((> :date2 "2017-12-12")) show)

;;df.select(coalesce(col("Description"), col("CustomerId"))).show()

(q df
   [(coalesce :Description :CustomerId)]
   show)

;df.na.drop()
;df.na.drop("any")
;df.na.drop("all")
;df.na.drop("all", Seq("StockCode", "InvoiceNo"))

(q df (drop-na))
(q df (drop-na "any"))                                   ;;drop row if any column null/nan
(q df (drop-na "all"))                                   ;;drop row if all colums are null/nan
(q df (drop-na "all" [:StockCode :InvoiceNo]))             ;;drop row if condition for this 2 columns

;df.na.fill("All Null values become this string")
;df.na.fill(5, Seq("StockCode", "InvoiceNo"))
;val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
;df.na.fill(fillColValues)

(q df (fill-na "All Null values become this string"))
(q df (fill-na 5 [:StockCode :InvoiceNo]))
(q df (fill-na {:StockCode 5 :Description "No Value"}))

;;;;df.na.replace("Description", Map("" -> "UNKNOWN"))
(q df (replace-na :Description {"" "UNKNOWN"}))

;;df.selectExpr("(Description, InvoiceNo) as complex", "*")
;df.selectExpr("struct(Description, InvoiceNo) as complex", "*")

(q df
   {:complex '(:Description :InvoiceNo)}
   (show false))

;;val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
;complexDF.createOrReplaceTempView("complexDF")
;complexDF.select("complex.Description")
;complexDF.select(col("complex").getField("Description"))

(def complex-df (q df [{:complex '(:Description :InvoiceNo)}]))
(q complex-df [(get :complex :Description)] (show false))
(q complex-df [:complex.Description] (show false))
(q complex-df [:complex.*] (show false))

;;df.select(split(col("Description"), " ")).show(2)
;df.select(split(col("Description"), " ").alias("array_col"))
;  .selectExpr("array_col[0]").show(2)

(q df [(split-str :Description " ")] (show 2))
(q df
   [{:array-col (split-str :Description " ")}]
   [(get :array-col 0)]
   (show 2))

;;df.select(size(split(col("Description"), " "))).show(2)

(q df [(count (split-str :Description " "))] (show 2))

;;;df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)
;df.withColumn("splitted", split(col("Description"), " "))
;  .withColumn("exploded", explode(col("splitted")))
;  .select("Description", "InvoiceNo", "exploded").show(2)

(q df [(contains? (split-str :Description " ") "WHITE")] (show 2))
(q df
   {:splitted (split-str :Description " ")}
   {:exploded (explode :splitted)}
   [:Description :InvoiceNo :exploded]
   (show 2))

;;df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)
;df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
;  .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)

(q df
   [{:complex-map {:Description :InvoiceNo}}]
   (show 2 false))

(q df
   [{:complex-map {:Description :InvoiceNo}}]
   [(mget :complex-map "WHITE METAL LANTERN")]
   (show 2))

;;;;df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
;;  .selectExpr("explode(complex_map)").show(2)

(q df [{:complex-map {:Description :InvoiceNo}}] [(explode :complex-map)] (show 2))

;;json skipped

;;for udf see udftest.clj

