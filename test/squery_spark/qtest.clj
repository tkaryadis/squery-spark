(ns squery-spark.qtest
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.utils :refer :all])
  (:refer-clojure)
  (:import (org.apache.spark.sql Dataset RelationalGroupedDataset Column)
           (java.util HashMap)))


(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")
(def data-path "/home/white/IdeaProjects/squery-spark/")

;;--------------------------Load test data--------------------------------

(def df
  (-> spark
      (.read)
      (.format "csv")
      (.option "header" "true")
      (.option "inferSchema" "true")
      (.load (str data-path  "/data-used/spark-definitive-book/retail-data/by-day/2010-12-01.csv"))))

(.printSchema df)

;;q is like ->, but does some transforms like convert [] to project, {} to addColumns etc

;;;----------------Project----------------------------------------------------


(-> df
    (.select (into-array [(col "CustomerID") (.as (col "CustomerID") "customer") (.as (lit 20) "Customerlit" ) (lit 20)]))
    show)



(-> df
    (select [:CustomerID {:customer :CustomerID} {:customerlit 5} 20])
    ;(.count)
    show)

(q df
   [:CustomerID {:customer :CustomerID} {:customerlit (lit 5)} (lit 20)]
   ;(.count)
   show)

;;;----------------Add columns----------------------------------------------------
(-> df
    (.withColumns (HashMap. {"a" (lit 100) "b" (col "StockCode")}))
    show)

(-> df
    (add-columns {:a (lit 100)})
    show)

(q df
   {:a (lit 100) :b :StockCode}
   show)

;;----------------filter------------------------------------------------------------


(-> df
    (.filter ^Dataset (.and (.equalTo (col "InvoiceNo") 536365)
                            (.equalTo (col "Quantity") 6)))
    show)

(q df
   (filter-columns [(= :InvoiceNo 536365) (= :Quantity 6)])
   show)

(q df
   ((= :InvoiceNo 536365) (= :Quantity 6)))

;;-----project/filter/addfields-------------------------------------------------------

(q df
   [{:newInvoice (+ :InvoiceNo 100000)}]
   ((= :InvoiceNo 536365) (= :Quantity 6))
   {:afield "hello"}
   (unset :afield)
   show)

;;------------sort--------------------------------------------------------------------

(q df
   (sort :InvoiceNo)
   show)

;;---------groupby--------------------------------------------------------------------

(q df
   (.groupBy (into-array Column [(col "InvoiceNo")]))
   (.sum (into-array String ["UnitPrice"]))
   show)

(q df
   (group :InvoiceNo
          {:sum (sum :UnitPrice)})
   show)

;;many accumulators
(q df
   (group :InvoiceNo
          {:sum (sum :UnitPrice)
           :avg (avg :UnitPrice)})
   [:sum]
   show)



;;------------combination example---------------------

(q df
   ((> :InvoiceNo 536365) (> :Quantity 2))
   {:afield "hello"}
   (unset :afield)
   (group :InvoiceNo
          {:sum (sum :UnitPrice)}
          {:avg (avg :UnitPrice)})
   [{:sumavg (div :sum :avg)}]
   show)
