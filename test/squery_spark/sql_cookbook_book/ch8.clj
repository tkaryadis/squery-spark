(ns squery-spark.sql-cookbook-book.ch8
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.datasets.udf :refer :all]
            [squery-spark.mongo-connector.utils :refer [load-collection]])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql functions Column RelationalGroupedDataset Encoders)
           (org.apache.spark.sql.expressions Window WindowSpec)
           (org.apache.spark.sql.types DataTypes ArrayType StructType)
           (accumulators ProductAcc))
  (:gen-class))

(defn -main [])

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(def emp (q (load-collection spark :cookbook.emp) [:empno :ename :job :mgr :hiredate :sal :comm :deptno]))
(def dept (load-collection spark :cookbook.dept))
(def bonus (load-collection spark :cookbook.bonus))
(def bonus1 (load-collection spark :cookbook.bonus1))
(def bonus2 (load-collection spark :cookbook.bonus2))
(def t1 (load-collection spark :cookbook.t1))
(def nulls (load-collection spark :cookbook.nulls))

;;1
(q emp
   ((= :ename "CLARK"))
   [:hiredate]
   {:HD_MINUS_5D (add-days :hiredate -5)
    :HD_PLUS_5D (add-days :hiredate 5)
    :HD_MINUS_5M (add-months :hiredate -5)
    :HD_PLUS_5M (add-months :hiredate 5)
    :HD_MINUS_5Y (add-years :hiredate -5)
    :HD_PLUS_5Y  (add-years :hiredate 5)
    }
   show)

;;2
(q emp
   ((or (= :ename "ALLEN") (= :ename "WARD")))
   (sort :!ename)
   (group {:dates (conj-each :hiredate)})
   [{:diff (days-diff (get :dates 0) (get :dates 1))}]
   (show false))

;;2 alt
(q (as emp :e1)
   ((= :e1.ename "ALLEN"))
   (join (as emp :e2)
         (= :e2.ename "WARD"))
   [{:diff (days-diff :e2.hiredate :e1.hiredate)}]
   (show false))

;;3
(q t1
   [{:date1 (date "2006-11-09" "yyyy-MM-dd")}
    {:date2 (date "2006-12-09" "yyyy-MM-dd")}]
   {:diff (days-diff :date2 :date1)}
   {:working-dates  (reduce (fn [v t]
                              (let [dt (add-days :date1 (int t))
                                    dt-n (day-of-week dt)]
                                (if- (and (not= dt-n 1) (not= dt-n 7))
                                  (conj v dt)
                                  v)))
                            (date-array [])
                            (range :diff))}
   {:working-days-count (count :working-dates)}
   (show false))

;;4
(q t1
   [{:date2 (date "1983-01-12" "yyyy-MM-dd")}
    {:date1 (date "1980-12-17" "yyyy-MM-dd")}]
   {:months-dif (months-diff :date2 :date1)
    :years-dif (years-diff :date2 :date1)}
   show)

;;5
(q (as emp :e1)
   ((= :e1.ename "ALLEN"))
   (join (as emp :e2)
         (= :e2.ename "WARD"))
   [{:diff-days (days-diff :e2.hiredate :e1.hiredate)}]
   {:diff-hour (days-to-hours :diff-days)
    :diff-min (days-to-minutes :diff-days)
    :diff-sec (days-to-seconds :diff-days)}
   show)

;;6
(q t1
   [{:date2 (date "1983-12-31" "yyyy-MM-dd")}
    {:date1 (date "1983-01-01" "yyyy-MM-dd")}]
   {:diff (days-diff :date2 :date1)}
   {:working-dates  (reduce (fn [v t]
                              (let [dt (add-days :date1 (int t))
                                    dt-n (day-of-week dt)
                                    dt-str (cond (= dt-n 1) "Sunday"
                                                 (= dt-n 2) "Monday"
                                                 (= dt-n 3) "Tuesday"
                                                 (= dt-n 4) "Wednesady"
                                                 (= dt-n 5) "Thurday"
                                                 (= dt-n 6) "Friday"
                                                 :else "Saturday")]
                                (conj v dt-str)))
                            (string-array [])
                            (range :diff))}
   [{:working-dates (explode :working-dates)}]
   (group :working-dates {:weekday-count (count-acc)})
   show)

;;7
(q (as emp :e1)
   (join (as emp :e2)
         (and (not= :e1.ename :e2.ename)
              (>= :e2.hiredate :e1.hiredate)))
   (group :e1.ename :e1.hiredate {:next-hiredate (min [:e2.hiredate :e2.ename])})
   {:days-diff (days-diff (get :next-hiredate 0) :e1.hiredate)}
   (show false))