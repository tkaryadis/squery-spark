(ns squery-spark.sql-cookbook-book.ch6
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
  (:import (org.apache.spark.sql functions Column RelationalGroupedDataset)
           (org.apache.spark.sql.expressions Window WindowSpec)
           (org.apache.spark.sql.types DataTypes ArrayType StructType)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(def emp (q (load-collection spark :cookbook.emp) [:empno :ename :job :mgr :hiredate :sal :comm :deptno]))
(def dept (load-collection spark :cookbook.dept))
(def bonus (load-collection spark :cookbook.bonus))
(def bonus1 (load-collection spark :cookbook.bonus1))
(def bonus2 (load-collection spark :cookbook.bonus2))
(def t1 (load-collection spark :cookbook.t1))

;;1
(q t1
   [{:a "Clojure+Spark"}]
   {:a (explode (split-str :a ""))}
   show)

;;2 skipped

;;3
(q t1
   [{:a "10,CLARK,MANAGER"}]
   {:ncommas (count-str (replace :a "[^,]" ""))}
   show)

;;4
(q emp
   [:ename :sal]
   {:stripped1 (replace :ename "[AEIOU]" "")
    :stripped2 (replace (string :sal) "0" "")}
   show)

;;5
(q emp
   [{:data (str :ename :sal)}]
   {:ename (replace :data "\\d" "")
    :sal (long (replace :data "\\D" ""))}
   show)

;;6
(q t1
   [{:data (explode ["CLARK"
                     "KING"
                     "MILLER"
                     "SMITH, $800.00"
                     "JONES, $2975.00"
                     "SCOTT, $3000.00"
                     "ADAMS, $1100.00"
                     "FORD, $3000.00"
                     "ALLEN30"
                     "WARD30"
                     "MARTIN30"
                     "BLAKE30"
                     "TURNER30"
                     "JAMES30"])}]
   ((not (re-find? "\\W" :data)))
   show)

;;7
(q t1
   [{:fullname (str "Stewie Griffin")}]
   {:initials (split-str :fullname " ")}
   {:initials (map (comp #(str % ".") (partial take-str 0 1)) :initials)}
   {:initials (reduce (fn [v m] (str v m)) "" :initials)}
   (show false))

;;7 alt
(q t1
   [{:fullname (str "Stewie Griffin")}]
   {:initials (second (reduce (fn [v m]
                                (if- (= (get v 0) " ")
                                  [m (str (get v 1) m ".")]
                                  [m (get v 1)]))
                              [" " ""]
                              (split-str :fullname "")))}
   (show false))

;;8
(q emp
   [:ename]
   (sort (take-str -2 2 :ename))
   show)

;;9
(q t1
   [{:data (explode ["CLARK 7782 ACCOUNTING"
                     "KING 7839 ACCOUNTING"
                     "MILLER 7934 ACCOUNTING"
                     "SMITH 7369 RESEARCH"
                     "JONES 7566 RESEARCH"
                     "SCOTT 7788 RESEARCH"
                     "ADAMS 7876 RESEARCH"
                     "FORD 7902 RESEARCH"
                     "ALLEN 7499 SALES"
                     "WARD 7521 SALES"])}]
   (sort (long (re-find "\\d+" :data)))
   show)

;;10
(q emp
   (group :deptno
          {:emps (conj-each :ename)})
   [:deptno {:emps (join-str "," :emps)}]
   show)

;;11
(q emp
   ((contains? (map long (split-str "7654,7698,7782,7788" ",")) :empno))
   show)

;;12
(q emp
   [{:old-name :ename}
    {:new-name (join-str (sort-array (split-str :ename "")))}]
   (sort :new-name)
   show)

;;13
(q t1
   [{:data (explode ["CL10AR"
                     "KI10NG"
                     "MI10LL"
                     "7369"
                     "7566"
                     "7788"
                     "7876"
                     "7902"
                     "ALLEN"
                     "WARD"
                     "MARTIN"
                     "BLAKE"
                     "TURNER"
                     "JAMES" ])}]
   [{:mixed (replace :data "\\D" "")}]
   ((> (count-str :mixed) 0))
   show)

;;14
(q t1
   [{:data (explode ["mo,larry,curly"
                     "tina,gina,jaunita,regina,leena"])}]
   {:sub (second (split-str :data ","))}
   show)

;;15  (solved in general case with uknown parts, with pivot)
(q t1
   [{:data "111.22.3.4"}]
   [{:data (explode (mget
                      (reduce (fn [v t]
                                {"index" [(inc (mget v ["index" 0]))]
                                 "data" (conj (mget v "data") (str (mget v ["index" 0]) "-" t))})
                              {"index" [1] "data" (string-array)}
                              (split-str :data "\\."))
                      "data"))}]
   (group)
   (pivot (first (split-str :data "-")))
   (agg (first-a (second (split-str :data "-"))))
   (show false))

;;16-17 skipped  soundex and some regex








