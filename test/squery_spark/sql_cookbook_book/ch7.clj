(ns squery-spark.sql-cookbook-book.ch7
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

;;it uses one udaf aot+run with lein

(defn -main [])

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(def emp (q (load-collection spark :cookbook.emp) [:empno :ename :job :mgr :hiredate :sal :comm :deptno]))
(def dept (load-collection spark :cookbook.dept))
(def bonus (load-collection spark :cookbook.bonus))
(def bonus1 (load-collection spark :cookbook.bonus1))
(def bonus2 (load-collection spark :cookbook.bonus2))
(def t1 (load-collection spark :cookbook.t1))

;;1
(q emp
   (group {:avg-sal (avg :sal)})
   show)

(q emp
   (group :deptno
          {:avg-sal (avg :sal)})
   show)

;;2
(q emp
   (group {:max-sal (max :sal)}
          {:min-sal (min :sal)})
   show)

(q emp
   (group :deptno
          {:max-sal (max :sal)}
          {:min-sal (min :sal)})
   show)

;;3
(q emp (group {:sum-sal (sum :sal)}) show)

;;4
(q emp (group {:nemp (count-a)}) show)
;;=
(q emp (group {:nemp (sum 1)}) show)
;;=
(prn (q emp (count-s)))

(q emp (group :deptno {:nemp (count-a)}) show)

;;5
(q emp (group {:comm (count-a :comm)}) show)

;;6
(q emp
   {:running-total (wfield (sum :sal) (wsort :sal :empno))}
   show)


;;7  udaf
(defudaf spark mul-acc (ProductAcc.) (Encoders/LONG))

(q emp
   ((= :deptno 10))
   {:running-prod (wfield (mul-acc :sal) (wsort :sal :empno))}
   show)

;;8 skipped

;;9
(q emp
   ((= :deptno 20))
   (group :sal
          {:count (count-a)})
   (sort :!count)
   (limit 1)
   [:sal]
   show)




