(ns squery-spark.sql-cookbook-book.ch14
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
  (:import (org.apache.spark.sql.expressions Window)
           (org.apache.spark.sql functions Column RelationalGroupedDataset)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(def emp (q (load-collection spark :cookbook.emp) [:empno :ename :job :mgr :hiredate :sal :comm :deptno]))
(def dept (load-collection spark :cookbook.dept))
(def bonus (load-collection spark :cookbook.bonus))
(def bonus1 (load-collection spark :cookbook.bonus1))
(def bonus2 (load-collection spark :cookbook.bonus2))
(def t1 (load-collection spark :cookbook.t1))
(def cnt (load-collection spark :cookbook.cnt))
(def cnt1 (load-collection spark :cookbook.cnt1))


;;1 assumin that i dont know how many deptno i have and i need to get them from dept
(q (as dept :d)
   (select-distinct :deptno)
   (join (as emp :e)  (= :e.deptno :d.deptno) :left_outer)
   (group :d.deptno
          {:cnt-emps (sum (if- (nil? :e.empno) 0 1))})
   (group)
   (pivot :deptno)
   (agg (first-acc :cnt-emps))
   show)

;;14- 2 un-pivot, explode map, column+value as pair
(q t1
   [{:accounting 3}
    {:research 5}
    {:sales 6}
    {:operations 0}]
   [{[:dname :cnt] (explode {"accounting" :accounting
                             "research" :research
                             "sales" :sales
                             "operations" :operations})}]
   show)

;;14- 2 un-pivot, explode array
(q t1
   [{:accounting 3}
    {:research 5}
    {:sales 6}
    {:operations 0}]
   [{:dname-cnt
     (explode [["acounting" :accounting]
               ["research" :research]
               ["sales" :sales]
               ["operations" :operations]])}]
   [{:dname (get :dname-cnt 0)}
    {:cnt (get :dname-cnt 1)}]
   show)

;;3
(q emp
   (group :deptno
          {:cnt (count-acc)})
   [{:deptno-cnt (str "D" :deptno "-" :cnt)}]
   (group)
   (pivot (first (split-str :deptno-cnt "-")))
   (agg (first-acc (second (split-str :deptno-cnt "-"))))
   show)

;;4 TODO when regex_extract_all is added,
#_(q t1
   [{:data (explode
             ["xxxxxabc[867]xxx[-]xxxx[5309]xxxxx"
              "xxxxxtime:[11271978]favnum:[4]id:[Joe]xxxxx"
              "call:[F_GET_ROWS()]b1:[ROSEWOOD…SIR]b2:[44400002]77.90xxxxx"
              "film:[non_marked]qq:[unit]tailpipe:[withabanana?]80sxxxxx"])}]
   {:parts (re-find "\\[[^]]+\\]" :data)}
   show)

;;5

