(ns squery-spark.sql-cookbook-book.pivot
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


;;TODO see stack and unpivot for spark, also unpivot with {[:a :b] (explode {k1 v1 k2 v2})}


;;pivot makes each distinct group value into a column
;;and then i run the accumulator for the initial group vars + pivot
;; i can think it as group in each pivot column

;;ch12-2     array way, general without knowing possible job types
(q emp
   [:job :ename]
   (group)
   (pivot :job)
   (agg (conj-each :ename))
   (show false))



;;ch6 - 15  (solved in general case with uknown parts, with pivot)
;; pivot with expression
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
   (agg (first-acc (second (split-str :data "-"))))
   (show false))


;;ch12 1
;;pivot GENERAL without knowing the possible departments
(q emp
   (group :deptno {:cnt (sum 1)})
   {:deptno (str "deptno_" :deptno)}
   (group)
   (pivot :deptno)
   (agg (first-acc :cnt))
   show)

;;ch12 1 manual alt
;;pivot manual, requires to know the possible departments
(q emp
   {:deptno_10 (if- (= :deptno 10) 1 0)
    :deptno_20 (if- (= :deptno 20) 1 0)
    :deptno_30 (if- (= :deptno 30) 1 0)}
   [:deptno_10 :deptno_20 :deptno_30]
   (group {:deptno_10 (sum :deptno_10)}
          {:deptno_20 (sum :deptno_20)}
          {:deptno_30 (sum :deptno_30)})
   show)

;;14-1 assumin that i dont know how many deptno i have and i need to get them from dept
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

;;14-3
(q emp
   (group :deptno
          {:cnt (count-acc)})
   [{:deptno-cnt (str "D" :deptno "-" :cnt)}]
   (group)
   (pivot (first (split-str :deptno-cnt "-")))
   (agg (first-acc (second (split-str :deptno-cnt "-"))))
   show)



;;14.8
;; array way, ok if they fit in array
(q emp
   {:drank (window (dense-rank) (ws-sort :!sal))}
   [:ename :sal {:type (cond (< :drank 4) "top3"
                             (< :drank 7) "rest3"
                             :else "rest")}]
   (group)
   (pivot :type)
   (agg (conj-each [:ename :sal]))
   (show false))

;;14.8 with self-joins
(q emp
   {:drank (window (dense-rank) (ws-sort :!sal))}
   [{:rest (if- (< :drank 7) [] [:ename :sal])} :drank]
   [:rest {:nrows1 (window (row-number) (ws-sort (desc (long (second :rest)))))}]
   (join (q emp
            {:drank (window (dense-rank) (ws-sort :!sal))}
            [{:top3 (if- (< :drank 4) [:ename :sal] [])} :drank]
            [:top3 {:nrows2 (window (row-number) (ws-sort (desc (long (second :top3)))))}])
         (= :nrows1 :nrows2))
   (join (q emp
            {:drank (window (dense-rank) (ws-sort :!sal))}
            [{:next3 (if- (and (>= :drank 4) (< :drank 7)) [:ename :sal] [])} :drank]
            [:next3 {:nrows3 (window (row-number) (ws-sort (desc (long (second :next3)))))}])
         (= :nrows1 :nrows3))
   [:top3 :next3 :rest]
   show)