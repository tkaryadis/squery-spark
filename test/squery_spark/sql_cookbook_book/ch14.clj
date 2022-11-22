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
            [squery-spark.mongo-connector.utils :refer [load-collection]]
            [squery-spark.utils.utils :refer :all])
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
(def it-research (load-collection spark :cookbook.it-research))
(def it-apps (load-collection spark :cookbook.it-apps))
(def parse-strings (load-collection spark :cookbook.parse-strings))


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
(q t1
   {:diff (days-diff (add-months (ISODate "2020-01-01") 12)
                     (ISODate "2020-01-01"))}
   show)

;;6
(q t1
   [{:strings (explode
                ["1010 switch"
                 "333"
                 "3453430278"
                 "ClassSummary"
                 "findRow 55"
                 "threes"])}]
   ((re-find? "\\d+.*[a-zA-Z]+|[a-zA-Z]+.*\\d+" :strings))
   show)

;;14.7 skipped

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

;;14.9
(q it-research
   (unset :_id)
   {:info (window (row-number) (-> (ws-group :deptno)
                                   (ws-sort :deptno)))}
   (union-with (q it-research
                  (select-distinct :deptno)
                  [:deptno {:ename ""} {:info 0}]))
   (sort :deptno)
   [{:research (if- (= :ename "") :deptno (str :ename "  "))}
    {:rown1 (window (row-number) (ws-sort :deptno :ename))}]
   (join (q it-apps
            (unset :_id)
            {:info (window (row-number) (-> (ws-group :deptno)
                                            (ws-sort :deptno)))}
            (union-with (q it-apps
                           (select-distinct :deptno)
                           [:deptno {:ename ""} {:info 0}]))
            (sort :deptno)
            [{:apps (if- (= :ename "") :deptno (str :ename "  "))}
             {:rown2 (window (row-number) (ws-sort :deptno :ename))}])
         (= :rown1 :rown2)
         :fullouter)
   [:research :apps]
   show)

;;14.10 skipped oracle related?

;;14.11
(q parse-strings
   [{:parts (split-str :strings ":")}]
   [{:val1 (get :parts 1)}
    {:val2 (get :parts 2)}
    {:val3 (get :parts 3)}]
   show)

;;14.12
(q emp
   (group :job
          {:n-emps (count-acc)}
          {:job-sal (sum :sal)})
   {:all-salaries (window (sum :job-sal))}
   [:job :n-emps {:perc (long (* (div :job-sal :all-salaries) 100))}]
   show)