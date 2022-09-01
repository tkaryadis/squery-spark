(ns squery-spark.sql-cookbook-book.ch3
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.mongo-connector.utils :refer [load-collection]])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql functions Column)
           (org.apache.spark.sql.expressions Window WindowSpec)))

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
   ((= :deptno 10))
   [:ename :deptno]
   (union-with (q t1 ["----------" nil]))
   (union-with (q dept [:dname :deptno]))
   [{:ename_dname :ename} :deptno]
   show)

;;2
(q emp
   ((= :deptno 10))
   (join-eq dept :deptno)
   [:ename :loc]
   show)

;;3
(q (as emp :e1)
   ((= :job "CLERK"))
   [:ename :job :sal]
   (join (as emp :e2)  (and (= :e1.job :e2.job)
                            (= :e1.ename :e2.ename)
                            (= :e1.sal :e2.sal)))
   [:e2.empno :e2.ename :e2.job :e2.sal :e2.deptno]
   show)

;;4
(q dept
   [:deptno]
   (difference-with (q emp [:deptno]))
   show)

;;5
(q (as dept :d)
   (join (as emp :e)
         (= :d.deptno :e.deptno)
         :left_anti)
   show)

;;6
(q (as emp :e)
   (join-eq dept :deptno)
   (join (as bonus :b)
         (= :e.empno :b.empno)
         :left_outer)
   [:ename :loc :received]
   (sort :loc)
   show)

(q emp
   ((not= :deptno 10))
   (union-all-with (q emp ((= :ename "WARD"))))
   {:row-number (wfield (row-number) (-> (wgroup :empno) (wsort :empno)))}
   show)

;;7 keeping duplicates in set operations, using 1 extra column with the row number inside the group (to make them distinct)
(q emp
   ((not= :deptno 10))
   (union-all-with (q emp ((= :ename "WARD"))))
   {:row-number (wfield (row-number) (-> (wgroup :empno) (wsort :empno)))}
   (difference-with (q emp {:row-number (wfield (row-number) (-> (wgroup :empno) (wsort :empno)))}))
   (unset :row-number)
   (union-with (q emp ((= :deptno 10))))
   show)

;;8
(q dept
   ((= :deptno 10))
   (join-eq emp :deptno)
   [:ename :loc]
   show)

;;9
(q (as emp :e)
   ((= :deptno 10))
   (join (q (as bonus1 :b)
            (group :empno
                   {:bonus-perc (sum (div :type 10))}))
         (= :e.empno :b.empno)
         :left_outer)
   (group nil
          {:deptno (first-a :deptno)}
          {:total-sal (sum :sal)}
          {:total-bonus (sum (* :sal :bonus-perc))})
   show)


;;10
(q (as emp :e)
   ((= :deptno 10))
   (join (q (as bonus2 :b)
            (group :empno
                   {:bonus-perc (sum (div :type 10))}))
         (= :e.empno :b.empno)
         :left_outer)
   (group nil
          {:deptno (first-a :deptno)}
          {:total-sal (sum :sal)}
          {:total-bonus (sum (* :sal :bonus-perc))})
   show)

;;11
(q (as emp :e)
   (union-with (q t1 [1111 "YODA" "JEDI" nil nil nil nil nil]))
   (join (as dept :d)
         (= :e.deptno :d.deptno)
         :fullouter)
   ;[:d.deptno :d.dname :e.ename]
   show)


;;12
(q emp
   (join (q emp
            ((= :ename "WARD"))
            [{:ward-comm :comm}])
         true)
   ((< (if- (nil? :comm) 0 :comm)  :ward-comm))
   show)








