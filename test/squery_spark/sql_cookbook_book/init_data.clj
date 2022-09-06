(ns squery-spark.sql-cookbook-book.init-data
  (:refer-clojure :only [])
  (:use squery-mongo-core.operators.operators
        squery-mongo-core.operators.qoperators
        squery-mongo-core.operators.uoperators
        squery-mongo-core.operators.stages
        squery-mongo-core.operators.options
        squery-mongo.driver.cursor
        squery-mongo.driver.document
        squery-mongo.driver.settings
        squery-mongo.driver.transactions
        squery-mongo.driver.utils
        squery-mongo.arguments
        squery-mongo.commands
        squery-mongo.macros
        flatland.ordered.map)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql SparkSession Dataset)
           (java.text SimpleDateFormat)
           (java.sql Date)
           (java.time Instant)
           (com.mongodb MongoClientSettings)
           (com.mongodb.client MongoClients)))

;;using squery-mongo



(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(def emp-docs [
               {:empno 7369 :ename "SMITH" :job  "CLERK" :mgr 7902
                :hiredate "17-12-2005" :sal 800 :comm nil :deptno 20 }
               {:empno 7499 :ename "ALLEN" :job  "SALESMAN" :mgr 7698
                :hiredate "20-02-2006" :sal 1600 :comm 300 :deptno 30 }
               {:empno 7521 :ename "WARD" :job  "SALESMAN" :mgr 7698
                :hiredate "22-02-2006" :sal 1250 :comm 500 :deptno 30 }
               {:empno 7566 :ename "JONES" :job  "MANAGER" :mgr 7839
                :hiredate "02-04-2006" :sal 2975 :comm nil :deptno 20 }
               {:empno 7654 :ename "MARTIN" :job  "SALESMAN" :mgr 7698
                :hiredate "28-09-2006" :sal 1250 :comm 1400 :deptno 30 }
               {:empno 7698 :ename "BLAKE" :job  "MANAGER" :mgr 7839
                :hiredate "01-05-2006" :sal 2850 :comm nil :deptno 30 }
               {:empno 7782 :ename "CLARK" :job  "MANAGER" :mgr 7839
                :hiredate "09-06-2006" :sal 2450 :comm nil :deptno 10 }
               {:empno 7788 :ename "SCOTT" :job  "ANALYST" :mgr 7566
                :hiredate "09-12-2007" :sal 3000 :comm nil :deptno 20 }
               {:empno 7839 :ename "KING" :job  "PRESIDENT" :mgr nil
                :hiredate "17-11-2006" :sal 5000 :comm nil :deptno 10 }
               {:empno 7844 :ename "TURNER" :job "SALESMAN"  :mgr 7698
                :hiredate "08-09-2006" :sal 1500 :comm 0 :deptno 30 }
               {:empno 7876 :ename "ADAMS" :job "CLERK"  :mgr 7788
                :hiredate "12-01-2008" :sal 1100 :comm nil :deptno 20 }
               {:empno 7900 :ename "JAMES" :job "CLERK"  :mgr 7698
                :hiredate "03-12-2006" :sal 950 :comm nil :deptno 30 }
               {:empno 7902 :ename "FORD" :job "ANALYST"  :mgr 7566
                :hiredate "03-12-2006" :sal 3000 :comm nil :deptno 20 }
               {:empno 7934 :ename "MILLER" :job "CLERK"  :mgr 7782
                :hiredate "23-01-2007" :sal 1300 :comm nil :deptno 10 }
             ])

(try (drop-collection :cookbook.emp) (catch Exception e ""))
(insert :cookbook.emp emp-docs)
(update- :cookbook.emp (uq {:hiredate (date-from-string :hiredate "%d-%m-%Y")}))

(def dept-docs [
               {:deptno 10 :dname "ACCOUNTING" :loc "NEW YORK"  }
                {:deptno 20 :dname "RESEARCH" :loc "DALLAS"  }
                {:deptno 30 :dname "SALES" :loc "CHICAGO"  }
                {:deptno 40 :dname "OPERATIONS" :loc "BOSTON"  }

               ])

(try (drop-collection :cookbook.dept) (catch Exception e ""))
(insert :cookbook.dept dept-docs)

(def bonus-docs [{:empno 7369 :received "14-03-2005" :type 1}
                 {:empno 7900 :received "14-03-2005" :type 2}
                 {:empno 7788 :received "14-03-2005" :type 3}])

(try (drop-collection :cookbook.bonus) (catch Exception e ""))
(insert :cookbook.bonus bonus-docs)
(update- :cookbook.bonus (uq {:received (date-from-string :received "%d-%m-%Y")}))

(def bonus1-docs [{:empno 7934 :received "17-03-2005" :type 1}
                   {:empno 7934 :received "15-02-2005" :type 2}
                   {:empno 7839 :received "15-02-2005" :type 3}
                   {:empno 7782 :received "15-02-2005" :type 1}])

(try (drop-collection :cookbook.bonus1) (catch Exception e ""))
(insert :cookbook.bonus1 bonus1-docs)
(update- :cookbook.bonus1 (uq {:received (date-from-string :received "%d-%m-%Y")}))

(def bonus2-docs [{:empno 7934 :received "17-03-2005" :type 1}
                   {:empno 7934 :received "15-02-2005" :type 2}])

(try (drop-collection :cookbook.bonus2) (catch Exception e ""))
(insert :cookbook.bonus2 bonus2-docs)
(update- :cookbook.bonus2 (uq {:received (date-from-string :received "%d-%m-%Y")}))

(def t1-docs [{:_id 1}])
(try (drop-collection :cookbook.t1) (catch Exception e ""))
(insert :cookbook.t1 t1-docs)


(def nulls [{:_id 1} {:_id nil} {:_id 2}])
(try (drop-collection :cookbook.nulls) (catch Exception e ""))
(insert :cookbook.nulls nulls)
