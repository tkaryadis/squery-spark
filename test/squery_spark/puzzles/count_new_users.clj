(ns squery-spark.puzzles.count-new-users
  (:refer-clojure :only [])
  (:require [squery-spark.datasets.queries :refer :all]
            [squery-spark.state.connection :refer [get-spark-session get-spark-context]]
            [squery-spark.datasets.stages :refer :all]
            [squery-spark.datasets.operators :refer :all]
            [squery-spark.datasets.schema :refer :all]
            [squery-spark.datasets.rows :refer :all]
            [squery-spark.datasets.utils :refer :all]
            [squery-spark.datasets.udf :refer :all]
            [squery-spark.utils.utils :refer :all]
            [squery-spark.mongo-connector.utils :refer [load-collection]])
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (org.apache.spark.sql.expressions Window)
           (org.apache.spark.sql functions Column RelationalGroupedDataset)))

(def spark (get-spark-session))
(.setLogLevel (get-spark-context spark) "ERROR")

(def table (seq->df spark
                    [[(ISODate "2022-02-20") 1 "abc"]
                     [(ISODate "2022-02-20") 2 "xyz"]
                     [(ISODate "2022-02-22") 1 "xyz"]
                     [(ISODate "2022-02-22") 3 "klm"]
                     [(ISODate "2022-02-24") 1 "abc"]
                     [(ISODate "2022-02-24") 2 "abc"]
                     [(ISODate "2022-02-24") 3 "abc"]]
                    (build-schema [[:date :date]
                                   [:userid :long]
                                   [:activity :string]])))

;;expected result = find each new user per day (if user was old dont count him)
;;2022-02-20  2
;;2022-2-22   1
;;2022-2-24   0

(.show table)

(q table
   {:first-date (window (first-acc :date) (-> (ws-group :userid)
                                              (ws-sort :date)))}
   {:counter (if- (= :date :first-date) 1 0)}
   (group :date
          {:counter (sum :counter)})
   show)