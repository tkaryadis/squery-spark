(defproject org.squery/squery-spark "0.1.0-SNAPSHOT"
  :description "Clojure library for apache spark."
  :url "https://github.com/tkaryadis/squery-spark"
  :license {:name "Eclipse Public License" 
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [                                           ;[org.clojure/clojure "1.12.0-rc1"]

                 [org.clojure/clojure "1.12.0-beta2"]

                 ;[com.wjoel/clj-bean "0.2.1"]

                 [org.apache.spark/spark-core_2.12 "3.3.0"]
                 [org.apache.spark/spark-sql_2.12 "3.3.0"]
                 [org.apache.spark/spark-mllib_2.12 "3.3.0"]
                 [graphframes/graphframes "0.8.1-spark3.0-s_2.12"]

                 ;;? need this?
                 [com.fasterxml.jackson.module/jackson-module-scala_2.12 "2.13.3"]

                 ;;mongodb-connector 10.0.4 works only with scala 2.12
                 [org.mongodb.spark/mongo-spark-connector "10.0.4"
                  :exclusions [org.mongodb/mongodb-driver-sync]]

                 ;;query mongo with squery for mongo
                 [org.squery/squery-mongo-core "0.2.0-SNAPSHOT"]
                 [org.squery/squery-mongoj "0.2.0-SNAPSHOT"]

                 [io.github.erp12/fijit "1.0.8"]
                 [org.flatland/ordered "1.5.9"]
                 [org.apache.commons/commons-math3 "3.6.1"]


                 ]
  ;;aot, main,gen-class, delete target, and run with leinengen (sources that use udf+rdds)
  :aot [
        ;squery-spark.rdds.e01create-and-various
        ;squery-spark.rdds.e04pairrdd
        ;squery-spark.rdds.e02map-filter-take
        ;squery-spark.sources-sinks.files
        ;squery-spark.rdds.e04pairrdd
        squery-spark.rdds.e03groupreduce
        ]

  :repositories [["graph" {:url "https://repos.spark-packages.org" :checksum :ignore}]]

  :plugins [[lein-codox "0.10.7"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  ;:main squery-spark.spark-definitive-book.ch12
  ;squery-spark.udaf_test
  ;squery-spark.spark-definitive-book.ch13
  ;squery-spark.udf-test
  :global-vars {*warn-on-reflection* false})

