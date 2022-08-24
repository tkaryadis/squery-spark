(defproject org.squery/squery-spark "0.1.0-SNAPSHOT"
  :description "Clojure library for apache spark."
  :url "https://github.com/tkaryadis/squery-spark"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 ;[com.wjoel/clj-bean "0.2.1"]

                 [org.apache.spark/spark-core_2.13 "3.2.0"]
                 [org.apache.spark/spark-sql_2.13 "3.2.0"]
                 [org.apache.spark/spark-mllib_2.13 "3.2.0"]
                 [io.delta/delta-core_2.13 "2.0.0"]

                 [io.github.erp12/fijit "1.0.8"]
                 [org.flatland/ordered "1.5.9"]

                 ]
  ;;aot, main,gen-class, delete target, and run with leinengen (sources that use udf+rdd)
  :aot [
        squery-spark.spark-definitive-book.ch13
        ;squery-spark.spark-definitive-book.ch12
        ;squery-spark.udftest
        ]

  :plugins [[lein-codox "0.10.7"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :main   squery-spark.spark-definitive-book.ch13                ;squery-spark.udftest
  :global-vars {*warn-on-reflection* false})

