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

                 [org.flatland/ordered "1.5.9"]

                 ;[com.datastax.spark/spark-cassandra-connector_2.11 "2.4.0"]
                 ;[com.datastax.spark/spark-cassandra-connector-embedded_2.11 "2.4.0"]
                 ;[org.postgresql/postgresql "42.2.5"]
                 ]

  :aot [
        ;squery-spark.udftest
        ]

  :plugins [[lein-codox "0.10.7"]]
  ;:javac-options ["-source" "1.8" "-target" "1.8"]
  ;:jvm-opts ^:replace ["-server" "-Xmx2g"]
  :main squery-spark.udftest
  :global-vars {*warn-on-reflection* false})