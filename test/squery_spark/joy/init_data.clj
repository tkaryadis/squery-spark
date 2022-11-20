(ns squery-spark.joy.init-data
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
  (:require [clojure.core :as c]
            [clojure.pprint :refer [pprint]])
  (:import (org.apache.spark.sql SparkSession Dataset)
           (java.text SimpleDateFormat)
           (java.sql Date)
           (java.time Instant)
           (com.mongodb MongoClientSettings)
           (com.mongodb.client MongoClients)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :joy.takis-vertices) (catch Exception e ""))
(try (drop-collection :joy.takis-edges) (catch Exception e ""))

(create-view :joy.users :takis-vertices
             (= :userid 224283837859889152)
             [:songs]
             (unwind :songs)
             (lookup :songs.songid :songs.songid :results)
             (replace-root (first :results))
             {:id :songid}
             (unset :_id :songid)
             [:id {:song (if- :spotify-song :spotify-song :song)} :link])

(create-view :joy.users :takis-edges
             (= :userid 224283837859889152)
             [:songs]
             (unwind :songs)
             (replace-root :songs)
             (> :points 4))