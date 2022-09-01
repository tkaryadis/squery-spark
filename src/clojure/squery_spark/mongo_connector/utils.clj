(ns squery-spark.mongo-connector.utils)

(defn load-collection [spark db-namespace]
  (let [[db collection] (clojure.string/split (name db-namespace) #"\.")
        df (-> spark .read (.format "mongodb") (.option "database" db) (.option "collection" collection) .load)]
    df))