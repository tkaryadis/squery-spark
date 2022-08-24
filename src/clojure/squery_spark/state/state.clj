(ns squery-spark.state.state)


;;random names for the udfs,the user dont use any of those,its for internal use
(def udf-names-counter (atom 0))

(defn new-udf-name []
  (let [n (swap! udf-names-counter inc)]
    (str "udf" n)))