(ns squery-spark.utils.functional-interfaces
  (:import (org.apache.spark.api.java.function FlatMapFunction PairFunction Function Function2 VoidFunction)
           (org.apache.spark.api.java JavaRDD JavaPairRDD)
           (scala Tuple2)))

(defmacro f
  ([vec-args body]
   `(reify
      org.apache.spark.api.java.function.Function
      (call [this# arg#]
        ((fn ~vec-args ~body) arg#))))
  ([f]
   `(reify
      org.apache.spark.api.java.function.Function
      (call [this# arg#]
        (~f arg#)))))