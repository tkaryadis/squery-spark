(ns squery-spark.datasets.query
  (:require [squery-spark.datasets.internal.query :refer [pipeline]]
            [squery-spark.datasets.operators]))

;;q
;; 1)translates to ->
;; 2)produces spark code,for some qforms
;; 3)louna functions for other qforms that will produce spark code on runtime
(defmacro q [qform & qforms]
  (let [qforms (pipeline qforms)
        ;_ (prn "qforms" qforms)
        query (concat (list '->) (list qform) qforms)
        ;- (prn "query" query)
        ]
    `(let ~squery-spark.datasets.operators/operators-mappings
       ~query)))