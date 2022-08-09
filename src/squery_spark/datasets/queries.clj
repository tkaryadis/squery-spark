(ns squery-spark.datasets.queries
  (:require [squery-spark.datasets.internal.query :refer [pipeline]]
            [squery-spark.datasets.operators]
            [squery-spark.utils.utils :refer [string-map]]
            [squery-spark.datasets.internal.common :refer [column string-keys-column-values]]))

;;q
;; 1)translates to ->
;; 2)macro is used to convert [] to project, {} to add fields, () to fitlers etc
;;   and to create the enviroment overriding some of clojure.core, only inside the q scope
;;   to access clojure.core inside the q scope use alias like c/str
;; 3)functions is used for everything else that can run on runtime
(defmacro q [qform & qforms]
  (let [qforms (pipeline qforms)
        ;_ (prn "qforms" qforms)
        query (concat (list '->) (list qform) qforms)
        ;- (prn "query" query)
        ]
    `(let ~squery-spark.datasets.operators/operators-mappings
       ~query)))

(defmacro sq [arg]
  `(let ~squery-spark.datasets.operators/operators-mappings
     ~arg))

(defmacro sq-> [& args]
  `(let ~squery-spark.datasets.operators/operators-mappings
     (-> ~@args)))

(defmacro not-sq [arg]
  `(let ~squery-spark.datasets.operators/core-operators-mappings
     ~arg))
