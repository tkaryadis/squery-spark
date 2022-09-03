(ns squery-spark.datasets.udf
  (:require [squery-spark.state.state]
            [squery-spark.datasets.operators])
  (:import (org.apache.spark.sql.api.java UDF1 UDF0 UDF2 UDF3 UDF4)
           [org.apache.spark.sql functions Column Encoders]
           (scala Function0 Function1 Function2 Function3)))

;;---------UDF---------------------------------

(defrecord UF0 [f]
  UDF0
  (call [this]
    (f)))

(defrecord UF1 [f]
  UDF1
  (call [this x]
    (f x)))

(defrecord UF2 [f]
  UDF2
  (call [this x y]
    (f x y)))

(defrecord UF3 [f]
  UDF3
  (call [this x y z]
    (f x y z)))

(defrecord UF4 [f]
  UDF4
  (call [this x y z w]
    (f x y z w)))


(defn get-udf-class [f nargs]
  (cond
    (= nargs 0)
    (UF0. f)

    (= nargs 1)
    (UF1. f)

    (= nargs 2)
    (UF2. f)

    (= nargs 3)
    (UF3. f)

    (= nargs 4)
    (UF4. f)

    :else
    (do (prn "unknown arity") (System/exit 0))))

(defn call-udf [name & col-names]
  (functions/callUDF name (into-array Column (squery-spark.datasets.internal.common/columns col-names))))

(defn register-udf [spark f nargs rtype]
  (let [rtype (if (keyword? rtype)
                (get  squery-spark.datasets.schema/schema-types rtype)
                rtype)
        new-udf (get-udf-class f nargs)
        name (squery-spark.state.state/new-udf-name)
        _ (->  spark
               (.udf)
               (.register name new-udf rtype))]
    (partial call-udf name)))

(defmacro defudf [spark fname arg2 arg3 arg4]
  (if (vector? arg3)
    ;;define "spark" function
    (let [rtype arg2
          args arg3
          body arg4]
      `(def ~fname (register-udf ~spark (fn ~args ~body) ~(count args) ~rtype)))
    ;;use already defined clojure function
    (let [f arg2
          nargs arg3
          rtype arg4]
      `(def ~fname (register-udf ~spark ~f ~nargs ~rtype)))))

;;spark.udf().register("myAverage", functions.udaf(new MyAverage(), Encoders.LONG()));

(defn register-udaf [spark udaf-object encoder]
  (let [name (squery-spark.state.state/new-udf-name)
        _ (-> spark
              .udf
              (.register name
                         (functions/udaf udaf-object encoder)))]
    (partial squery-spark.datasets.udf/call-udf name)))

(defmacro defudaf [spark fname udaf-object encoder]
  `(def ~fname (register-udaf ~spark ~udaf-object ~encoder)))

;;----------------------------------scala functions--------------------------------------------------------------------

(defn ->scala-function0 [f]
  (reify Function0 (apply [_] (f))))

(defmacro defnscala0 [name f]
  `(let ~squery-spark.datasets.operators/operators-mappings
     (def ~(symbol name) (squery-spark.datasets.udf/->scala-function0 ~f))))

(defn ->scala-function1 [f]
  (reify Function1 (apply [_ x] (f x))))

(defmacro defnscala1 [name f]
  `(let ~squery-spark.datasets.operators/operators-mappings
     (def ~(symbol name) (squery-spark.datasets.udf/->scala-function1 ~f))))

(defn ->scala-function2 [f]
  (reify Function2 (apply [_ x y] (f x y))))

(defmacro defnscala2 [name f]
  `(let ~squery-spark.datasets.operators/operators-mappings
     (def ~(symbol name) (squery-spark.datasets.udf/->scala-function2 ~f))))

(defn ->scala-function3 [f]
  (reify Function3 (apply [_ x y z] (f x y z))))

(defmacro defnscala3 [name f]
  `(let ~squery-spark.datasets.operators/operators-mappings
     (def ~(symbol name) (squery-spark.datasets.udf/->scala-function3 ~f))))

#_(-> (class myf1)
      (print-str)
      (demunge)
      (symbol)
      (find-var)
      (meta)
      (prn))

;(prn (meta #'myf1))

;;  (let [f (fn [x y z] (println x y z))]
;    (require 'clojure.reflect)
;    (->> (clojure.reflect/reflect f)
;         :members
;         (filter #(instance? clojure.reflect.Method %))
;         first
;         :parameter-types
;         count))
;
;


;  (let [f (fn
;            ([x] (println x))
;            ([x y] (println x y))
;            ([x y z] (println x y z)))]
;    (require 'clojure.reflect)
;    (->> (clojure.reflect/reflect f)
;         :members
;         (filter #(instance? clojure.reflect.Method %))
;         (map (comp count :parameter-types))))