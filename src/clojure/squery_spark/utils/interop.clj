(ns squery-spark.utils.interop
  (:require [erp12.fijit.collection :refer [to-scala-seq]])
  (:import (scala.jdk.javaapi CollectionConverters)
           (java.util ArrayList List)
           (scala Function0 Function1 Function2 Function3)))


;;moved them to udf

(comment
(defn ->scala-function0 [f]
  (reify Function0 (apply [_] (f))))

(defn ->scala-function1 [f]
  (reify Function1 (apply [_ x] (f x))))

(defn ->scala-function2 [f]
  (reify Function2 (apply [_ x y] (f x y))))

(defn ->scala-function3 [f]
  (reify Function3 (apply [_ x y z] (f x y z))))
)