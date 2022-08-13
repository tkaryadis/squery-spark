(ns squery-spark.interop.interop
  (:import (scala Function0 Function1 Function2 Function3)))

(defn ->scala-function0 [f]
      (reify Function0 (apply [_] (f))))

(defn ->scala-function1 [f]
      (reify Function1 (apply [_ x] (f x))))

(defn ->scala-function2 [f]
      (reify Function2 (apply [_ x y] (f x y))))

(defn ->scala-function3 [f]
      (reify Function3 (apply [_ x y z] (f x y z))))

;(def f (->scala-function0 (fn [] (println "xxx"))))

