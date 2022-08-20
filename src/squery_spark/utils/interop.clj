(ns squery-spark.utils.interop
  (:require [erp12.fijit.collection :refer [to-scala-seq]])
  (:import (scala.jdk.javaapi CollectionConverters)
           (java.util ArrayList List)))


#_(defn clj->scala1 [seq]
  (let [scala-buf (ListBuffer.)]
    (doall (map #(.$plus$eq scala-buf %) (flatten [seq])))
    scala-buf))

;;scala.collection.JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
#_(defn clj->scala [coll]
  (-> coll JavaConversions/asScalaBuffer .toList))

;;CollectionConverters.asScala(javaList).toSeq();

(defn clj->scala1 [seq]
  (.toSeq (CollectionConverters/asScala ^List (ArrayList. seq))))
