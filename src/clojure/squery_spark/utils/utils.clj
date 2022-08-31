(ns squery-spark.utils.utils
  (:require clojure.repl
            flatland.ordered.map
            [clojure.java.io :as io]))

(defn ordered-map
  ([] (flatland.ordered.map/ordered-map))
  ([other-map] (into (flatland.ordered.map/ordered-map) other-map))
  ([k1 v1 & keyvals] (apply flatland.ordered.map/ordered-map (cons k1 (cons v1 keyvals)))))

(defn keyword-map
  "Makes string keys that dont start with $,to keywords ($ keys are MQL operators)"
  [m]
  (if (map? m)
    (reduce (fn [m-k k]
              (assoc m-k (if (and (string? k) (not (clojure.string/starts-with? k "$")))
                           (keyword k)
                           k)
                         (get m k)))
            {}
            (keys m))
    m))


(defn string-map
  "Makes keyword keys to strings"
  [m]
  (if (map? m)
    (reduce (fn [m-k k]
              (assoc m-k (if (keyword? k)
                           (name k)
                           k)
                         (get m k)))
            {}
            (keys m))
    m))

(defn nested2 [f args]
  (let [first-value (f (first args) (second args))
        args (rest (rest args))]
    (loop [args args
           nested-f first-value]
      (if (empty? args)
        nested-f
        (recur (rest args) (f (first args) nested-f))))))


(defn nested3 [f args]
  (let [first-value (f (first args) (second args) (nth args 2))
        args (rest (rest (rest args)))]
    (loop [args args
           nested-f first-value]
      (if (empty? args)
        nested-f
        (recur (rest (rest args)) (f (first args) (second args) nested-f))))))

(defn delete-directory-recursive
  "Recursively delete a directory."
  [^java.io.File file]
  (when (.isDirectory file)
    (run! delete-directory-recursive (.listFiles file)))
  (io/delete-file file))

