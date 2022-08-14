(ns squery-spark.datasets.internal.common
  (:require [squery-spark.utils.utils :refer [keyword-map]]
            clojure.set)
  (:import [org.apache.spark.sql functions Column]))

;;the first 2 functions is to use
;;  keyword instead(col ...)
;;  simple literals instead (lit ...)
;;the first replaces only keywords
;;the second replaces keywords and numbers,strings to (lit  )

(defn column
  " converts always to column
    if keyword => column, else lit, but always column result
    and if {k v} converts the v to col,lit (if keyword,string,number)
    field keyword => col
    field map {k v} => (.as v (name k))
      v is also converted
      if keyword => col
      if not column => lit
      else v (no change)"
  [field]
  (cond

    (vector? field)                                         ;;TODO, just lit or array?
    ;(functions/lit (into-array field))
    #_(functions/array (into-array Column field))
    (functions/array (into-array Column (mapv column field)))

    (keyword? field)
    (functions/col (name field))

    (map? field)
    (let [meta (get field :meta)
          field (dissoc field :meta)
          k (name (first (keys field)))
          v (first (vals field))
          v (column v)]
      (if meta
        (.as v k meta)
        (.as v k)))

    (not (instance? org.apache.spark.sql.Column field))
    (functions/lit field)

    :else
    field))

(defn column-keyword
  "keyword to column
   if map does this inside also"
  [field]
  (cond

    (vector? field)
    ;(functions/lit (into-array field))
    (functions/array (into-array Column (mapv column field)))

    (keyword? field)
    (functions/col (name field))

    (map? field)
    (let [meta (get field :meta)
          field (dissoc field :meta)
          k (name (first (keys field)))
          v (first (vals field))
          v (column-keyword v)]
      (if meta
        (.as v k meta)
        (.as v k)))

    :else
    field))

(defn columns [fields]
  (mapv column fields))

(defn columns-keyword [fields]
  (mapv column-keyword fields))


(defn single-maps
  "Makes all map members to have max 1 pair,and key to be keyword(if not starts with $) on those single maps.
   [{:a 1 :b 2} 20 {'c' 3} [1 2 3]] => [{:a 1} {:b 2} 20 {:c 3} [1 2 3]]
   It is used from read-write/project/add-fields
   In commands its needed ONLY when i want to seperate command options from extra command args.
   (if i only need command to have keywords i use command-keywords function)"
  ([ms keys-to-seperate]
   (loop [ms ms
          m  {}
          single-ms []]
     (if (and (empty? ms)
              (or (nil? m)                                  ; last element was not a map
                  (and (map? m) (empty? m))))               ; last element was a map that emptied
       single-ms
       (cond

         (not (map? m))
         (recur (rest ms) (first ms) (conj single-ms m))

         (empty? m)
         (recur (rest ms) (first ms) single-ms)

         ; if keys-to-seperate
         ;   and map doesnt have any key that needs seperation,keep it as is
         (and (not (empty? keys-to-seperate))
              (empty? (clojure.set/intersection (set (map (fn [k]
                                                            (if (string? k)
                                                              (keyword k)
                                                              k))
                                                          (keys m)))
                                                keys-to-seperate)))
         (recur (rest ms) (first ms) (conj single-ms (keyword-map m)))

         :else
         (let [[k v] (first m)]
           (recur ms (dissoc m k) (conj single-ms (keyword-map {k v}))))))))
  ([ms]
   (single-maps ms #{})))

(defn string-keys-column-values [m]
  (reduce (fn [new-m k]
            (assoc new-m (name k) (column (get m k))))
          {}
          (keys m)))
