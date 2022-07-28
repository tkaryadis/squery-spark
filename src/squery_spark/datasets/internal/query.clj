(ns squery-spark.datasets.internal.query
  (:require [squery-spark.datasets.internal.common :refer [single-maps]]
            [squery-spark.utils.utils :refer [ordered-map]]))

(defn pipeline
  "Converts a csql-pipeline to a mongo pipeline (1 vector with members stage operators)
   [],{}../nil  => empty stages or nil stages are removed
   [[] []] => [] []   flatten of stages (used when one stage produces more than 1 stages)
   cmql-filters combined =>  $match stage with $and
   [] projects  => $project"
  [csql-pipeline]
  (loop [csql-pipeline csql-pipeline
         filters []
         sql-pipeline []]
    (if (empty? csql-pipeline)
      (if (empty? filters)
        sql-pipeline
        sql-pipeline
        #_(conj sql-pipeline (cmql-filters->match-stage filters)))
      (let [stage (first csql-pipeline)]
        (cond

          (or (= stage []) (nil? stage))                    ; ignore [] or nil stages
          (recur (rest csql-pipeline) filters sql-pipeline)

          (map? stage)                                      ; {:a ".." :!b ".."}
          (let [stage `(squery-spark.datasets.stages/add-columns ~stage)]
            (if (vector? stage)                             ; 1 project stage might produce nested stages,put the nested and recur
              (recur (concat stage (rest csql-pipeline)) filters sql-pipeline) ; do what is done for nested stages(see below)
              (if (empty? filters)
                (recur (rest csql-pipeline) [] (conj sql-pipeline stage))
                (recur (rest csql-pipeline) [] (conj sql-pipeline
                                                     ;(cmql-filters->match-stage filters)
                                                     stage)))))

          (vector? stage)                                   ; [:a ....]
          (let [stage `(squery-spark.datasets.stages/select ~@stage)]
            (if (vector? stage)                             ; 1 project stage might produce nested stages,put the nested and recur
              (recur (concat stage (rest csql-pipeline)) filters sql-pipeline) ; do what is done for nested stages(see below)
              (if (empty? filters)
                (recur (rest csql-pipeline) [] (conj sql-pipeline stage))
                (recur (rest csql-pipeline) [] (conj sql-pipeline
                                                     ;(identity filters)
                                                     ;(cmql-filters->match-stage filters)
                                                     stage)))))
          (and (list? stage) (list? (first stage)))
          (let [stage (into [] stage)
                stage `(squery-spark.datasets.stages/filter-columns ~stage)]
            (if (vector? stage)                             ; 1 project stage might produce nested stages,put the nested and recur
              (recur (concat stage (rest csql-pipeline)) filters sql-pipeline) ; do what is done for nested stages(see below)
              (if (empty? filters)
                (recur (rest csql-pipeline) [] (conj sql-pipeline stage))
                (recur (rest csql-pipeline) [] (conj sql-pipeline
                                                     ;(identity filters)
                                                     ;(cmql-filters->match-stage filters)
                                                     stage)))))

          ;(vector? stage)      ; vector but no project = nested stage,add the members as stages and recur     ; TODO: REVIEW:
          ;(recur (concat stage (rest csql-pipeline)) filters sql-pipeline)

          false                                             ;(stage-operator? stage)                                    ; normal stage operator {}
          (if true                                          ;(empty? filters)
            (recur (rest csql-pipeline) [] (conj sql-pipeline stage))
            1
            #_(recur (rest csql-pipeline) [] (conj sql-pipeline (cmql-filters->match-stage filters) stage)))

          :else                                             ; filter stage (not qfilter,they are collected above)
          (recur (rest csql-pipeline) [] (conj sql-pipeline stage))
          ;(recur (rest csql-pipeline) (conj filters stage) sql-pipeline)
          )))))