(ns squery-spark.datasets.streams)

(defn print-stream
  "print the table by query, instead of using sink=console"
  [stream-table n delay]
  (dotimes [_ n] (.show stream-table) (Thread/sleep delay)))