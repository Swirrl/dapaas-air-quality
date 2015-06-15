(ns aqmesh-graft.transform
  (:require [grafter.rdf.io :as io]
            [clojure.string :as st]))

(defn s [s] (if (seq s) (io/s s) ""))

(defn slugify
  "Cleans and slugifies string"
  [s]
  (when (seq s)
    (-> s
        st/trim
        (st/lower-case)
        (st/replace "(" "-")
        (st/replace ")" "")
        (st/replace "  " "")
        (st/replace "," "-")
        (st/replace "." "")
        (st/replace " " "-")
        (st/replace "/" "-")
        (st/replace "'" "")
        (st/replace "---" "-")
        (st/replace "--" "-"))))

(defn parse-sensor
  [s]
  (when (seq s)
    (let [a (-> s (st/replace ".csv" "") (st/split #"_"))]
      (last a))))
