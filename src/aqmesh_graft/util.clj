(ns aqmesh-graft.util
  (:require [grafter.rdf.protocols :as pr]
            [grafter.rdf.io :as io]))

(defn blank?
  "Blank boolean"
  [v]
  (or (nil? v) (= "" v)))

(defn basic-filter
    "Filters blank triples"
    [triples]
    (filter #(not (blank? (pr/object %1))) triples))

(defn import-rdf
  "Given a seq of Quads, returns a RDF file"
  ([quads-seq destination]
   (import-rdf quads-seq destination basic-filter))
  ([quads-seq destination filter]
   (let [now (java.util.Date.)
         quads (->> quads-seq
                    filter)]
     (pr/add (io/rdf-serializer destination) quads))))
