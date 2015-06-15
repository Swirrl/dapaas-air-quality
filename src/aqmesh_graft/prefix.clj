(ns aqmesh-graft.prefix
  (:require [grafter.rdf :refer [prefixer]]))

;; Bases

(def base-domain (prefixer "http://data.dapaas.eu"))
(def base-graph (prefixer (base-domain "/graph/")))
(def base-id (prefixer (base-domain "/id/")))
(def base-vocab (prefixer (base-domain "/def/")))
(def base-data (prefixer (base-domain "/data/")))
