(ns aqmesh-graft.prefix
  (:require [grafter.rdf :refer [prefixer]]
            [grafter.vocabularies.sdmx-attribute :refer [sdmx-attribute]]))

;; Bases

(def base-domain (prefixer "http://data.dapaas.eu"))
(def base-graph (prefixer (base-domain "/graph/")))
(def base-id (prefixer (base-domain "/id/")))
(def base-vocab (prefixer (base-domain "/def/")))
(def base-data (prefixer (base-domain "/data/")))
(def base-concept (prefixer (base-vocab "concept-scheme/")))

;; PMD

(def licence "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/")
(def pmd-doc "http://docs.publishmydata.com")
(def AQMesh (base-domain "/AQMesh"))

;; Vocabularies

(def sdmx-attribute:obsStatus (sdmx-attribute "obsStatus"))
(def sdmx-dimension:refTime "http://purl.org/linked-data/sdmx/2009/dimension#refTime")
(def qudt (prefixer "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#"))
(def qudt:Millibar (qudt "Millibar"))
(def qudt:DegreeCelsius (qudt "DegreeCelsius"))
(def ppb (base-vocab "unit/partPerBillion"))
(def percentage (base-vocab "unit/percentage"))
(def geo:long "http://www.w3.org/2003/01/geo/wgs84_pos#long")
(def geo:lat "http://www.w3.org/2003/01/geo/wgs84_pos#lat")
(def belowLOD (base-vocab "belowLimitOfDetection"))

;; Prefixes

(def dimension (prefixer (base-vocab "dimensions/")))
(def parameter (dimension "parameter"))
(def sensor-id (prefixer (base-id "sensor/")))
(def AQMeshSensor (base-vocab "AQMeshSensor"))
(def sensor (base-vocab "sensor"))
(def parameter-def (prefixer (base-vocab "air-quality/")))
(def parameter-cs (base-concept "air-quality/parameter"))
