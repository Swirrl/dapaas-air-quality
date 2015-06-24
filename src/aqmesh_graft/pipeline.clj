(ns aqmesh-graft.pipeline
  (:require [grafter.tabular :refer [defpipe defgraft columns derive-column
                                     mapc drop-rows read-dataset make-dataset
                                     move-first-row-to-header _ graph-fn
                                     take-rows rename-columns add-column]]
            [clojure.string]
            [grafter.rdf.io :as io]
            [grafter.rdf :refer [prefixer]]
            [grafter.tabular.melt :refer [melt]]
            [grafter.rdf.templater :refer [graph]]
            [grafter.vocabularies.rdf :refer [rdf:a rdfs:label]]
            [grafter.vocabularies.qb :refer [qb:Observation qb:dataSet]]
            [grafter.vocabularies.sdmx-measure :refer [sdmx-measure:obsValue]]
            [grafter.vocabularies.sdmx-attribute :refer [sdmx-attribute sdmx-attribute:unitMeasure]]
            [grafter.vocabularies.sdmx-attribute :refer [sdmx-attribute]]
            [grafter.vocabularies.skos :refer [skos:prefLabel skos:inScheme skos:note
                                               skos:ConceptScheme skos:hasTopConcept
                                               skos:topConceptOf skos:Concept]]
            [grafter.vocabularies.dcterms :refer [dcterms:issued dcterms:license
                                                  dcterms:references dcterms:modified
                                                  dcterms:publisher]]))

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


(defn s [s] (if (seq s) (io/s s) ""))

(defn trim [s] (if (seq s) (clojure.string/trim s) ""))

(defn lower-case [s] (if (seq s) (clojure.string/lower-case s) ""))

(defn capitalize [s] (if (seq s) (-> s trim clojure.string/capitalize) ""))

(defn titleize
  "Capitalizes each word in a string"
  [s]
  (when (seq s)
    (let [a (clojure.string/split s #" ")
          c (map clojure.string/capitalize a)]
      (->> c (interpose " ") (apply str) trim))))

(defn slugify
  "Cleans and slugifies string"
  [s]
  (let [replace clojure.string/replace]
    (when (seq s)
      (-> s
          clojure.string/trim
          clojure.string/lower-case
          (replace "(" "-")
          (replace ")" "")
          (replace "  " "")
          (replace "," "-")
          (replace "." "")
          (replace " " "-")
          (replace "/" "-")
          (replace "'" "")
          (replace "---" "-")
          (replace "--" "-")))))

(defn slug-combine
  "Combines slugs to create URI"
  [& args]
  (apply str (interpose "/" args)))

(defn remove-blanks
  "Removes blanks in a string"
  [s]
  (when (seq s)
    (clojure.string/replace s " " "")))

(defn title-slug
  "String -> PascalCase"
  [s]
  (when (seq s)
    (-> s
        titleize
        remove-blanks)))

;;
;; Parsing
;;

(defn parse-sensor
  "Get the sensor number from the filename"
  [s]
  (when (seq s)
    (let [a (-> s (clojure.string/replace ".csv" "") (clojure.string/split #"_"))]
      (first a))))

(defmulti parseValue class)
(defmethod parseValue :default            [x] x)
(defmethod parseValue nil                 [x] nil)
(defmethod parseValue java.lang.Character [x] (Character/getNumericValue x))
(defmethod parseValue java.lang.String    [x] (if (= "" x)
                                                nil
                                                (if (.contains x ".")
                                                  (Double/parseDouble x)
                                                  (Integer/parseInt x))))

(defn belowLOD?
  "When a AQMesh sensor data value concentration is negative it's
  because it's below limit of detection TODO use value given in the
  source data"
  [s]
  (when (seq s)
    (let [v (parseValue s)]
      (when (< v 0)
        belowLOD))))

;;
;; Date
;;

(defn organize-date
  "Transform date dd/mm/yyyy ~> yyyy-mm-dd"
  [date]
  (when (seq date)
    (let [[d m y] (clojure.string/split date #"/")]
      (apply str (interpose "-" [y m d])))))

(defn ->datetime
  "Given a date dd/mm/yyyy and a time hh:mm
  returns a XSDDatetime"
  [date time]
  (when (and (seq date) (seq time))
    (let [d (organize-date date)
          dt (str d "T" time)]
      (read-string (str "#inst " (pr-str dt))))))

(defn time-slug
  "Transform time to use in a slug"
  [s]
  (when (seq s)
    (clojure.string/replace s ":" "-")))

;;
;; Measure cleaning
;;

(def measure-slug
  {:no-final "NO"
   :no2-final "NO2"
   :co-final "CO"
   :o3-final "O3"
   :8-temp-celcius "temperature"
   :9-rh-% "relativeHumidity"
   :10-ap-mbar "airPressure"})

(def measure-unit
  {:no-final ppb
   :no2-final ppb
   :co-final ppb
   :o3-final ppb
   :8-temp-celcius qudt:DegreeCelsius
   :9-rh-% percentage
   :10-ap-mbar qudt:Millibar})

(def measure-label
  {:no-final "Concentration NO"
   :no2-final "Concentration NO2"
   :co-final "Concentration CO"
   :o3-final "Concentration O3"
   :8-temp-celcius "Temperature"
   :9-rh-% "Relative Humidity"
   :10-ap-mbar "Air Pressure"})


;;
;; Templates
;;

(def aqmesh-sensor-template
  (graph-fn [{:keys [obs-uri ds value datetime unit sensor-uri below-lod param-uri label sensor-no date time]}]
            (graph (base-graph "air-quality")
                   [obs-uri
                    [rdf:a qb:Observation]
                    [rdfs:label (s (str label
                                        ", sensor " sensor-no
                                        ", " date
                                        ", " time))]
                    [sensor sensor-uri]
                    [qb:dataSet ds]
                    [parameter param-uri]
                    [sdmx-dimension:refTime datetime]
                    [sdmx-measure:obsValue (parseValue value)]
                    [sdmx-attribute:unitMeasure unit]
                    [sdmx-attribute:obsStatus below-lod]])))

(def sensor-template
  (graph-fn [{:keys [sensor-uri sensor-no longtitude latitude]}]
            (graph (base-graph "sensor")
                   [sensor-uri
                    [rdf:a AQMeshSensor]
                    [rdfs:label (s (str "Sensor " sensor-no))]
                    [geo:long (s longtitude)]
                    [geo:lat (s latitude)]])))

(def sensor-parameter-concept-scheme-template
  (graph-fn [{:keys [param-uri label]}]
            (graph (base-graph "sensor-parameter-concept-scheme")
                   [param-uri
                    [rdf:a skos:Concept]
                    [rdfs:label (s label)]
                    [skos:prefLabel (s label)]
                    [skos:inScheme parameter-cs]
                    [skos:topConceptOf parameter-cs]]

                   [parameter-cs
                    [rdf:a skos:ConceptScheme]
                    [rdfs:label (s "AQMesh Sensor Parameter Concept Scheme")]
                    [dcterms:issued (java.util.Date. "2015/06/16")]
                    [dcterms:modified (java.util.Date. "2015/06/16")]
                    [dcterms:license licence]
                    [dcterms:publisher AQMesh]
                    [dcterms:references pmd-doc]
                    [skos:hasTopConcept param-uri]])))

;;
;; Pipes
;;

(defpipe convert-aqmesh-sensor-data
  "Pipeline to convert tabular AQMesh sensor measure data"
  [data-file]
  (let [sensor (parse-sensor data-file)]
    (-> (read-dataset data-file)
        ;(take-rows 2)
        ;; There is a small problem in the CSV as the header columns have a blank last column (thanks to a trailing ,)
        (make-dataset move-first-row-to-header)
        (rename-columns (comp keyword slugify))
        (columns [:date :time :no-final :no2-final :co-final :o3-final :8-temp-celcius :9-rh-% :10-ap-mbar])
        (melt [:date :time])
        (add-column :sensor-no sensor)
        (derive-column :datetime [:date :time] ->datetime)
        (derive-column :sensor-uri [:sensor-no] sensor-id)
        (derive-column :date-slug [:date] organize-date)
        (derive-column :time-slug [:time] time-slug)
        (derive-column :measure-slug [:variable] measure-slug)
        (derive-column :obs-slug [:date-slug :time-slug :sensor-no :measure-slug] slug-combine)
        (derive-column :obs-uri [:obs-slug] base-data)
        (derive-column :dt-slug [:date-slug :time-slug] slug-combine)
        (derive-column :ds [:dt-slug] base-data)
        (derive-column :unit [:variable] measure-unit)
        (derive-column :below-lod [:value] belowLOD?)
        (derive-column :label [:variable] measure-label)
        (derive-column :param-uri [:label] (comp parameter-def remove-blanks)))))

(defpipe convert-aqmesh-sensor
  "Pipeline to convert tabular AQMesh sensor data"
  [data-file]
  (let [ss (parse-sensor data-file)]
    (-> (read-dataset data-file)
        (make-dataset [:longtitude :latitude])
        (drop-rows 1)
        (take-rows 1)
        (add-column :sensor-no ss)
        (derive-column :sensor-uri [:sensor-no] sensor-id))))

(defpipe convert-sensor-parameter-concept-scheme
  "Pipeline to convert tabular AQMesh sensor measure concept scheme"
  [data-file]
  (-> (read-dataset data-file)
      (make-dataset move-first-row-to-header)
      (take-rows 1)
      (rename-columns (comp keyword slugify))
      (columns [:date :time :no-final :no2-final :co-final :o3-final :8-temp-celcius :9-rh-% :10-ap-mbar])
      (melt [:date :time])
      (columns [:variable])
      (derive-column :label [:variable] measure-label)
      (derive-column :param-uri [:label] (comp parameter-def remove-blanks))))

;;
;; Grafts
;;

(defgraft aqmesh-sensor-data->graph
  "Pipeline to convert the tabular AQMesh sensor measure data sheet into graph data."
  convert-aqmesh-sensor-data aqmesh-sensor-template)

(defgraft aqmesh-sensor->graph
  "Pipeline to convert the tabular AQMesh sensor data into graph data."
  convert-aqmesh-sensor sensor-template)

(defgraft sensor-parameter-concept-scheme->graph
  "Pipeline to convert the tabular AQMesh sensor concept scheme into graph data."
  convert-sensor-parameter-concept-scheme sensor-parameter-concept-scheme-template)
