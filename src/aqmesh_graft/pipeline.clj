;; (ns aqmesh-graft.pipeline
;;   (:require [grafter.tabular :refer :all]
;;             [clojure.string]
;;             [grafter.rdf :refer [prefixer s]]
;;             [grafter.rdf.templater :refer [graph]]
;;             [grafter.vocabularies.rdf :refer :all]
;;             [grafter.vocabularies.qb :refer :all]
;;             [grafter.vocabularies.sdmx-measure :refer :all]
;;             [grafter.vocabularies.sdmx-attribute :refer :all]
;;             [grafter.vocabularies.skos :refer :all]
;;             [grafter.vocabularies.foaf :refer :all]
;;             [grafter.vocabularies.dcterms :refer :all]))

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


(defn ->s [st] (if st (s st) ""))

(defn trim [st] (if st (clojure.string/trim st) ""))

(defn lower-case [st] (if st (clojure.string/lower-case st) ""))

(defn capitalize [st] (if st (-> st trim clojure.string/capitalize) ""))

(defn titleize
  "Capitalizes each word in a string"
  [st]
  (when (seq st)
    (let [a (clojure.string/split st #" ")
          c (map clojure.string/capitalize a)]
      (->> c (interpose " ") (apply str) trim))))

(defn slugify
  "Cleans and slugifies string"
  [st]
  (let [replace clojure.string/replace]
    (if (seq st)
      (-> (str st)
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
          (replace "--" "-"))
      (throw (RuntimeException. (str "Cannot slugify value '" st "'"))))))

(defn slug-combine
  "Combines slugs to create URI"
  [& args]
  (apply str (interpose "/" args)))

(defn remove-blanks
  "Removes blanks in a string"
  [st]
  (when (seq st)
    (clojure.string/replace st " " "")))

(defn title-slug
  "String -> PascalCase"
  [st]
  (when (seq st)
    (-> st
        titleize
        remove-blanks)))

;;
;; Parsing
;;

(defn parse-sensor
  "Get the sensor number from the filename"
  [st]
  (when (seq st)
    (let [a (-> st (clojure.string/replace #"\..*" "") (clojure.string/split #"_"))]
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
  [st]
  (when (seq st)
    (let [v (parseValue st)]
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
  [st]
  (when (seq st)
    (clojure.string/replace st ":" "-")))

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

(defn add-filename-to-column [ds destination-column]
  (let [fname (:grafter.tabular/data-source (meta ds))]
    (add-column ds destination-column fname)))

;;
;; Templates
;;

(def aqmesh-sensor-template
  (graph-fn [{:keys [obs-uri ds value datetime unit sensor-uri below-lod param-uri label sensor-no date time]}]
            (graph (base-graph "air-quality")
                   [obs-uri
                    [rdf:a qb:Observation]
                    [rdfs:label (->s (str label
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
                    [rdfs:label (->s (str "Sensor " sensor-no))]
                    [geo:long (->s longtitude)]
                    [geo:lat (->s latitude)]])))

(def sensor-parameter-concept-scheme-template
  (graph-fn [{:keys [param-uri label]}]
            (graph (base-graph "sensor-parameter-concept-scheme")
                   [param-uri
                    [rdf:a skos:Concept]
                    [rdfs:label (->s label)]
                    [skos:prefLabel (->s label)]
                    [skos:inScheme parameter-cs]
                    [skos:topConceptOf parameter-cs]]

                   [parameter-cs
                    [rdf:a skos:ConceptScheme]
                    [rdfs:label (->s "AQMesh Sensor Parameter Concept Scheme")]
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
  (-> (read-dataset data-file)
      (take-rows 2)
      ;; There seems to be a last blank column ~> problem 'move-first-row-to-header
      (columns (range 31))

      (make-dataset move-first-row-to-header)
      (rename-columns (comp keyword slugify))
      (add-filename-to-column :sensor-no)
      (mapc {:sensor-no parse-sensor})
      (columns [:date :time :no-final :no2-final :co-final :o3-final :8-temp-celcius :9-rh-% :10-ap-mbar :sensor-no])
      (derive-column :sensor-uri [:sensor-no] sensor-id)
      (melt [:date :time :sensor-uri :sensor-no])
      (derive-column :datetime [:date :time] ->datetime)
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
      (derive-column :param-uri [:label] (comp parameter-def remove-blanks))))

(defpipe convert-aqmesh-sensor
  "Pipeline to convert tabular AQMesh sensor data"
  [data-file]
  (-> (read-dataset data-file :format :csv)
      (make-dataset [:longtitude :latitude])
      (drop-rows 1)
      (take-rows 1)
      (add-filename-to-column :sensor-no)
      (mapc {:sensor-no parse-sensor})
      (derive-column :sensor-uri [:sensor-no] sensor-id)))

(defpipe convert-sensor-parameter-concept-scheme
  "Pipeline to convert tabular AQMesh sensor measure concept scheme"
  [data-file]
  (-> (read-dataset data-file :format :csv)
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
