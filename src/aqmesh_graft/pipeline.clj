(ns aqmesh-graft.pipeline
  (:require [grafter.tabular :refer [defpipe defgraft columns derive-column
                                     mapc drop-rows read-dataset make-dataset
                                     move-first-row-to-header _ graph-fn
                                     take-rows rename-columns add-column]]
            [grafter.tabular.melt :refer [melt]]
            [grafter.rdf.templater :refer [graph]]
            [grafter.vocabularies.rdf :refer [rdf:a rdfs:label]]
            [grafter.vocabularies.qb :refer [qb:Observation qb:dataSet]]
            [grafter.vocabularies.sdmx-measure :refer [sdmx-measure:obsValue]]
            [grafter.vocabularies.sdmx-attribute :refer [sdmx-attribute:unitMeasure]]
            [grafter.vocabularies.skos :refer [skos:prefLabel skos:inScheme skos:note
                                               skos:ConceptScheme skos:hasTopConcept
                                               skos:topConceptOf skos:Concept]]
            [grafter.vocabularies.dcterms :refer [dcterms:issued dcterms:license
                                                  dcterms:references dcterms:modified
                                                  dcterms:publisher]]
            [aqmesh-graft.prefix :refer :all]
            [aqmesh-graft.transform :refer :all]
            [aqmesh-graft.util :refer [import-rdf]]))

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
        (take-rows 2)
; There seems to be a last blank column ~> problem 'move-first-row-to-header
        (columns ["a" "b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n" "o" "p"
                  "q" "r" "s" "t" "u" "v" "w" "x" "y" "z" "aa" "ab" "ac" "ad" "ae"])
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
  (let [sensor (parse-sensor data-file)]
    (-> (read-dataset data-file)
        (columns ["c" "d"])
        (make-dataset [:longtitude :latitude])
        (drop-rows 1)
        (take-rows 1)
        (add-column :sensor-no sensor)
        (derive-column :sensor-uri [:sensor-no] sensor-id))))

(defpipe convert-sensor-parameter-concept-scheme
  "Pipeline to convert tabular AQMesh sensor measure concept scheme"
  [data-file]
  (-> (read-dataset data-file)
      (take-rows 2)
      (columns ["a" "b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n" "o" "p"
                "q" "r" "s" "t" "u" "v" "w" "x" "y" "z" "aa" "ab" "ac" "ad" "ae"])
      (make-dataset move-first-row-to-header)
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

;;
;; Pipelines
;;

(defn aqmesh-sensor-data-pipeline
  "Pipeline to convert the tabular AQMesh sensor measure data sheet into graph data."
  [data-file output]
  (-> (convert-aqmesh-sensor-data data-file)
      aqmesh-sensor-template
      (import-rdf output))
  (println "Grafted: " data-file))

(defn aqmesh-sensor-pipeline
  "Pipeline to convert the tabular AQMesh sensor data into graph data."
  [data-file output]
  (-> (convert-aqmesh-sensor data-file)
      sensor-template
      (import-rdf output))
  (println "Grafted: " data-file))

(defn sensor-parameter-concept-scheme-pipeline
  "Pipeline to convert the tabular AQMesh sensor concept scheme"
  [data-file output]
  (-> (convert-sensor-parameter-concept-scheme data-file)
      sensor-parameter-concept-scheme-template
      (import-rdf output))
  (println "Grafted: " data-file))
