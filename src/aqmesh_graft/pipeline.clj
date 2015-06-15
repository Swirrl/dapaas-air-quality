(ns aqmesh-graft.pipeline
    (:require
     [grafter.tabular :refer [defpipe defgraft columns derive-column
                              mapc drop-rows read-dataset make-dataset
                              move-first-row-to-header _ graph-fn
                              take-rows rename-columns add-column]]
     [grafter.tabular.melt :refer [melt]]
     [grafter.rdf.templater :refer [graph]]
     [grafter.vocabularies.rdf :refer [rdf:a rdfs:label]]
     [aqmesh-graft.prefix :refer :all]
     [aqmesh-graft.transform :refer :all]))

(def aqmesh-sensor-template
  (graph-fn [{:keys []}]
            (graph (base-graph "")
                   [
                    [rdf:a ]
                    [rdfs:label]])))

(defpipe convert-aqmesh-sensor-data
  "Pipeline to convert tabular AQMesh sensor data into a different tabular format."
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
        (add-column :sensor sensor))))

(defgraft aqmesh-sensor-data->graph
  "Pipeline to convert the tabular AQMesh sensor data sheet into graph data."
  convert-aqmesh-sensor-data aqmesh-sensor-template)
