(ns aqmesh-graft.core
  (:require [miner.ftp :as ftp]
            [clojure.string :as st]
            [aqmesh-graft.pipeline :refer [aqmesh-sensor-data-pipeline
                                           aqmesh-sensor-pipeline]]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs])
  (:gen-class))

;;
;; Main
;;

(defn files
  [ftp-url]
  (ftp/list-files ftp-url))

(defn ->graph-name
  "Create output graph name from a CSV input"
  [s]
  (when (seq s)
    (let [prefix "graphs/"
          file (first (st/split s #"\."))
          ext ".ttl"]
      (str prefix file ext))))

(defn ->sensor-name
  "Create output sensor name from a CSV input"
  [s]
  (when (seq s)
    (let [prefix "sensors/sensor_"
          f (first (st/split s #"\."))
          file (first (st/split f #"_"))
          ext ".ttl"]
      (str prefix file ext))))

(defn apply-pipeline
  "Apply AQMesh Sensor Data Pipeline to a file"
  [pipeline namef]
  (fn
    [file]
    (let [output-name (namef file)]
      (pipeline file output-name))))

(defn ftp->graph
  "Given a a filename, get it from the FTP server, graft it and delete it"
  [ftp-url file f]
  (ftp/with-ftp [client ftp-url]
    (ftp/client-get client file)
    (f file)
    ;; If you want to delete files after downloading them uncomment this line.
    ;; Otherwise they'll be downloaded into your local directory.
    ;;(fs/delete file)
    ))

(defn -main
  "Get the source data from the FTP server and apply the pipeline
  Return the graphs in `graphs/`
  Given a integer `n` as argument, returns the last n graphs"
  [ftp-url & [n]]
  (let [n (if (string? n) (read-string n) n)]
    (if (integer? n)
      (doall (map #(ftp->graph ftp-url
                               %
                               (apply-pipeline aqmesh-sensor-data-pipeline ->graph-name))
                  (take-last n (files ftp-url))))
      (doall (map #(ftp->graph ftp-url
                               %
                               (apply-pipeline aqmesh-sensor-data-pipeline ->graph-name))
                  (files ftp-url))))))

(defn get-sensors-names
  [coll]
  (let [f (fn [name] (first (st/split name #"_")))]
    (vec (set (map f coll)))))

(defn sensors-names [ftp-url] (get-sensors-names (files ftp-url)))

(defn find-sensor-in-files
  [ftp-url sensor]
  (let [p (re-pattern sensor)]
    (first (filter #(re-find p %) (files ftp-url)))))

(defn coll-of-sensor-files
  [ftp-url]
  (map #(find-sensor-in-files ftp-url %)
       (sensors-names ftp-url)))

(defn sensor-main
  "Get the source data from the FTP server and apply the sensor pipeline
  Return the graphs in `sensors/`"
  [ftp-url]
  (doall (map #(ftp->graph ftp-url
                           %
                           (apply-pipeline aqmesh-sensor-pipeline ->sensor-name))
              (coll-of-sensor-files ftp-url))))
