(ns aqmesh-graft.transform
  (:require [grafter.rdf.io :as io]
            [clojure.string :as st]
            [aqmesh-graft.prefix :refer :all]))

(defn s [s] (if (seq s) (io/s s) ""))

(defn trim [s] (if (seq s) (st/trim s) ""))

(defn lower-case [s] (if (seq s) (st/lower-case s) ""))

(defn capitalize [s] (if (seq s) (-> s trim st/capitalize) ""))

(defn titleize
  "Capitalizes each word in a string"
  [s]
  (when (seq s)
    (let [a (st/split s #" ")
          c (map st/capitalize a)]
      (->> c (interpose " ") (apply str) trim))))

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

(defn slug-combine
  "Combines slugs to create URI"
  [& args]
  (apply str (interpose "/" args)))

(defn remove-blanks
  "Removes blanks in a string"
  [s]
  (when (seq s)
    (st/replace s " " "")))

(defn title-slug
  "String -> PascalCase"
  [s]
  (when (seq s)
    (-> s
        titleize
        remove-blanks)))

(defn parse-sensor
  [s]
  (when (seq s)
    (let [a (-> s (st/replace ".csv" "") (st/split #"_"))]
      (last a))))

(defn organize-date
  [date]
  (when (seq date)
    (let [[d m y] (st/split date #"/")]
      (apply str (interpose "-" [y m d])))))

(defn ->datetime
  [date time]
  (when (and (seq date) (seq time))
    (let [d (organize-date date)
          dt (str d "T" time)]
      (read-string (str "#inst " (pr-str dt))))))

(defn time-slug
  [s]
  (when (seq s)
    (st/replace s ":" "-")))

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
  [s]
  (when (seq s)
    (let [v (parseValue s)]
      (when (< v 0)
        belowLOD))))
