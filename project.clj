(defproject aqmesh-graft "0.1.0-SNAPSHOT"
  :description "A Grafter project to RDFize AQMesh Sensor Data"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [grafter "0.5.0-SNAPSHOT"]
                 [grafter/vocabularies "0.1.1-SNAPSHOT"]
                 [org.slf4j/slf4j-jdk14 "1.7.5"]]

  :repl-options {:init (set! *print-length* 200)
                 :init-ns aqmesh-graft.pipeline }

  :plugins [[lein-grafter "0.5.0-SNAPSHOT"]])
