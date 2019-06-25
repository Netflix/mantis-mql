(ns io.mantisrx.mql.test-anomaly
  "Namespace designated for testing anomaly detection."
  (:require [clojure.test :refer :all]
            [io.mantisrx.mql.core :refer :all]
            [instaparse.core :as insta]
            [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.blocking :as rxb])
  (:import rx.Observable java.util.concurrent.TimeUnit))

(deftest test-simple-anomaly
  (testing "select * from stream returns everything."
    (let
      [q "select * from stream anomaly mad cpu 1.0 2.5"
       context {"stream" (Observable/just
                           {"node" "a" "cpu" 11.0}
                           {"node" "b" "cpu" 10.1}
                           {"node" "c" "cpu" 12.5}
                           {"node" "c" "cpu" 200.0})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"node" "a" "cpu" 11.0 "anomalous" false}
              {"node" "b" "cpu" 10.1 "anomalous" false}
              {"node" "c" "cpu" 12.5 "anomalous" false}
              {"node" "c" "cpu" 200.0 "anomalous" true}]
             result)))))
