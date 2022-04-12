;
; Copyright 2022 Netflix, Inc.
;
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
;     http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.
;
(ns io.mantisrx.mql.test-anomaly
  "Namespace designated for testing anomaly detection."
  (:require [clojure.test :refer :all]
            [io.mantisrx.mql.jvm.core :refer :all]
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
