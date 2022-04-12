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
 (ns io.mantisrx.mql.test-properties
  "The objective of this namespace is to verify correct functionality with
   regards to fetching properties in MQL. Many components of the query such
   as select (aggregate), where, group by, order by all depend on MQL's robust
   property fetching.

   It runs in two modes; simple and jsonpath.
   Simple mode queries allow for a minimal syntax such as:
   `select node, latency from stream where latency > 500`
   whereas jsonpath allows for a more robust key specification as well as
   accessing nested properties such as:
   `select e['metrics']['latency'], e['node'] from stream
    where e['metrics']['latency'] > 500`

   Furthermore MQL provides functionality for accessing nested lists via
   numeric properties:

   `select e['falcor.paths'][0]['path'] from stream`

   Finally MQL also provides an 'any' search for handling unknown property
   locations:
   `select * from stream where e['falcor.paths'][*]['path'] == 'search'`"
  (:require [clojure.test :refer :all]
            [io.mantisrx.mql.jvm.core :as mql]
            [io.mantisrx.mql.transformers :as t]
            [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.blocking :as rxb]
            [io.mantisrx.mql.properties :as mqlp])
  (:import rx.Observable java.util.concurrent.TimeUnit))

(deftest test-mql-get-in-works-on-maps
  (testing "mqlp/get-in works with Clojure maps."
    (let
      [result (mqlp/get-in {"test" 550} ["test"])]
    (is (= 550 result))))
  (testing "mqlp/get-in works with nested Clojure maps."
    (is (= 550 (mqlp/get-in {"test" {"test" 550}} ["test" "test"]))))
  (testing "mqlp/get-in works with missing keys in Clojure maps."
    (is (= nil (mqlp/get-in {} ["test"]))))
  (testing "mqlp/get-in works with missing keys in nested Clojure maps."
    (is (= nil (mqlp/get-in {"test" {}} ["test" "test"])))))

(deftest test-mql-get-in-works-on-vectors
  (testing "mqlp/get-in works with Clojure vectors."
    (is (= 1.0 (mqlp/get-in [5.0 1.0 3.0] [1]))))
  (testing "mqlp/get-in works with nested Clojure vectors."
    (is (= 1.0 (mqlp/get-in [[5.0 10.0] [10.0 1.0] [3.0 4.2]] [1 1]))))
  (testing "mqlp/get-in works with missing indices in nested Clojure vectors"
    (is (= nil (mqlp/get-in [[5.0 10.0] [10.0 1.0] [3.0 4.2]] [1 2])))))

(deftest test-mql-get-in-works-on-mixed-clojure-maps-and-vectors
  (testing "mqlp/get-in works with mixed Clojure maps and vectors."
    (is (= 111 (mqlp/get-in {"metrics" [{"latencies" [120.0 55.2 111]}]} ["metrics" 0 "latencies" 2])))))

(deftest test-mql-get-in-works-on-java-maps
  (testing "mqlp/get-in works with Java maps."
    (let
      [m1 (doto (java.util.HashMap.)
            (.put "test" "bacon")
            identity)]
      (is (= "bacon" (mqlp/get-in m1 ["test"])))))
  (testing "mqlp/get-in works with nested Java maps."
    (let
      [m1 (doto (java.util.HashMap.)
            (.put "test" "bacon")
            identity)
       m2 (doto (java.util.HashMap.)
            (.put "test" m1)
            identity)]
      (is (= "bacon" (mqlp/get-in m2 ["test" "test"]))))))

(deftest test-mql-get-in-works-with-mixed-java-maps-and-lists
  (testing "mqlp/get-in works with mixed Java maps and lists."
    (let
      [list1 (java.util.ArrayList. [120.0 55.2 111])
       map1 (doto (java.util.HashMap.)
            (.put "latencies" list1)
            identity)
       list2 (java.util.ArrayList. [map1])
       m2 (doto (java.util.HashMap.)
            (.put "metrics" list2)
            identity)]
      (is (= (mqlp/get-in m2 ["metrics" 0 "latencies" 2]))))))

;;;;
;;;; Queries
;;;;

(deftest test-mql-get-in-negative-indicies
  (testing "mqlp/get-in works with negative indicies on a Java list."
    (is (= 5.0 (mqlp/get-in (java.util.ArrayList. [0.0 1.0 2.0 3.0 4.0 5.0]) [-1]))))
  (testing "mqlp/get-in works with negative indicies on a Clojure vector"
    (is (= 5.0 (mqlp/get-in [0.0 1.0 2.0 3.0 4.0 5.0] [-1])))))

(def data [{"node" 1 "metrics" {"latency" 125.0}}
           {"node" 2 "metrics" {"latency" 221.2}}
           {"node" 3 "metrics" {"latency" 421.55}}])

(def data2 [{"node" 1 "metrics" [{"latency" 125.0} {"loadAvg5" 12.54}]}
           {"node" 2 "metrics" [ {"latency" 221.2} {"loadAvg5" 10.25}]}
           {"node" 3 "metrics" [ {"latency" 421.55} {"loadAvg5" 15.38}]}])

(def java-data
  [(java.util.HashMap. {"node" 1 "metrics" {"latency" 125.0}})
   (java.util.HashMap. {"node" 2 "metrics" {"latency" 221.2}})
   (java.util.HashMap. {"node" 3 "metrics" {"latency" 421.55}})])

(def java-data2
  [(java.util.HashMap. {"node" 1 "metrics"
                        (java.util.ArrayList. [(java.util.HashMap. {"latency" 125.0})
                                               (java.util.HashMap. {"loadAvg5" 12.54})])})
   (java.util.HashMap. {"node" 2 "metrics"
                        (java.util.ArrayList. [(java.util.HashMap. {"latency" 221.2})
                                               (java.util.HashMap. {"loadAvg5" 10.25})])})
   (java.util.HashMap. {"node" 3 "metrics"
                        (java.util.ArrayList. [(java.util.HashMap. {"latency" 421.55})
                                               (java.util.HashMap. {"loadAvg5" 15.38})])})])

(deftest test-simple-properties
  (testing "`select node from stream` correctly selects node."
    (let
      [q "select node from stream"
       context {"stream" (Observable/from data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"node" 1} {"node" 2} {"node" 3}] result)))))

(deftest test-jsonpath-properties
  (testing "`select e['node'] from stream` correctly selects node."
    (let
      [q "select e['node'] from stream"
       context {"stream" (Observable/from data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"node" 1} {"node" 2} {"node" 3}] result)))))

(deftest test-jsonpath-nested-properties
  (testing "`select e['metrics']['latency'] from stream` correctly selects
            latency."
    (let
      [q "select e['metrics']['latency'] from stream"
       context {"stream" (Observable/from data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"metrics" {"latency" 125.0}} {"metrics" {"latency" 221.2}}
              {"metrics" {"latency" 421.55}}] result))))
  (testing "`select e['metrics'][1]['loadAvg5'] from stream where e['metrics'][1]['loadAvg5'] > 12.0`
            correctly selects and filters data with a list."
    (let
      [q "select e['metrics'][1]['loadAvg5'] from stream where e['metrics'][1]['loadAvg5'] > 12.0"
       context {"stream" (Observable/from data2)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"metrics" { 1 {"loadAvg5" 12.54} }}
              {"metrics" {1 {"loadAvg5" 15.38}}}]
             result)))))

;;;;
;;;; Types
;;;;


;;; We need to ensure that this works with Java Map and List abstractions.

(deftest test-jsonpath-nested-properties
  (testing "`select e['metrics']['latency'] from stream` correctly selects
            latency with HashMap and ArrayList."
    (let
      [q "select e['metrics']['latency'] from stream"
       context {"stream" (Observable/from java-data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"metrics" {"latency" 125.0}} {"metrics" {"latency" 221.2}}
              {"metrics" {"latency" 421.55}}] result))))
  (testing "`select e['metrics'][1]['loadAvg5'] from stream where e['metrics'][1]['loadAvg5'] > 12.0`
            correctly selects and filters data with HashMap and ArrayList."
    (let
      [q "select e['metrics'][1]['loadAvg5'] from stream where e['metrics'][1]['loadAvg5'] > 12.0"
       context {"stream" (Observable/from java-data2)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"metrics" { 1 {"loadAvg5" 12.54} }}
              {"metrics" {1 {"loadAvg5" 15.38}}}]
             result)))))


;;;;
;;;; StartsWith
;;;;

;; TODO: Test all of the below items with MRE as well.

(deftest test-startswith-function
  (testing "sw-property->fn correctly produces a seq of name/value pairs."
    (let
      [f (t/sw-property->fn "nod")
       result (map f data)]
      (is (= [[{:name [ "node"] :value 1}]
             [{:name [ "node"] :value 2}]
             [{:name ["node"] :value 3}]]
             result)))))

(deftest test-prefix-properties
  (testing "select e[^'nod'] from stream` selects node and ignores metrics."
    (let
      [q "select e[^'nod'] from stream"
       context {"stream" (Observable/from data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"node" 1} {"node" 2 } {"node" 3}] result))))
  (testing "select e[^'nod'] from stream` selects node and ignores metrics
            with Java data structures."
    (let
      [q "select e[^'nod'] from stream"
       context {"stream" (Observable/from data2)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"node" 1} {"node" 2 } {"node" 3}] result)))))

(deftest test-prefix-properties-with-simplification
  (testing "select e[^'metric'], metrics, e['metrics']['latency'] from stream simplifies correctly."
    (let
      [q "select e[^'metric'], metrics, e['metrics']['latency'] from stream"
       context {"stream" (Observable/from data)}
       result (rxb/into []  (mql/eval-mql q context))]
      (is (= (map #(identity {"metrics" (get % "metrics")}) data) result)))))

;;;;
;;;; Tick
;;;;

(deftest test-tick-inserts-timestamp
  (testing "select e['node'], tick() from stream` selects node and tick."
    (let
      [q "select e['node'], tick() from stream"
       context {"stream" (Observable/from data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= ["node" "tick"] (keys (first result)))))))

;;;;
;;;; Star Properties
;;;;


(deftest star-property
  (testing "select * from stream where e['metrics'][*]['latency'] > 300.0
            correctly filters events."
    (let
      [q "select * from stream where e['metrics'][*]['latency'] > 300.0"
       context {"stream" (Observable/from data2)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= (second (rest data2))
             (first result)))))
  (testing "select * from stream where e['metrics'][*]['latency'] > 300.0
            correctly filters events when Java datastructures are used."
    (let
      [q "select * from stream where e['metrics'][*]['latency'] > 300.0"
       context {"stream" (Observable/from java-data2)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= (second (rest java-data2))
             (first result))))))

;; TODO: Double stacking does not currently work.
(comment (deftest star-property-double-stacked
  (testing "select * from stream where e['metrics'][*][*] < 12
            correctly filters events."
    (let
      [q "select * from stream where e['metrics'][*][*] < 12"
       context {"stream" (Observable/from data2)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= (second (rest data2))
             (first result)))))
  (testing "select * from stream where e['metrics'][*][*] < 12
            correctly filters events when Java datastructures are used."
    (let
      [q "select * from stream where e['metrics'][*][*] < 12"
       context {"stream" (Observable/from java-data2)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= (second (rest java-data2))
             (first result)))))))

;;;;
;;;; Distinct
;;;;

(deftest distinct-count-correctly-counts
  (testing "select COUNT(distinct node) from stream correctly computes
            disinct count of nodes."
    (let
      [q "select COUNT(distinct node) from stream"
       context {"stream" (Observable/from (conj data {"node" 1}))}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= {"COUNT(node)" 3})))))
