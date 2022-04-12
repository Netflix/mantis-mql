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
(ns io.mantisrx.mql.test-join
  ""
  (:require [clojure.test :refer :all]
            [io.mantisrx.mql.jvm.core :refer :all]
            [instaparse.core :as insta]
            [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.blocking :as rxb])
  (:import rx.Observable java.util.concurrent.TimeUnit))

(deftest test-join-query
  (testing "A join works"
    (let
      [q "select * from table1 join table2 1 seconds on e['node']"
       context {"table1" (Observable/just {"node" "mantisagent-1" "latency" 123.45})
                "table2" (Observable/just {"node" "mantisagent-1" "cpu" 87.13} {"node" "mantisagent-1" "cpu" 10.2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"node" "mantisagent-1" "latency" 123.45 "cpu" 87.13}] result)))))

