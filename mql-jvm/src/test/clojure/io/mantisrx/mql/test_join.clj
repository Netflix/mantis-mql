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

