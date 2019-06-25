 (ns io.mantisrx.mql.test-aggregate-query
  "The objective of this namespace is to verify correct functionality with
   aggregate queries. This verification includes complex interactions
   such as windowing, grouping, ordering and limiting at the same time."
  (:require [clojure.test :refer :all]
            [io.mantisrx.mql.jvm.core :as mql]
            [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.blocking :as rxb])
  (:import rx.Observable java.util.concurrent.TimeUnit))

(defn stream-data
  [interv data]
  (rx/take (count data)
           (->>
           (Observable/interval interv TimeUnit/MILLISECONDS)
           (rx/map #(nth data %)))))

(def data [{"a" 1} {"a" 2} {"a" 3}])

(deftest test-count-query
  (testing "`select COUNT(a) from stream window 1` counts the stream."
    (let
      [q "select COUNT(a) from stream window 1"
       context {"stream" (stream-data 10 data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"COUNT(a)" 3}] result))))
  (testing "`select COUNT(a) from stream window 1` counts the stream correctly
            with data for which which some values do not contain property a."
    (let
      [q "select COUNT(a) from stream window 1"
       context {"stream" (stream-data 10 (concat data [{"b" 1}]))}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"COUNT(a)" 3}] result))))
  (testing "`select COUNT(a) from stream window 1` counts the stream correctly
            when some values are filtered out."
    (let
      [q "select COUNT(a) from stream window 1 where a < 3"
       context {"stream" (stream-data 10 data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"COUNT(a)" 2}] result)))))

(deftest test-sum-query
  (testing "`select SUM(a) from stream window 1` sums the stream."
    (let
      [q "select SUM(a) from stream window 1"
       context {"stream" (stream-data 10 data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"SUM(a)" 6}] result))))
  (testing "`select SUM(a) from stream window 1` sums the stream correctly
            with data for which which some values do not contain property a."
    (let
      [q "select SUM(a) from stream window 1"
       context {"stream" (stream-data 10 (concat data [{"b" 1}]))}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"SUM(a)" 6}] result))))
  (testing "`select SUM(a) from stream window 1` sums the stream correctly
            with data for which which no values contain property a."
    (let
      [q "select SUM(a) from stream window 1"
       context {"stream" (stream-data 10 [{"b" 1}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"SUM(a)" 0}] result)))))

(deftest test-average-query
  (testing "`select average(a) from stream window 1` averages the stream."
    (let
      [q "select average(a) from stream window 1"
       context {"stream" (stream-data 10 data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"AVERAGE(a)" 2}] result))))
  (testing "`select average(a) from stream window 1` averages the stream 
            correctly when the stream contains some data which does not
            contain a."
    (let
      [q "select average(a) from stream window 1"
       context {"stream" (stream-data 10 (concat data [{"b" 1}]))}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"AVERAGE(a)" 2}] result))))
  (comment (testing "`select average(a) from stream window 1` averages the stream 
            correctly when the stream contains no data.
                     TODO: Sometimes we get nil, sometimes we get empty list."
    (let
      [q "select average(a) from stream window 1"
       context {"stream" (stream-data 100 [])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"AVERAGE(a)" nil}] result))))))

(deftest test-min-query
  (testing "`select min(a) from stream window 1` computes the min."
    (let
      [q "select min(a) from stream window 1"
       context {"stream" (stream-data 10 data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"MIN(a)" 1}] result))))
  (testing "`select min(a) from stream window 1` computes the min correctly
            when the stream contains some data which does not contain a."
    (let
      [q "select min(a) from stream window 1"
       context {"stream" (stream-data 10 (concat data [{"b" 1}]))}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"MIN(a)" 1}] result))))
  (testing "`select min(a) from stream window 1` computes the min as nil
            when the stream contains no data which contains a."
    (let
      [q "select min(a) from stream window 1"
       context {"stream" (stream-data 10 [{"b" 1}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"MIN(a)" nil}] result)))))

(deftest test-max-query
  (testing "`select max(a) from stream window 1` computes the max"
    (let
      [q "select MAX(a) from stream window 1"
       context {"stream" (stream-data 10 data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"MAX(a)" 3}] result))))
  (testing "`select max(a) from stream window 1` computes the max as nil
            when the stream contains no data which contains a."
    (let
      [q "select max(a) from stream window 1"
       context {"stream" (stream-data 10 [{"b" 1}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"MAX(a)" nil}] result)))))

(deftest test-aggregate-query-no-window
  (testing "`select max(a) from stream` computes the max on a discrete stream. "
    (let
      [q "select MAX(a) from stream"
       context {"stream" (stream-data 10 data)}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"MAX(a)" 3}] result)))))

;;;;
;;;; GROUPING
;;;;

(deftest test-aggregate-query-group-by-no-window
  (testing "`select max(a), b from stream group by b` computes the min on a discrete stream. "
    (let
      [q "select MAX(a), b from stream group by b"
       context {"stream" (Observable/just {"a" 5 "b" 1} {"a" 1 "b" 2}
                                           {"a" 1 "b" 1} {"a" 1 "b" 1}
                                           {"a" 11 "b" 2})}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"MAX(a)" 5 "b" 1} {"MAX(a)" 11 "b" 2}] result)))))

;;;;
;;;; WINDOWING
;;;;

(deftest test-aggregate-count-multiple-windows
  (testing "`select count(a) from stream window 1` counts correctly
           over three windows."
    (let
      [q "select count(a) from stream window 1"
       context {"stream" (stream-data 525 [{"a" 1} {"a" 1} {"a" 1} {"a" 1}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"COUNT(a)" 1} {"COUNT(a)" 2} {"COUNT(a)" 1}] result)))
    (testing "`select sum(a) from stream window |1|` sum correctly"
      (let
        [q "select sum(a) from stream window |2|"
         context {"stream" (stream-data 10 [{"a" 5} {"a" 8} {"a" 2} {"a" 4}])}
         result (rxb/into []  (mql/eval-mql q context))]
        (is (= [{"SUM(a)" 13} {"SUM(a)" 6}] result)))))

  (testing "`select count(a) from stream window 1 group by b` counts correctly
           over two windows with groups."
    (let
      [q "select count(a), b from stream window 1 group by b"
       context {"stream" (stream-data 325 [{"a" 1 "b" 1} {"a" 1 "b" 2}
                                           {"a" 1 "b" 1} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [[{"COUNT(a)" 2 "b" 1} {"COUNT(a)" 1 "b" 2}]
              [{"COUNT(a)" 1 "b" 1} {"COUNT(a)" 1 "b" 2}]] result)))))

(deftest test-aggregate-window-group-limit
  (testing "`select count(a) from stream window 1 group by b limit 1`
            correctly limits each window to one result."
    (let
      [q "select count(a), b from stream window 1 group by b limit 1"
       context {"stream" (stream-data 325 [{"a" 1 "b" 1} {"a" 1 "b" 2}
                                           {"a" 1 "b" 1} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [[{"COUNT(a)" 2 "b" 1}]
              [{"COUNT(a)" 1 "b" 1}]] result)))))

(deftest test-aggregate-window-order
  (testing "`select count(a) from stream window 1 order by e['COUNT(a)']`
            correctly orders the output."
    (let
      [q "select count(a) from stream window 1 order by e['COUNT(a)']"
       context {"stream" (stream-data 325 [{"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [
              {"COUNT(a)" 2} {"COUNT(a)" 3}] result))))
  (testing "`select count(a) from stream window 1 order by e['COUNT(a)'] desc`
            correctly reverse orders the output."
    (let
      [q "select count(a) from stream window 1 order by e['COUNT(a)'] desc"
       context {"stream" (stream-data 325 [{"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [
              {"COUNT(a)" 3} {"COUNT(a)" 2}] result)))))

(deftest test-aggregate-window-group-order
  (testing "`select count(a) from stream window 1 group by b order by b`
            correctly orders the output."
    (let
      [q "select count(a), b from stream window 1 group by b order by b"
       context {"stream" (stream-data 325 [{"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [[{"COUNT(a)" 1 "b" 1} {"COUNT(a)" 2 "b" 2}]
              [{"COUNT(a)" 1 "b" 1} {"COUNT(a)" 1 "b" 2}]] result)))))

(deftest test-aggregate-window-group-order-limit
  (testing "`select count(a) from stream window 1 group by b order by e['COUNT(a)'] limit 1`
            correctly orders the output limiting to the lowest result."
    (let
      [q "select count(a), b from stream window 1 group by b order by e['COUNT(a)'] limit 1"
       context {"stream" (stream-data 325 [{"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [[{"COUNT(a)" 1 "b" 1}]
              [{"COUNT(a)" 1 "b" 1}]] result))))

  (testing "`select count(a) from stream window 1 group by b order by e['COUNT(a)'] DESC limit 1`
            correctly orders the output limiting to the highest result."
    (let
      [q "select count(a), b from stream window 1 group by b order by e['COUNT(a)'] DESC limit 1"
       context {"stream" (stream-data 325 [{"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [[{"COUNT(a)" 2 "b" 2}]
              [{"COUNT(a)" 1 "b" 1}]] result)))))

(deftest test-aggregate-window-group-order-limit-with-threading
  (testing "`select count(a) from stream window 1 group by b order by e['COUNT(a)'] limit 1`
            correctly orders the output limiting to the lowest result with threading."
    (let
      [q "select count(a), b from stream window 1 group by b order by e['COUNT(a)'] limit 1"
       context {"stream" (stream-data 325 [{"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context "full" true))]
      (is (= [[{"COUNT(a)" 1 "b" 1}]
              [{"COUNT(a)" 1 "b" 1}]] result))))

  (testing "`select count(a) from stream window 1 group by b order by e['COUNT(a)'] DESC limit 1`
            correctly orders the output limiting to the highest result with threading."
    (let
      [q "select count(a), b from stream window 1 group by b order by e['COUNT(a)'] DESC limit 1"
       context {"stream" (stream-data 325 [{"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2} {"a" 1 "b" 1}
                                           {"a" 1 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context "full" true))]
      (is (= [[{"COUNT(a)" 2 "b" 2}]
              [{"COUNT(a)" 1 "b" 1}]] result)))))

(deftest test-list-query
  (testing "`select list(a) from stream window 1` returns the list"
    (let
      [q "select list(a) from stream window 1"
       context {"stream" (stream-data 10 data)}
       result (rxb/into []  (mql/eval-mql q context))]
      (is (= [{"LIST(a)" [1 2 3]}] result))))
  (testing "`select list(distinct a) from stream window 1` returns the distinct list."
    (let
      [q "select list(distinct a) from stream window 1"
       context {"stream" (stream-data 10 [{"a" 1} {"a" 2} {"a" 1}])}
       result (rxb/into [] (mql/eval-mql q context))]
      (is (= [{"LIST(a)" [1 2]}] result)))))

(deftest test-having-clause
  (testing "`select count(distinct a), list(distinct a), b from stream window 1 group by b having e['COUNT(a)'] > 2`
            correctly filters the aggregated results."
    (let
      [q "select count(distinct a), list(distinct a), b from stream window 1 group by b having e['COUNT(a)'] > 2"
       context {"stream" (stream-data 10 [{"a" 1 "b" 1} {"a" 2 "b" 1} {"a" 3 "b" 1} {"a" 1 "b" 2} {"a" 2 "b" 2}])}
       result (rxb/into []  (mql/eval-mql q context))] []
      (is (= [[{"COUNT(a)" 3 "LIST(a)" [1 2 3] "b" 1}]] result)))))

(deftest test-percentile-clause
  (testing "`select p50(a) from stream window 1` returns median of a."
    (let
      [q "select p50(a) from stream window 1"
       context {"stream" (stream-data 8 (map #(identity {"a" %}) (range 100)))}
       result (rxb/into [] (mql/eval-mql q context))]
      (is  (= [{"P50(a)" 50}] result)))))

(deftest test-extrapolation
  (testing "Query with extrapolation should return approximate count and sum."
    (let
      [q "select COUNT(a), SUM(a) from stream window 1 sample {'strategy': 'RANDOM', 'threshold':5000, 'extrapolate': true}"
       context {"stream" (stream-data 1 (map (fn [_] {"a" 1}) (range 500)))}
       result (rxb/into []  (mql/eval-mql q context))
       cnt (.get (first result) "COUNT(a)")
       sum (.get (first result) "SUM(a)")]
       (is (< cnt 550))
       (is (> cnt 450))
       (is (< sum 550))
       (is (> sum 450)))))

(deftest select-literals
  (testing "Selecting literals in aggregate query functions correctly."
    (let
      [q "select COUNT(a), 'abc', 3 from stream window 1"
       context {"stream" (stream-data 10 [{"a" 1 "b" 1} {"a" 2 "b" 1} {"a" 3 "b" 1} {"a" 1 "b" 2} {"a" 2 "b" 2}])}
       result (rxb/into []  (mql/eval-mql q context))]
      (is (= [{"COUNT(a)" 5 "abc" "abc" "3" 3}] result)))))


(deftest test-as-clause
  (testing "select a constant with as clause renames it in client mode"
    (let
      [q "select 1 as 'test_id', 'b' as 'code', 3.14 as 'pi', a as 'prop', COUNT(a) as 'cnt' from stream window 1"
       context {"stream" (stream-data 10 [{"a" 1 "b" 1} {"a" 2 "b" 1} {"a" 3 "b" 1} {"a" 1 "b" 2} {"a" 2 "b" 2}])}
       result (rxb/into [] (mql/eval-mql q context "client"))]
      (is (= [{"test_id" 1, "code" "b", "pi" 3.14, "prop" 1, "cnt" 5}] result)))))


