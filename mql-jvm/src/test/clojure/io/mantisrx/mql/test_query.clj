(ns io.mantisrx.mql.test-query
  "The objective of this namespace is to verify correct functionality with
   non-aggregate queries."
  (:require [clojure.test :refer :all]
            [io.mantisrx.mql.jvm.core :refer :all]
            [instaparse.core :as insta]
            [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.blocking :as rxb]
            )
  (:import rx.Observable java.util.concurrent.TimeUnit))


(deftest test-simple-query
  (testing "select * from stream returns everything."
    (let
      [q "select * from stream"
       context {"stream" (Observable/just {"a" 1} {"b" 2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" 1} {"b" 2}] result)))))

(deftest test-query-where-clause
  (testing "select * from stream where a == 1 filters out non-matching data."
    (let
      [q "select * from stream where a == 1"
       context {"stream" (Observable/just {"a" 1} {"b" 2} {"a" 2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" 1}] result))))
  (testing "`select * from stream where a ==~ /ab.*ab/` filters out non-matching data."
    (let
      [q "select * from stream where a ==~ /ab.*ab/"
       context {"stream" (Observable/just {"a" "ab123ab"} {"b" 2} {"a" "abba"})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" "ab123ab"}] result))))
  (testing "select * from stream where e['a']['b'] == 1"
    (let
      [q "select * from stream where e['a']['b'] == 1"
       context {"stream" (Observable/just {"a" {"b" 1}} {"a" {"b" 2}} {"a" {"b" 2}})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" {"b" 1}}] result))))
  (testing "select e['a']['b'] from stream where e['a']['b'] == 1"
    (let
      [q "select * from stream where e['a']['b'] == 1"
       context {"stream" (Observable/just {"a" {"b" 1}} {"b" 2} {"a" {"b" 2}})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" {"b" 1}}] result))))
  (testing "`select * from stream where a ==+ '123'` filters out non-matching data."
    (let
      [q "select * from stream where a ==+ '123'"
       context {"stream" (Observable/just {"a" "ab123ab"} {"b" 2} {"a" "abba"})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" "ab123ab"}] result)))))

  (deftest test-optimization-rules
    (testing "Queries transformed by the optimizer should function as intended."
      (testing "`select * from stream where a ==~ /abba/` filters out non-matching data.
                Note that this query is expected to be transformed by the optimizer."
        (let
          [q "select * from stream where a ==~ /abba/"
           context {"stream" (Observable/just {"a" "ab123ab"} {"b" 2} {"a" "abba"})}
           result (rxb/into [] (eval-mql q context))]
          (is (= [{"a" "abba"}] result))))

      (testing "`select * from stream where a ==~ /abba.*/` is optimized into a startsWith
                query which implements the proper predicate."
        (let
          [q "select * from stream where a ==~ /abba.*/"
           context {"stream" (Observable/just {"a" "abba1234"} {"b" 2} {"a" "abba"} {"a" "dabba"})}
           result (rxb/into [] (eval-mql q context))]
          (is (= [{"a" "abba1234"} {"a" "abba"}] result))))

      (testing "`select * from stream where a ==~ /^abba.*/` is optimized into a startsWith
                query which implements the proper predicate."
        (let
          [q "select * from stream where a ==~ /^abba.*/"
           context {"stream" (Observable/just {"a" "abba1234"} {"b" 2} {"a" "abba"} {"a" "dabba"})}
           result (rxb/into [] (eval-mql q context))]
          (is (= [{"a" "abba1234"} {"a" "abba"}] result))))

      (testing "`select * from stream where a ==~ /.*123.*/` filters out non-matching data.
                Note that this query is expected to be transformed by the optimizer."
        (let
          [q "select * from stream where a ==~ /.*123.*/"
           context {"stream" (Observable/just {"a" "ab123ab"} {"b" 2} {"a" "abba"})}
           result (rxb/into [] (eval-mql q context))]
          (is (= [{"a" "ab123ab"}] result))))
      (testing "`select * from stream where a ==~ /.*(abc|def).*/` filters out non-matching data.
                Note that this query is expected to be transformed by the optimizer."
        (let
          [q "select * from stream where a ==~ /.*(abc|def).*/"
           context {"stream" (Observable/just {"a" "abc123ab"} {"b" 2} {"a" "abba"})}
           result (rxb/into [] (eval-mql q context))]
          (is (= [{"a" "abc123ab"}] result))))
      (testing "`select * from stream where a ==~ /abc|def|ghi/` filters out non-matching data.
                Note that this query is expected to be transformed by the optimizer."
        (let
          [q "select * from stream where a ==~ /abc|def|ghi/"
           context {"stream" (Observable/just {"a" "ghi"} {"b" 2} {"a" "abba"} {"a" "xyz"} {"a" "abc"})}
           result (rxb/into [] (eval-mql q context))]
          (is (= [{"a" "ghi"} {"a" "abc"}] result))))
      (testing "`select * from stream where a ==~ /.*prop\\\"\\:\\\"123.*/` matches nested json.
                Note that this query is expected to be transformed by the optimizer."
        (let
          [q "select * from stream where a ==~ /.*prop\\\"\\:\\\"123.*/"
           context {"stream" (Observable/just {"a" "{\"prop\":\"123\"}"} {"b" 2} {"a" "abba"})}
           result (rxb/into [] (eval-mql q context))]
          (is (= [{"a" "{\"prop\":\"123\"}"}] result))))
      (testing "`select * from stream where a ==~ /^\\(focus\\)/` matches nested json.
                Note that this query is expected to be transformed by the optimizer."
        (let
          [q "select * from stream where a ==~ /^\\(focus\\).*/"
           context {"stream" (Observable/just {"a" "(focus)test"} {"b" 2} {"a" "abba"})}
           result (rxb/into [] (eval-mql q context))]
          (is (= [{"a" "(focus)test"}] result))))
      ))

  (deftest test-query-limit-clause
    (testing "select * from stream limit 1 returns one result."
      (let
        [q "select * from stream limit 1"
         context {"stream" (Observable/just {"a" 1} {"b" 2})}
         result (rxb/into [] (eval-mql q context))]
        (is (= [{"a" 1}] result)))))

  (deftest test-query-select-properties
    (testing "select a from stream returns only a in results."
      (let
        [q "select a from stream"
         context {"stream" (Observable/just {"a" 1 "b" 2 "c" 3})}
         result (rxb/into [] (eval-mql q context))]
        (is (= [{"a" 1}] result)))))

(deftest test-query-order-by
  (testing "select * from stream order by a"
    (let
      [q "select * from stream order by a"
       context {"stream" (Observable/just {"a" 1} {"a" 3} {"a" 2})}
       result (rxb/into []  (eval-mql q context))]
      (is (= [{"a" 1} {"a" 2} {"a" 3}] result))))
  (testing "select * from stream order by a desc reverses stream."
    (let
      [q "select * from stream order by a desc"
       context {"stream" (Observable/just {"a" 1} {"a" 3} {"a" 2})}
       result (rxb/into []  (eval-mql q context))]
      (is (= [{"a" 3} {"a" 2} {"a" 1}] result)))))

;;;;
;;;; Modes
;;;;

(deftest test-modes
  (testing "Full mode produces less results than client mode when sampling is enabled.
            Client mode should disable the sampling."
    (let
      [q "select * from stream SAMPLE {'strategy': 'RANDOM', 'threshold': 200}"
       context {"stream" (Observable/just {"a" 1} {"b" 2} {"c" 3} {"d" 4} {"e" 5} {"f" 6})}
       full-result (rxb/into [] (eval-mql q context "full"))
       client-result (rxb/into [] (eval-mql q context "client"))]
      (is (< (count full-result) (count client-result)))
      (is (= 6 (count client-result))))))

;;;;
;;;; WHERE true, WHERE false
;;;;

(deftest test-where-true-where-false
  (testing "select * from stream where true matches all data."
    (let
      [q "select * from stream where true"
       context {"stream" (Observable/just {"a" 1} {"b" 2} {"a" 2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" 1} {"b" 2} {"a" 2}] result))))
  (testing "select * from stream where false matches no data."
    (let
      [q "select * from stream where false"
       context {"stream" (Observable/just {"a" 1} {"b" 2} {"a" 2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [] result)))))

;;;;
;;;; Queries without a FROM clause
;;;;

(deftest test-query-without-from-clause
  (testing "select * where true matches all data."
    (let
      [q "select * where true"
       context {"stream" (Observable/just {"a" 1} {"b" 2} {"a" 2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" 1} {"b" 2} {"a" 2}] result))))
  (testing "select * where false matches no data."
    (let
      [q "select * where false"
       context {"stream" (Observable/just {"a" 1} {"b" 2} {"a" 2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [] result))))
  (testing "select * where a == 2 matches correct data."
    (let
      [q "select * where a == 2"
       context {"stream" (Observable/just {"a" 1} {"b" 2} {"a" 2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" 2}] result)))))

;;;;
;;;; Constants
;;;;

(deftest test-select-constants
  (testing "selecting a string constant"
    (let
      [q "select a, 'b' where a == 2"
       context {"stream" (Observable/just {"a" 1} {"b" 2} {"a" 2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" 2 "b" "b"}] result))))
  (testing "selecting a numeric constant"
    (let
      [q "select a, 2 where a == 2"
       context {"stream" (Observable/just {"a" 1} {"b" 2} {"a" 2})}
       result (rxb/into [] (eval-mql q context))]
      (is (= [{"a" 2 "2" 2}] result)))))

;;;;
;;;; As Clause
;;;;

(deftest test-as-clause
  (testing "select a constant with as clause renames it in client mode"
    (let
      [q "select 1 as 'test_id', 'b' as 'code', 3.14 as 'pi', a as 'prop' from stream"
       context {"stream" (Observable/just {"a" 1})}
       result (rxb/into [] (eval-mql q context "client"))]
      (is (= [{"test_id" 1, "code" "b", "pi" 3.14, "prop" 1}] result)))))

;;;;
;;;; Spaces in Properties
;;;;

(deftest test-spaces-in-property-names
  (testing "selecting a space in a property name"
    (let
      [q "select a, e['c '] where e['c '] == 2"
       context {"stream" (Observable/just {"a" 1, "c " 2} {"b" 2} {"a" 2 "c " 3})}
       result (rxb/into [] (eval-mql q context))]
      (is  (= [{"a" 1 "c " 2}] result)))))
