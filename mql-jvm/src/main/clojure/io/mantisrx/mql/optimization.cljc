(ns io.mantisrx.mql.optimization
  "Query optimization functions, the objective
   of this namespace is to take a parse tree
   from the parser and produce a higher performing but equivalent tree.
   
   Note that the pred and apply functions execute at compile time for the query
   and thus should "
  (:require [clojure.zip :as zip]
            [clojure.string :as string]))

(defn- apply-rule
  [loc rule]
  (if (try ((:pred rule) (zip/node loc))
            (catch Exception e (identity false)))
       (zip/edit loc (:apply rule))
       loc))

(defn- regex-to-contains
  "Helper method for turning a regex into a string contains.
   Does not check for valid input"
  [regex]
  [:string_literal (str "'" (subs (second regex)
                         2
                         (- (count (second regex)) 2))
                        "'")])

;;;; Regex below defines a "word" for MQL optimization purposes.
;;;; This is any unit of text that we might consider an input token
;;;; to a regex which we might extract and use in other manners.
;;;; Allows alphanumeric, _, :, double quotes, -, \., \:, \", \/, \(, \)
;;;; This allows users to search JSON
(def word-regex
  #"(?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\"|\\/|\\\(|\\\)|\\\{|\\\})+"
  )

(defn unescape
  [x]
  (-> x
       (string/replace "\\:" ":")
       (string/replace "\\." ".")
       (string/replace "\\/" "/")
       (string/replace "\\(" "(")
       (string/replace "\\)" ")")
       (string/replace "\\\"" "\"")
       (string/replace "\\}" "}")
       (string/replace "\\{" "{")
       ))

(def optimization-rules
  [
   ;;;; Basic equals rule, converts foo ==~ /abcd/ into an equals call.
   {:pred (fn [node] (and (= :boolean_test (first node))
                          (= (first (nth node 2)) :REGEX_OPERATOR)
                          (re-matches
                            word-regex
                            (second (last node)))))
    :apply (fn [node] [:boolean_test
                       (second node)
                       [:BINARY_OPERATOR "=="]
                       (unescape (second (last node)))
                       ])}

   ;;;; Basic starts with rule, converts foo ==~ /abcd.*/ into a startswith call.
   {:pred (fn [node] (and (= :boolean_test (first node))
                         (= (first (nth node 2)) :REGEX_OPERATOR)
                         (re-matches
                           #"\^?(?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\"|\\/|\\\(|\\\)|\\\{|\\\})+\.\*$"
                           (second (last node)))))
   :apply (fn [node] [:boolean_test
                      (second node)
                      [:BINARY_OPERATOR "startsWith"]
                      (clojure.string/replace
                        (unescape (subs (second (last node)) 0 (- (count (second (last node))) 2)))
                        #"^\^"
                        "")]) 
   }

   ;;;; Basic contrains rule, converts foo ==~ /.*abcd.*/ into a contains call.
   {:pred (fn [node] (and (= :boolean_test (first node))
                          (= (first (nth node 2)) :REGEX_OPERATOR)
                          (re-matches
                            #"\.\*(?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\"|\\/|\\\(|\\\)|\\\{|\\\})+\.\*"
                            (second (last node)))))
    :apply (fn [node] [:boolean_test
                       (second node)
                       [:BINARY_OPERATOR "==+"]
                        (unescape (subs (second (last node)) 2 (- (count (second (last node))) 2)))
                        ])}

   ;;;; Multiple equals rule, converts foo ==~ /abc|def|ghi/ into multiple equals calls. 
   {:pred (fn [node] (and (= :boolean_test (first node))
                          (= (first (nth node 2)) :REGEX_OPERATOR)
                          (re-matches
                            #"(?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\"|\\/|\\\(|\\\)|\\\{|\\\})+(\|(?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\"|\\/|\\\(|\\\)|\\\{|\\\})+)*"
                                      (second (last node)))))
    :apply (fn [node] 
             (let
               [target (subs (second (last node)) 3 (- (count (second (last node))) 3))
                words (map unescape (string/split (second (last node)) #"\|"))
                ]
               [:boolean_test
                (second node)
                [:BINARY_OPERATOR "==*"]
                words])
             )}

   ;;;; Multiple contrains rule, converts foo ==~ /.*(abc|def).*/ into multiple contains calls.
   {:pred (fn [node] (and (= :boolean_test (first node))
                          (= (first (nth node 2)) :REGEX_OPERATOR)
                          (re-matches
                            #"\.\*\((?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\"|\\/|\\\(|\\\)|\\\{|\\\})+(\|(?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\"|\\/|\\\(|\\\)|\\\{|\\\})+)*\)\.\*"
                                      (second (last node)))))
    :apply (fn [node] 
             (let
               [target (subs (second (last node)) 3 (- (count (second (last node))) 3))
                words (map unescape (string/split target #"\|"))]
               [:boolean_test
                (second node)
                [:BINARY_OPERATOR "==+"]
                words])
             )}

   ;;;; Prefix multiple contrains rule, converts foo ==~ /.*(abc|def)ghi.*/ into multiple contains calls.
   {:pred (fn [node] (and (= :boolean_test (first node))
                          (= (first (nth node 2)) :REGEX_OPERATOR)
                          (re-matches
                            #"\.\*\((?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\"|\\/|\\\(|\\\)|\\\{|\\\})+(\|(?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\"|\\/|\\\(|\\\)|\\\{|\\\})+)+\)(?:[A-Za-z0-9_:\"-]|\\\.|\\:|\\\ |\\/|\\\(|\\\)|\\\{|\\\})+\.\*"
                                      (second (last node)))))
    :apply (fn [node] 
             (let
               [target (subs (second (last node)) 3 (- (count (second (last node))) 2))
                ;; TODO: Separate the tail, and then this will be the same.
                tail (last (string/split target #"\)"))
                words (map unescape (string/split (string/replace target (str ")" tail) "") #"\|"))]
               [:boolean_test
                (second node)
                [:BINARY_OPERATOR "==+"]
                (map #(str %1 tail) words)])
             )}
   ])

(defn- apply-rules
  [loc rules]
  (reduce apply-rule loc optimization-rules))

(defn run
  [loc]
  (if (zip/end? loc)
    (zip/root loc)
    (recur (zip/next (apply-rules loc optimization-rules)))))
