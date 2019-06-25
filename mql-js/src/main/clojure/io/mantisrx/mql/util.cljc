(ns io.mantisrx.mql.util
  "Utility functions which do not quite belong in any other namespace."
  #?(:clj (:import java.util.TreeMap)))

(defn strip-surround
  "Strips the surrounding elements of a string.
   Handy for string literals, regex"
  [^String literal]
  (.substring literal 1 (dec (count literal))))

(defn extract-clause
  "Extracts a sequence of a specific clause type from the provided parse tree."
  [clause tree]
  (filter #(= (first %) clause) (tree-seq vector? rest tree)))

(defn
  ops->clause-map
  [ops]
  (into {} (map (fn [op] [(get (meta op) :clause :unknown) op]) ops)))

(defn map-assoc-in!
  "Associates a value in a nested TreeMap using mutable behavior.
   This function mimics the behavior of assoc-in and will create
   new nested TreeMaps as necessary if nested levels do not exist."
  {:static true}
  [m [k & ks] v]
  (if ks
    (.put ^java.util.Map m k (map-assoc-in! (get m k (java.util.TreeMap.)) ks v))
    (.put ^java.util.Map m k v))
  m)

(defn proper-superset?
  "A predicate determining if xs is a proper superset of ys.
   Set theory dictates that a proper superset must not equal the set to which it compared.
   Note that this function works on seqs, we're intentionally considering order."
  [xs ys]
  (and
    (> (count xs) (count ys))
    (= (seq (take (count ys) xs)) (seq ys))))

(defn
  simplify-select-list
  "Remove all elements in xs which have a proper subset in xs."
  [xs]
  (filter
    (complement (fn [x] (some #(proper-superset? x %) (filter (complement empty?) xs))))
    xs))
