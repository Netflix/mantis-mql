(ns io.mantisrx.mql.util
  "Utility functions which do not quite belong in any other namespace."
  )

(defn strip-surround
  "Strips the surrounding elements of a string.
   Handy for string literals, regex
   .substring works in clj/cljs"
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
