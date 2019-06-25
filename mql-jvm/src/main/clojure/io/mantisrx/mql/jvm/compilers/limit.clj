(ns io.mantisrx.mql.jvm.compilers.limit)

(defn limit->fn
  "Transforms an integer/long n into a limit map.

   n: A whole number.

   Returns a map of {:limit n} for use with rx/take."
  [n]
  {:limit (n {})})
