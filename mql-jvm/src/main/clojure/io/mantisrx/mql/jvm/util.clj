(ns io.mantisrx.mql.jvm.util)

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
