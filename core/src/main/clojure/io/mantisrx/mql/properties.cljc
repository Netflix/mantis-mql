(ns io.mantisrx.mql.properties
  (:refer-clojure :exclude [get get-in])
  (:import (clojure.lang RT)
           (java.util ArrayList List RandomAccess)))

(defn get
  "Get function with more robust implementation to the types of data
   which MQL encounters. JSON decoding is primarily done into
   Map<String, Object> in Java so the primary data types are Map and
   List.
   
   m: The associative strucutre or array from which to fetch the value.
   k: The key or integer index associated with the desired value.
   not-found: The value to return if k is not associated in m.

   - All of Clojure's get functionality.
   - Operates on ArrayList and other random access lists.
   - Implements negative indexing for arrays.

   Returns the value in m associated with k or not-found."
  ([m k]
   (get m k nil))
  ([m k not-found]
   (if (and (every? #(instance? % m) [List RandomAccess]) (integer? k))
     (let [^List m m
           size (.size m)
           k (int (if (neg? k) (+ k size) k))]
       (if (and (<= 0 k) (< k size))
         (.get m k)
         not-found))
     (RT/get m k not-found))))

(defn ^:private ^:static
  reduce1
       ([f coll]
             (let [s (seq coll)]
               (if s
         (reduce1 f (first s) (next s))
                 (f))))
       ([f val coll]
          (let [s (seq coll)]
            (if s
              (if (chunked-seq? s)
                (recur f
                       (.reduce (chunk-first s) f val)
                       (chunk-next s))
                (recur f (f val (first s)) (next s)))
         val))))

(defn get-in
  "A re-implementation of Clojure's get-in using mql-get."
  {:static true}
  ([m ks]
     (reduce get m ks))
  ([m ks not-found]
     (loop [sentinel (Object.)
            m m
            ks (seq ks)]
       (if ks
         (let [m (get m (first ks) sentinel)]
           (if (identical? sentinel m)
             not-found
             (recur sentinel m (next ks))))
         m))))
