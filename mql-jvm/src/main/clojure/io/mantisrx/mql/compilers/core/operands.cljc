(ns io.mantisrx.mql.compilers.core.operands
  (:require [io.mantisrx.mql.properties :as mqlp])
  (:import java.util.Map)
  )

(defn sw-property->fn
  [prop]
  (with-meta
    (fn [datum]
      (let
        [ks (filter (fn [^String k] (.startsWith k prop)) (keys datum))]
        (map (fn [k] {:name [k] :value (mqlp/get-in datum [k])}) ks)))
    {:type :starts-with :name nil}))

(defn literal->fn
  [parse-fn literal & names]
  (let [l (parse-fn literal)]
    (with-meta (fn [_] l) {:name [(str l)]})))

(defn literal->fn-with-as
  [parse-fn literal & names]
  (let [l (parse-fn literal)]
    (with-meta (fn [_] l) {:name (if (empty? names)
                                   [(str l)]
                                   (map str names)
                                   )})))  

(defn property->fn
  "Converts a parsed property into a function which will attempt to extract
   said property from a nested associative structure.
   Attaches a property name as metadata.
   
   TODO: As clause is a little more complicated here because of the blob
   parameters. How to tell the last one from 'as' could add separate parse rule."
  [& props]
  (with-meta 
    #?(:clj (fn [obj] (mqlp/get-in obj props))
       :cljs (fn [obj] (get-in obj props nil)))
    {:name props}))

(defn property-with-as->fn
  [prop as-name]
  (with-meta prop {:name [as-name]}))

(defn distinct-operand->property
  [prop]
  (with-meta
    prop
    (merge
      (meta prop)
      {:distinct true})))

#?(:clj (defn is-associative?
          [x]
          (and
            (not (vector? x))
            (or
              (instance? Map x)
              (associative? x)))))

(defn star-property->projection
  [& props]
  (let
    [components (split-with (complement #{"*"}) props)
     prefix (first components)]
    (apply property->fn prefix)))

#?(:clj (defn star-property->fn
          "Returns a seq of {:name 'a' :value 14} pairs for all properties matching the
           star property's name.
           Discern between lists and maps, and act accordingly the list? below doesn't do it.
           Also ensure this functions when suffix is empty."
          [& props]
          (let
            [components (split-with (complement #{"*"}) props)
             prefix (first components)
             suffix (rest (second components))]
            (fn [datum]
              (let
                [root-obj (mqlp/get-in datum prefix)]
                (cond
                  (isa? java.util.LinkedHashMap (type root-obj))
                  (map
                    (fn [entry]
                      {:name (concat prefix [(key entry)] suffix)
                       :value (mqlp/get-in (val entry) suffix)})
                    root-obj)
                  (is-associative? root-obj)
                  (map
                    (fn [key+value]
                      {:name (concat prefix [(first key+value)] suffix)
                       :value (mqlp/get-in (second key+value) suffix)})
                    root-obj)
                  :else
                  (map-indexed
                    (fn [index element]
                      {:name (concat prefix [index] suffix)
                       :value (mqlp/get-in element suffix)})
                    root-obj)))))))

(defn tick->operand
  [tick]
  (with-meta (fn [_]
               #?(:clj (System/currentTimeMillis)
                  :cljs (.getTime (js/Date.))))
             {:name ["tick"]}))


(defn not-operand->operand
  "Complements an operand as a function or a boolean.

   op: A fn to be complimented or a realized value as a boolean.

   Returns the compliment of the operand as an operand."
  [op]
  (if (fn? op) (complement op) (not op)))
