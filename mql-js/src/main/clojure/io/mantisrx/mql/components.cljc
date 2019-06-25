(ns io.mantisrx.mql.components
  (:require [io.mantisrx.mql.core :as mql]
            [io.mantisrx.mql.transformers :as t]
            [instaparse.core :as insta]
            #?(:cljs [cljs.nodejs :as nodejs])
            )
  #?(:clj
     (:import
       java.util.List
       java.util.HashMap
       java.util.Map
       io.mantisrx.mql.core.Query)))

#?(:cljs
   (defprotocol
     Query
     (getSubscriptionId [this])
     (getRawQuery [this])
     (getSubjects [this])
     (matches [this ^java.util.Map datum])
     (sample [this ^java.util.Map datum])
     (project [this ^java.util.Map datum]))) 

(defrecord
  MQLQuery
  [^:export id ^:export query ^:export projector ^:export subjects ^:export matcher ^:export sampler]
  Query
  (getSubscriptionId ^String [this] id)
  (getRawQuery ^String [this] query)
  (getSubjects ^java.util.List [this] subjects)
  (matches ^Boolean [this datum] #?(:clj (and (sampler datum) (matcher datum))
                                    :cljs (let
                                            [datum (js->clj datum)]
                                            (and (sampler datum) (matcher datum)))))
  (sample ^Boolean [this datum] (sampler #?(:clj datum :cljs (js->clj datum))))
  (project ^java.util.Map [this datum] #?(:clj
                                          (projector datum)
                                          :cljs
                                          (clj->js (projector
                                                     (js->clj datum)))))
  Object
  (toString [this] (str "[" id "] " query)))

(defn- make-failure-projector
  [query reason-str id]
  #?(:clj (fn [_] (doto
                    (new java.util.HashMap)
                    (.put "query" query)
                    (.put "error" reason-str)
                    (.put "id" id)))
     :cljs (fn [_] {"query" query
                    "error" reason-str
                    "id" id})))

(defn- make-matcher
  [query failure?]
  (if failure?
    (fn [_] true)
    (comp boolean (mql/query->pred query))))

(defn- make-projector
  [query failure? reason-str query-id]
  (if failure?
    (make-failure-projector query reason-str query-id)
    (mql/query->projection query)))

(defn- make-sampler
  [query failure?]
  (if failure?
    (partial t/random-sampler t/default-factor 200)
    (mql/query->sampler query)))

(defn make-query
  [^String query-id ^String query]
  (let
    [query (.trim query)
     parsed (mql/parser query)
     failure? (insta/failure? parsed)
     failure (insta/get-failure parsed)
     reason-str (mql/failure->reason-str failure)
     subjects (if failure?
                []
                (mql/query->subjects query))
     matcher (make-matcher query failure?)
     projector (make-projector query failure? reason-str query-id)
     sampler (make-sampler query failure?)] 
    (MQLQuery. query-id query projector subjects matcher sampler)))

(defn ^:export makeQuery 
  [id query]
  (make-query id query))

; Superset Projection

#?(:cljs (defn ^:export makeSupersetProjector
  [& queries]
  (let
    [projector (mql/queries->superset-projection queries)]
    (fn [datum]
      (clj->js (projector (js->clj datum)))))))

#?(:cljs (def ^:export makeSupersetProjectorMemoized (memoize makeSupersetProjector)))
