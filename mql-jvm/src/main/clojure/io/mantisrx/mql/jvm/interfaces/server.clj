(ns io.mantisrx.mql.jvm.interfaces.server
  (:require [io.mantisrx.mql.core :as mql]
            [io.mantisrx.mql.parser :as parser]
            [io.mantisrx.mql.jvm.interfaces.core :as mqli]
            [io.mantisrx.mql.transformers :as t]
            [io.mantisrx.mql.compilers.core.sampling :as sampling]
            [instaparse.core :as insta])
  (:import
    java.util.List
    java.util.HashMap
    java.util.Map
    io.mantisrx.mql.jvm.core.Query))

(defrecord
  MQLQuery
  [^:export id ^:export query ^:export projector ^:export subjects ^:export matcher ^:export sampler]
  Query
  (getSubscriptionId ^String [this] id)
  (getRawQuery ^String [this] query)
  (getSubjects ^java.util.List [this] subjects)
  (matches ^Boolean [this datum]  (and (sampler datum) (matcher datum)))
  (sample ^Boolean [this datum] (sampler datum))
  (project ^java.util.Map [this datum] (projector datum))
  Object
  (toString [this] (str "[" id "] " query)))

(defn- make-failure-projector
  [query reason-str id]
  (fn [_] (doto
            (new java.util.HashMap)
            (.put "query" query)
            (.put "error" reason-str)
            (.put "id" id))))

(defn- make-matcher
  [query failure?]
  (if failure?
    (fn [_] true)
    (comp boolean (mqli/query->pred query))))

(defn- make-projector
  [query failure? reason-str query-id]
  (if failure?
    (make-failure-projector query reason-str query-id)
    (mqli/query->projection query)))

(defn- make-sampler
  [query failure?]
  (if failure?
    (partial sampling/random-sampler sampling/default-factor 200)
    (mqli/query->sampler query)))

(defn make-query
  [^String query-id ^String query]
  (let
    [query (.trim query)
     parsed (mql/parser query)
     failure? (insta/failure? parsed)
     failure (insta/get-failure parsed)
     reason-str (parser/query->parse-error query)
     subjects (if failure?
                []
                (mqli/query->subjects query))
     matcher (make-matcher query failure?)
     projector (make-projector query failure? reason-str query-id)
     sampler (make-sampler query failure?)] 
    (MQLQuery. query-id query projector subjects matcher sampler)))

(defn ^:export makeQuery 
  [id query]
  (make-query id query))
