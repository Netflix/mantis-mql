(ns io.mantisrx.mql.interfaces.browser
  (:require [io.mantisrx.mql.core :as mql]
            [io.mantisrx.mql.parser :as parser]
            [instaparse.core :as insta]))

(defn ^:export parser
  [query]
  (mql/parser query))

(defn ^:export parses
  [query]
  (parser/parses? query))

(defn ^:export get_reason
  [query]
  (parser/query->parse-error query))
