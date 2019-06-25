(ns io.mantisrx.mql.examples
  (:require [io.mantisrx.mql.core :as mql]
            [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.interop :as rxi]
            [io.mantisrx.mql.transformers :as t]
            [io.mantisrx.mql.properties :as mqlp]
            [io.mantisrx.mql.interfaces.browser :as browser])
  (:import rx.Observable java.util.concurrent.TimeUnit))

(def nodes ["i-abcdef" "i-ghijkl" "i-mnopqr" "i-stuvwx"])
(def clusters ["api-test" "api-prod"])
(defn random-servo [nodes] {"node" (nodes (rand-int (count nodes)))
                            "cluster" (clusters (rand-int (count clusters)))
                            "app" "api"
                            "metrics" {"systemLoadAverage" (rand 16.0)
                                       "latency" (+ 250.0 (rand 250.0))
                                       }})

(def test-obs (Observable/from ^Iterable (repeatedly random-servo)))
(def inlier-servo (rx/map (fn [_] (random-servo nodes)) (Observable/interval 500 TimeUnit/MILLISECONDS)))
(def outlier-servo (rx/map (fn [_] {"node" "i-outlier"
                                    "cluster" "api-prod"
                                    "app" "api"
                                    "metrics" {"systemLoadAverage" (rand 16.0)
                                               "latency" (+ 500.0 (rand 1000.0))}
                                    } ) (Observable/interval 500 TimeUnit/MILLISECONDS)))
(def servo (rx/merge inlier-servo outlier-servo))
(def context {"servo" servo
              "stream" (rx/take 20 inlier-servo)})

; Outlier Query
; Aggregate, nested property, windowing, group by, regex, order by
(def outlier-query "select MAX(e[\"metrics\"][\"latency\"]), node, cluster from servo window 10 2 where app == \"api\" AND cluster ==~ /.*\\-prod/ group by cluster, node order by node")
(def outlier-query2 "select MAX(e[\"metrics\"][\"latency\"]), node, cluster from servo window 10 2 where app == \"api\" AND cluster ==~ /.*\\-prod/ group by cluster, node")

; Latency Query
(def latency-query "select e[\"metrics\"][\"latency\"], node from servo window 10 order by e[\"metrics\"][\"latency\"]")

; Sampler Query
(def sampler-query "select * from servo SAMPLE {\"strategy\": \"RANDOM\", \"threshold\": 1000}")

; Sampler Limit Query
(def sampler-limit-query "select * from servo LIMIT 10 SAMPLE {\"strategy\": \"RANDOM\", \"threshold\": 500}")

; Event Time Query
(def event-time-outlier-query "select MAX(e[\"metrics\"][\"latency\"]), node, cluster, tick() from servo window 10 2 where app == \"api\" AND cluster ==~ /.*\\-prod/ group by cluster, node order by node")

; Order By Test
(def order-by-query "select MAX(e[\"metrics\"][\"latency\"]), node, cluster, tick() from servo window 10 where app == \"api\" AND cluster ==~ /.*\\-prod/ group by cluster, node order by node")

(def top-n "select count(node), node from servo window 5 group by node order by e['COUNT(node)'] DESC limit 2")
