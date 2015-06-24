(ns clj-hazelcast.test.nicotest
      (:use [clojure.test])
  (:require [clj-hazelcast.core :as hz]
            [clj-hazelcast.mr :as mr]
            [clojure.tools.logging :as log])
  (:import (java.util.concurrent TimeoutException TimeUnit ExecutionException))
(:import
        (com.hazelcast.config XmlConfigBuilder Config TcpIpConfig)))


(comment


  (def localhost-config
    (let [config (.. (XmlConfigBuilder.) build)]
      (.. config getNetworkConfig getJoin getMulticastConfig
          (setEnabled false))
      (.. config getNetworkConfig getJoin getTcpIpConfig
          (setEnabled true)
          (addMember "127.0.0.1"))
      config))

  ; init connection to remote
  (hz/init localhost-config)

  ; local map to hold data
  (def test-map (atom nil))
  (reset! test-map (hz/get-map "clj-hazelcast.cluster-tests.test-map"))

  (def nico-map (atom nil))
  (reset! nico-map (hz/get-map "clj.nico"))

  ; put/get
  (hz/put! @test-map :foo 1)
  (:foo @test-map)
  (hz/put! @nico-map :hello 1)

  ; map reduce

  (mr/defmapper m1 [k _] [[k 1]])
  (mr/defreducer r1 [k v acc] (if (nil? acc) v (+ acc v)))

  (def mr-test-map (atom nil))
  (def wordcount-map (atom nil))
  (def validation-map (atom nil))
  (def combiner-map (atom nil))

  (reset! mr-test-map (hz/get-map "clj-hazelcast.cluster-tests.mr-test-map"))
  (reset! wordcount-map (hz/get-map "clj-hazelcast.cluster-tests.wordcount-map"))
  (reset! combiner-map (hz/get-map "clj-hazelcast.cluster-tests.combiner-map"))
  (reset! validation-map (hz/get-map "clj-hazelcast.cluster-tests.validation-map"))

  (hz/put! @mr-test-map :k1 "v1")
  (hz/put! @mr-test-map :k2 "v2")
  (hz/put! @mr-test-map :k3 "v3")
  (hz/put! @mr-test-map :k4 "v4")
  (hz/put! @mr-test-map :k5 "v5")

  (let [tracker (mr/make-job-tracker @hz/hazelcast)
        fut (mr/submit-job {:map @mr-test-map :mapper-fn m1 :reducer-fn r1 :tracker tracker})
        res (.get fut 2 TimeUnit/SECONDS)]
    (log/infof "Result %s" res))


  (hz/put! @combiner-map :sentence1 "the quick brown fox jumps over the lazy dog")
  (hz/put! @combiner-map :sentence2 "the fox and the hound the")
  (hz/put! @combiner-map :sentence3 "going all over the sea")

  (mr/defcollator col-fn [seq] (reduce + (vals seq)))
  (mr/defmapper mapper
                [k v]
                (let [words (re-seq #"\w+" v)]
                  (partition 2 (interleave words (take (count words) (repeatedly (fn [] 1)))))))

  (let [tracker (mr/make-job-tracker @hz/hazelcast)
        fut (mr/submit-job {:map @combiner-map :mapper-fn mapper :reducer-fn r1 :collator-fn col-fn :tracker tracker})
        res (.get fut 2 TimeUnit/SECONDS)]
    (log/infof "Result %s" res))

  ; shutdown the connection
  (hz/shutdown)

  )
