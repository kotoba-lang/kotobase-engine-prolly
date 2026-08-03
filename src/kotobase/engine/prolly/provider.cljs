(ns kotobase.engine.prolly.provider
  "ClojureScript/R2 coordinator. Blocks are constructed into a synchronous
  local batch, uploaded durably, and only then exposed by CAS. Restore uses an
  explicit missing-block retry bridge until Arrangement gains an async cursor."
  (:require [kotobase.engine.contract :as engine]
            [kotobase.engine.prolly :as prolly]
            [kotobase.engine.prolly.cursor :as cursor]
            [kotobase.storage.core :as storage]))

(defn- runtime-of [eng]
  (or (:kotobase.provider/runtime (meta eng))
      (throw (ex-info "engine was not created by engine-from-backend"
                      {:type :kotobase.engine/missing-provider-runtime}))))

(defn engine-from-backend
  "Create a Prolly engine whose block writes are buffered for one atomic
  publish attempt. CRYPTO functions keep the normal Promise-based CLJS shape."
  [backend crypto]
  (storage/validate-block-store! backend)
  (let [cache (atom {})
        pending (atom {})
        requests (atom 0)
        put! (fn [cid bytes]
               (swap! cache assoc cid bytes)
               (swap! pending assoc cid bytes)
               nil)
        get-fn (fn [cid]
                 (if-some [bytes (get @cache cid)]
                   bytes
                   (throw (ex-info "block is not in the synchronous read cache"
                                   {:type :kotobase.engine/missing-block
                                    :missing-cid cid}))))
        async-get-fn
        (fn [cid]
          (swap! requests inc)
          (-> (storage/-get-blocks backend [cid])
              (.then #(get % cid))))
        scan-snapshot-fn
        (fn [snapshot-root history basis-t pattern opts]
          (cursor/scan async-get-fn (:blind-fn crypto) (:decrypt-fn crypto)
                       snapshot-root history basis-t pattern opts))
        eng (prolly/prolly-engine
             (merge {:put! put! :get-fn get-fn
                     :scan-snapshot-fn scan-snapshot-fn}
                    crypto))]
    (with-meta eng
      {:kotobase.provider/runtime
       {:backend backend :cache cache :pending pending :requests requests
        :async-get-fn async-get-fn}})))

(defn request-count [eng]
  @(-> eng runtime-of :requests))

(defn- flush-pending! [eng]
  (let [{:keys [backend pending]} (runtime-of eng)
        blocks (mapv (fn [[cid bytes]] {:cid cid :bytes bytes}) @pending)]
    (-> (if (seq blocks)
          (storage/-put-blocks! backend blocks)
          (js/Promise.resolve nil))
        (.then (fn [result]
                 (reset! pending {})
                 result)))))

(defn restore-head [eng backend ref-name]
  (storage/validate-backend! backend)
  (-> (storage/-read-ref backend ref-name)
      (.then (fn [head]
               (if head
                 (let [{:keys [cache async-get-fn]} (runtime-of eng)
                       cid (:cid head)]
                   (-> (async-get-fn cid)
                       (.then (fn [bytes]
                                (when-not bytes
                                  (throw (ex-info "published manifest is missing"
                                                  {:type :kotobase.engine/block-not-found
                                                   :cid cid})))
                                (swap! cache assoc cid bytes)
                                (engine/restore-state eng cid {:lazy? true})))))
                 nil)))))

(defn transact-and-publish!
  "Upload all immutable blocks before conditional head publication. A CAS
  loser returns :conflict and never rewrites the winner."
  [eng backend ref-name state request]
  (storage/validate-backend! backend)
  (let [expected (:physical-root state)]
    (-> (engine/transact eng state request)
        (.then (fn [result]
                 (-> (flush-pending! eng)
                     (.then (fn [_]
                              (let [next-root (get-in result
                                                      [:receipt :physical-root])]
                                (-> (storage/-compare-and-set-ref!
                                     backend ref-name expected next-root)
                                    (.then
                                     (fn [publication]
                                       (if (:published? publication)
                                         (assoc result
                                                :publication publication
                                                :publish-status :published)
                                         {:publish-status :conflict
                                          :publication publication
                                          :expected-root expected
                                          :candidate-root next-root
                                          :winner-root (:current publication)})))))))))))))
