(ns run
  (:require [kotobase.engine.contract :as engine]
            [kotobase.engine.prolly.provider :as provider]
            [kotobase.storage.core :as storage]))

(defrecord AsyncStore [blocks refs]
  storage/IBlockStore
  (-put-blocks! [_ values]
    (doseq [{:keys [cid bytes]} values] (swap! blocks assoc cid bytes))
    (js/Promise.resolve nil))
  (-get-blocks [_ cids]
    (js/Promise.resolve
     (into {} (keep (fn [cid] (when-let [bytes (get @blocks cid)]
                                [cid bytes]))) cids)))
  storage/IRefStore
  (-read-ref [_ name]
    (js/Promise.resolve
     (when-let [cid (get @refs name)] {:cid cid :version cid})))
  (-compare-and-set-ref! [_ name expected next]
    (let [current (get @refs name)]
      (if (= expected current)
        (do (swap! refs assoc name next)
            (js/Promise.resolve {:published? true :current next}))
        (js/Promise.resolve {:published? false :current current}))))
  (-capabilities [_]
    (conj storage/required-capabilities :linearizable-ref)))

(def crypto
  {:blind-fn #(js/Promise.resolve (str "blind:" %))
   :encrypt-fn #(js/Promise.resolve %)
   :decrypt-fn #(js/Promise.resolve %)
   :digest-fn #(str "digest:" (hash %))})

(defn check [truth message]
  (when-not truth (throw (js/Error. message)))
  (println "ok -" message))

(defn- verify-read [backend]
  (let [reader (provider/engine-from-backend backend crypto)]
    (-> (provider/restore-head reader backend "main")
        (.then
         (fn [restored]
           (check (nil? (:db restored))
                  "reopen is manifest-only, not full hydrate")
           (engine/scan reader (engine/open-snapshot reader restored)
                        ["alice" :person/name nil])))
        (.then
         (fn [rows]
           (check (= ["Alice"] (mapv :v rows))
                  "direct async cursor answers point read")
           (let [requests (provider/request-count reader)
                 blocks (count @(:blocks backend))]
             (check (< requests 12) (str "point read requests=" requests))
             (check (>= blocks (* 10 requests))
                    (str "range pruning: " requests " reads over " blocks
                         " stored blocks"))))))))

(defn main []
  (let [backend (->AsyncStore (atom {}) (atom {}))
        writer (provider/engine-from-backend backend crypto)
        s0 (engine/empty-state writer "async/db")
        tx {:database-id "async/db" :request-id "a1"
            :tx-data (into [[:db/add "alice" :person/name "Alice"]]
                           (map (fn [n]
                                  [:db/add (str "entity-" n) :metric/value n]))
                           (range 2000))}]
    (-> (provider/transact-and-publish! writer backend "main" s0 tx)
        (.then (fn [published]
                 (check (= :published (:publish-status published))
                        "immutable blocks publish before CAS")
                 (verify-read backend)))
        (.then (fn [_] (println "kotobase-engine-prolly cljs: all green")))
        (.catch (fn [error]
                  (js/console.error error)
                  (js/process.exit 1))))))

(main)
