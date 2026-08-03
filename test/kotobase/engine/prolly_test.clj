(ns kotobase.engine.prolly-test
  (:require [clojure.test :refer [deftest is testing]]
            [arrangement.core :as arrangement]
            [kotobase.engine.conformance :as conformance]
            [kotobase.engine.contract :as engine]
            [kotobase.engine.memory :as memory]
            [kotobase.engine.prolly :as prolly]))

(defn- fixture []
  (let [blocks (atom {})]
    {:blocks blocks
     :engine (prolly/prolly-engine
              {:put! (fn [cid bytes] (swap! blocks assoc cid bytes))
               :get-fn #(get @blocks %)
               :blind-fn #(str "blind:" %)
               :encrypt-fn identity
               :decrypt-fn identity
               :digest-fn #(str "digest:" (hash %))})}))

(deftest shared-engine-conformance
  (is (:passed? (conformance/verify (:engine (fixture))))))

(deftest typed-values-and-object-store-restore
  (let [{:keys [engine blocks]} (fixture)
        s0 (engine/empty-state engine "typed/db")
        tx {:database-id "typed/db" :request-id "typed-1"
            :tx-data [[:db/add [:entity 42] :typed/value
                       {:enabled true :count 7 :tags #{:a :b}}]]}
        r1 (engine/transact engine s0 tx)
        root (get-in r1 [:receipt :physical-root])
        restored (engine/restore-state engine root)
        snapshot (engine/open-snapshot engine restored)
        rows (engine/scan engine snapshot [[:entity 42] :typed/value nil])]
    (testing "state is reconstructed from persisted blocks, not process state"
      (is (pos? (count @blocks)))
      (is (= [{:e [:entity 42]
               :a :typed/value
               :v {:enabled true :count 7 :tags #{:a :b}}
               :added true}]
             (mapv #(dissoc % :t) rows))))
    (testing "idempotency survives restore"
      (is (= :replayed
             (get-in (engine/transact engine restored tx)
                     [:receipt :status]))))))

(deftest differential-equivalence-with-memory-oracle
  (let [digest-fn #(str "digest:" (hash %))
        prolly-engine (:engine (fixture))
        memory-engine (memory/memory-engine digest-fn)
        database-id "differential/db"
        requests [{:database-id database-id :request-id "d1"
                   :tx-data [[:db/add 101 :person/name "Alice"]
                             [:db/add 101 :person/meta {:roles #{:admin :writer}}]]}
                  {:database-id database-id :request-id "d2"
                   :tx-data [[:db/retract 101 :person/name "Alice"]
                             [:db/add 101 :person/name "Alicia"]]}]
        run (fn [candidate]
              (reduce (fn [state request]
                        (:state (engine/transact candidate state request)))
                      (engine/empty-state candidate database-id)
                      requests))
        ps (run prolly-engine)
        ms (run memory-engine)]
    (doseq [selector [{} {:as-of 1} {:history true}]]
      (is (= (engine/scan memory-engine
                          (engine/open-snapshot memory-engine ms selector)
                          [nil nil nil])
             (engine/scan prolly-engine
                          (engine/open-snapshot prolly-engine ps selector)
                          [nil nil nil]))
          (str "selector " selector)))
    (is (= (:logical-checkpoint-root
            (engine/checkpoint memory-engine
                               (engine/open-snapshot memory-engine ms)))
           (:logical-checkpoint-root
            (engine/checkpoint prolly-engine
                               (engine/open-snapshot prolly-engine ps)))))))

(deftest assertion-write-is-path-incremental
  (let [blocks (atom {})
        writes (atom 0)
        put! (fn [cid bytes]
               (swap! writes inc)
               (swap! blocks assoc cid bytes))
        get-fn #(get @blocks %)
        blind #(str "blind:" %)
        eng (prolly/prolly-engine
             {:put! put! :get-fn get-fn :blind-fn blind
              :encrypt-fn identity :decrypt-fn identity
              :digest-fn #(str "digest:" (hash %))})
        database-id "delta/db"
        seed {:database-id database-id :request-id "seed"
              :tx-data (mapv (fn [n] [:db/add (str "e-" n) :value n])
                             (range 2000))}
        seeded (:state (engine/transact eng (engine/empty-state eng database-id)
                                        seed))
        previous (get-in seeded [:snapshots (:basis-t seeded)])
        _ (reset! writes 0)
        added (:state (engine/transact
                       eng seeded
                       {:database-id database-id :request-id "one-more"
                        :tx-data [[:db/add "new" :value 2001]]}))
        delta-writes @writes
        full-writes (atom 0)
        full-put! (fn [cid bytes]
                    (swap! full-writes inc)
                    (swap! blocks assoc cid bytes))]
    (arrangement/commit! full-put! (:db added) previous
                         arrangement/current-schema-version blind identity)
    (is (<= (* 3 delta-writes) @full-writes)
        (str "delta=" delta-writes " full=" @full-writes))))

(deftest mixed-assert-retract-write-is-path-incremental
  (let [blocks (atom {})
        writes (atom 0)
        put! (fn [cid bytes]
               (swap! writes inc)
               (swap! blocks assoc cid bytes))
        get-fn #(get @blocks %)
        blind #(str "blind:" %)
        eng (prolly/prolly-engine
             {:put! put! :get-fn get-fn :blind-fn blind
              :encrypt-fn identity :decrypt-fn identity
              :digest-fn #(str "digest:" (hash %))})
        database-id "mixed-delta/db"
        seed {:database-id database-id :request-id "seed"
              :tx-data (mapv (fn [n] [:db/add (str "e-" n) :value n])
                             (range 2000))}
        seeded (:state (engine/transact eng (engine/empty-state eng database-id)
                                        seed))
        previous (get-in seeded [:snapshots (:basis-t seeded)])
        _ (reset! writes 0)
        changed (:state
                 (engine/transact
                  eng seeded
                  {:database-id database-id :request-id "mixed"
                   :tx-data (into
                             (mapv (fn [n]
                                     [:db/retract (str "e-" n) :value n])
                                   (range 900 920))
                             (map (fn [n] [:db/add (str "e-" n) :value n])
                                  (range 2000 2020)))}))
        delta-writes @writes
        full-writes (atom 0)
        full-put! (fn [cid bytes]
                    (swap! full-writes inc)
                    (swap! blocks assoc cid bytes))]
    (arrangement/commit! full-put! (:db changed) previous
                         arrangement/current-schema-version blind identity)
    (is (contains? (:engine/capabilities (engine/engine-profile eng))
                   :incremental-retractions))
    (is (<= (* 4 delta-writes) (* 3 @full-writes))
        (str "mixed delta=" delta-writes " full=" @full-writes))))

(deftest final-operation-wins-within-one-transaction
  (let [{:keys [engine]} (fixture)
        state (engine/empty-state engine "last-op/db")
        result (engine/transact
                engine state
                {:database-id "last-op/db" :request-id "last-op"
                 :tx-data [[:db/add "e" :value 1]
                           [:db/retract "e" :value 1]
                           [:db/add "e" :value 1]]})
        snapshot (engine/open-snapshot engine (:state result))]
    (is (= [1] (mapv :v (engine/scan engine snapshot ["e" :value nil]))))))
