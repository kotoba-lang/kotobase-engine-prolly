(ns kotobase.engine.prolly
  "Typed Datom engine over string-valued Arrangement/Prolly primitives."
  (:require [clojure.edn :as edn]
            [arrangement.core :as arrangement]
            [arrangement.query :as query]
            [ipld.core :as ipld]
            [kotobase.engine.canonical :as canonical]
            [kotobase.engine.completion :as completion]
            [kotobase.engine.contract :as contract]
            [kotobase.engine.profile :as profile]))

(def engine-format-version 1)

(def prolly-profile
  (-> profile/prolly
      (assoc :engine/implementation :kotobase.engine/prolly)
      (update :engine/capabilities disj :structural-diff)
      (update :engine/capabilities conj :typed-edn :durable-manifest
              :incremental-assertions :incremental-retractions
              :mixed-delta-commit)))

(defn- digest [engine x]
  ((:digest-fn engine) (canonical/canonical-string x)))

(defn encode-component [x]
  (canonical/canonical-string x))

(defn decode-component [x]
  (canonical/restore-canonical-value (edn/read-string x)))

(defn- logical->quad [{:keys [e a v]}]
  {:s (encode-component e)
   :p (encode-component a)
   :o (encode-component v)})

(defn- quad->logical [{:keys [s p o]}]
  {:e (decode-component s)
   :a (decode-component p)
   :v (decode-component o)
   :added true})

(defn- apply-datom [db {:keys [op] :as datom}]
  (let [quad (logical->quad datom)]
    (case op
      :assert (arrangement/assert-quad db quad)
      :retract (arrangement/retract-quad db quad))))

(defn- live-transaction-coordinates [history basis-t]
  (->> history
       (filter #(<= (:t %) basis-t))
       (reduce (fn [coordinates datom]
                 (let [k (canonical/logical-datom-key datom)]
                   (if (:added datom)
                     (assoc coordinates k (:t datom))
                     (dissoc coordinates k))))
               {})))

(defn- visible-rows
  ([db history basis-t]
   (visible-rows db history basis-t [nil nil nil]))
  ([db history basis-t pattern]
   (let [encoded-pattern (mapv #(when (some? %) (encode-component %)) pattern)
         coordinates (live-transaction-coordinates history basis-t)]
     (->> (query/query db encoded-pattern (constantly true))
       (map quad->logical)
       (map #(assoc % :t (get coordinates (canonical/logical-datom-key %))))
       canonical/canonical-datoms))))

(defn- matches? [[pe pa pv] {:keys [e a v]}]
  (and (or (nil? pe) (= pe e))
       (or (nil? pa) (= pa a))
       (or (nil? pv) (= pv v))))

(defn- manifest-node [state snapshot-root current-request]
  {"engine" "kotobase-engine-prolly"
   "format-version" engine-format-version
   "database-id" (:database-id state)
   "basis-t" (:basis-t state)
   "snapshot" (some-> snapshot-root ipld/link)
   "history-edn" (pr-str (:history state))
   "requests-edn" (pr-str (:requests state))
   "snapshots-edn" (pr-str (:snapshots state))
   "manifests-edn" (pr-str (:manifests state))
   "current-request-edn" (pr-str current-request)})

(defn- read-edn-field [node field]
  (edn/read-string (get node field)))

(defn- validate-manifest [node physical-root]
  (when-not (and (= "kotobase-engine-prolly" (get node "engine"))
                 (= engine-format-version (get node "format-version")))
    (throw (ex-info "unsupported Prolly engine manifest"
                    {:type :kotobase.engine/unsupported-manifest
                     :physical-root physical-root
                     :format (get node "format-version")})))
  node)

(defrecord ProllyEngine [put! get-fn blind-fn encrypt-fn decrypt-fn digest-fn
                         scan-snapshot-fn]
  contract/IEngine
  (-engine-profile [_] prolly-profile)

  (-empty-state [_ {:keys [database-id]}]
    {:database-id database-id
     :basis-t 0
     :db (arrangement/empty-db)
     :history []
     :requests {}
     :snapshots {}
     :manifests {}})

  (-restore-state [_ physical-root opts]
    (let [node (validate-manifest (ipld/decode (get-fn physical-root))
                                  physical-root)
          database-id (get node "database-id")
          basis-t (get node "basis-t")
          snapshot-root (some-> (get node "snapshot") ipld/link-cid)
          history (read-edn-field node "history-edn")
          prior-requests (read-edn-field node "requests-edn")
          snapshots (assoc (read-edn-field node "snapshots-edn")
                           basis-t snapshot-root)
          manifests (assoc (read-edn-field node "manifests-edn")
                           basis-t physical-root)
          current-request (read-edn-field node "current-request-edn")
          requests (cond-> prior-requests
                     current-request
                     (assoc (:request-id current-request)
                            (assoc current-request :physical-root physical-root)))
          finish (fn [db]
                   {:database-id database-id :basis-t basis-t :db db
                    :history history :requests requests :snapshots snapshots
                    :manifests manifests :physical-root physical-root})]
      (if (:lazy? opts)
        (finish nil)
        (completion/then-result
         (arrangement/restore get-fn snapshot-root decrypt-fn)
         finish))))

  (-transact [this state {:keys [database-id request-id tx-data]}]
    (when-not (= database-id (:database-id state))
      (throw (ex-info "transaction database does not match state"
                      {:type :kotobase.engine/database-mismatch})))
    (let [tx (canonical/normalize-tx tx-data)
          tx-root (digest this tx)]
      (if-let [prior (get-in state [:requests request-id])]
        (if (= tx-root (:tx-root prior))
          {:state state
           :receipt {:database-id database-id
                     :epoch (:epoch prior)
                     :request-id request-id
                     :tx-root tx-root
                     :physical-root (:physical-root prior)
                     :engine prolly-profile
                     :status :replayed}}
          (throw (ex-info "request-id was already used for another transaction"
                          {:type :kotobase.engine/idempotency-conflict
                           :request-id request-id})))
        (let [t (inc (:basis-t state))
              db (reduce apply-datom (:db state) tx)
              previous-snapshot (get-in state [:snapshots (:basis-t state)])
              final-operations (->> tx
                                    (reduce (fn [ops datom]
                                              (assoc ops
                                                     (canonical/logical-datom-key datom)
                                                     datom))
                                            {})
                                    vals
                                    (sort-by canonical/canonical-string)
                                    vec)
              assertions (->> final-operations
                              (filter #(= :assert (:op %)))
                              (mapv logical->quad))
              retractions (->> final-operations
                               (filter #(= :retract (:op %)))
                               (mapv logical->quad))
              appended (mapv (fn [{:keys [e a v op]}]
                               {:e e :a a :v v :t t :added (= :assert op)})
                             tx)]
          (completion/then-result
           (arrangement/commit-changes!
            put! get-fn previous-snapshot
            {:assertions assertions :retractions retractions}
            arrangement/current-schema-version blind-fn encrypt-fn)
           (fn [snapshot-root]
             (let [next-state (-> state
                                  (assoc :basis-t t :db db)
                                  (update :history into appended)
                                  (assoc-in [:snapshots t] snapshot-root))
                   current-request {:request-id request-id :tx-root tx-root :epoch t}
                   physical-root (ipld/put-node!
                                  put! (manifest-node next-state snapshot-root
                                                      current-request))
                   request-record (assoc current-request :physical-root physical-root)
                   final-state (-> next-state
                                   (assoc :physical-root physical-root)
                                   (assoc-in [:manifests t] physical-root)
                                   (assoc-in [:requests request-id] request-record))]
               {:state final-state
                :receipt {:database-id database-id :epoch t
                          :request-id request-id :tx-root tx-root
                          :physical-root physical-root :engine prolly-profile
                          :status :committed}})))))))

  (-open-snapshot [_ state selector]
    (let [basis (:basis-t state)
          as-of (get selector :as-of basis)]
      (when-not (and (integer? as-of) (<= 0 as-of basis))
        (throw (ex-info "snapshot :as-of is outside the database basis"
                        {:type :kotobase.engine/invalid-snapshot-selector
                         :selector selector :basis basis})))
      (if (zero? as-of)
        {:database-id (:database-id state) :basis-t 0
         :db (arrangement/empty-db) :history (:history state)
         :history? (true? (:history selector))}
        (let [snapshot-root (get-in state [:snapshots as-of])
              finish (fn [db]
                       {:database-id (:database-id state) :basis-t as-of :db db
                        :snapshot-root snapshot-root
                        :history (:history state)
                        :history? (true? (:history selector))
                        :physical-root (get-in state [:manifests as-of])})]
          (if (and (nil? (:db state)) scan-snapshot-fn)
            (finish nil)
            (completion/then-result
             (arrangement/restore get-fn snapshot-root decrypt-fn)
             finish))))))

  (-scan [_ {:keys [db snapshot-root basis-t history history?]} pattern opts]
    (if history?
      (->> history
           (filter #(<= (:t %) basis-t))
           canonical/canonical-datoms
           (filter #(matches? pattern %))
           vec)
      (if db
        (->> (visible-rows db history basis-t pattern)
             (filter #(matches? pattern %)) vec)
        (if scan-snapshot-fn
          (scan-snapshot-fn snapshot-root history basis-t pattern opts)
          (throw (ex-info "snapshot is lazy but no async cursor was injected"
                          {:type :kotobase.engine/missing-snapshot-cursor}))))))

  (-history [_ {:keys [history basis-t]} _opts]
    (->> history
         (filter #(<= (:t %) basis-t))
         canonical/canonical-datoms))

  (-checkpoint [this {:keys [database-id basis-t db history physical-root]
                      :as snapshot} _opts]
    (completion/then-result
     (if db
       (visible-rows db history basis-t)
       (contract/-scan this snapshot [nil nil nil] {}))
     (fn [rows]
       {:database-id database-id
        :epoch basis-t
        :logical-checkpoint-root
        (digest this (canonical/checkpoint-datoms rows))
        :physical-root physical-root
        :engine prolly-profile}))))

(defn prolly-engine
  [{:keys [put! get-fn blind-fn encrypt-fn decrypt-fn digest-fn
           scan-snapshot-fn] :as opts}]
  (doseq [[k f] [[:put! put!] [:get-fn get-fn] [:blind-fn blind-fn]
                 [:encrypt-fn encrypt-fn] [:decrypt-fn decrypt-fn]
                 [:digest-fn digest-fn]]]
    (when-not (ifn? f)
      (throw (ex-info "Prolly engine requires injected capabilities"
                      {:type :kotobase.engine/missing-capability
                       :capability k :provided (keys opts)}))))
  (->ProllyEngine put! get-fn blind-fn encrypt-fn decrypt-fn digest-fn
                  scan-snapshot-fn))
