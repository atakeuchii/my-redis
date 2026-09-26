(ns my-redis.db
  (:refer-clojure :exclude [keys]))

(def wrong-type
  ::wrong-type)

(defn now
  []
  (System/currentTimeMillis))

(defn create 
  []
  (atom {:data {} :expires #{}}))

(defn- data [db] (:data @db))

(defn expired? [entry ^long now]
  (boolean (when-let [exp (:expire-at entry)]
             (>= now (long exp)))))

(defn- reap! [db k]
  (let [e (get (data db) k)]
    (cond
      (nil? e) nil
      (expired? e (now)) (do (swap! db (fn [s]
                                         (-> s
                                             (update :data dissoc k)
                                             (update :expires disj k))))
                             nil)
      :else e)))

(defn entry [type value]
  {:type type :value value})

(defn get-entry [db k] (reap! db k))

(defn get-value [db k] (:value (get-entry db k)))

(defn exists? [db k] (some? (get-entry db k)))

(defn key-type [db k] (:type (get-entry db k)))

(defn keys [db]
  (let [t (now)]
    (into []
          (comp (remove (fn [[_ e]] (expired? e t)))
                (map first))
          (data db))))

(defn size [db]
  (let [t (now)]
    (count (remove #(expired? % t) (vals (data db))))))


(defn snapshot [db]
  (let [t (now)]
    (into {} (remove (fn [[_ e]] (expired? e t))) (data db))))

(defn entry-in [snapshot k] (get snapshot k))

(defn- reindex-expires [expires k entry]
  (if (:expire-at entry)
    (conj expires k)
    (disj expires k)))

(defn set-entry! [db k entry]
  (swap! db (fn [s]
              (-> s
                  (assoc-in [:data k] entry)
                  (update :expires reindex-expires k entry))))
  nil)

(defn set-value! [db k type value]
  (set-entry! db k (entry type value))
  nil)

(defn set-entries! [db entries]
  (swap! db (fn [s]
              (-> s
                  (update :data merge entries)
                  (update :expires #(reduce-kv reindex-expires % entries)))))
  nil)

(defn delete! [db k]
  (let [[old _] (swap-vals! db (fn [s]
                                 (-> s
                                     (update :data dissoc k)
                                     (update :expires disj k))))]
    (contains? (:data old) k)))

(defn delete-many! [db ks]
  (let [uniq (distinct ks)
        [old _] (swap-vals! db (fn [s]
                                 (-> s
                                     (update :data #(apply dissoc % uniq))
                                     (update :expires #(apply disj % uniq)))))]
    (count (filter #(contains? (:data old) %) uniq))))

(defn clear! [db]
  (reset! db {:data {} :expires #{}})
  nil)

(defn wrong-type? [x]
  (= x wrong-type))

(defn fetch-typed
  [db k type]
  (let [e (get-entry db k)]
    (cond
      (nil? e) nil
      (= type (:type e)) e
      :else wrong-type)))

(defn typed-value
  [db k type default]
  (let [e (fetch-typed db k type)]
    (cond 
      (wrong-type? e) wrong-type
      (nil? e) default
      :else (:value e))))

(defn update-entry!
  [db k f]
  (let [t (now)
        [_ new-state] (swap-vals! db
                                  (fn [s]
                                    (let [cur (get (:data s) k)
                                          cur (when-not (expired? cur t)
                                                cur)
                                          next (f cur)
                                          next (cond
                                                 (nil? next) nil

                                                 (and (:expire-at cur) (not (contains? next :expire-at)))
                                                 (assoc next :expire-at (:expire-at cur))

                                                 :else next)]
                                      (if (nil? next)
                                        (-> s (update :data dissoc k) (update :expires disj k))
                                        (-> s (assoc-in [:data k] next) (update :expires reindex-expires k next))))))]
    (get (:data new-state) k)))

(defn update-value!
  [db k type f]
  (:value (update-entry! db k
                         (fn [e]
                           (let [next (f (:value e))]
                             (when (some? next)
                               (entry type next)))))))

(defn set-expire! [db k ^long expired-at]
  (if (get-entry db k)
    (do (swap! db (fn [s]
                    (-> s
                        (assoc-in [k :expire-at] expired-at)
                        (update :expires conj k))))
        true)
    false))

(defn persist! [db k]
  (if-let [e (get-entry db k)]
    (if (:expire-at e)
      (do (swap! db (fn [s]
                      (-> s
                          (update-in [:data k] dissoc :expire-at)
                          (update :expires disj k))))
          true)
      false)
    false))

(defn expired-at [db k]
  (:expire-at (get-entry db k)))

(defn expires-count ^long [db]
  (count (:expires @db)))

;; (defn expire-cycle-full!
;;   [db]
;;   (let [t (now)
;;         [old new] (swap-vals! db
;;                               (fn [m]
;;                                 (persistent!
;;                                  (reduce-kv (fn [acc k e]
;;                                               (if (expired? e t) (dissoc! acc k) acc))
;;                                             (transient m)
;;                                             m))))]
;;     (- (count old) (count new))))

;; ---------- 能動的期限切れ ----------

(def ^:private sample-size 20)
(def ^:private continue-threshold 0.25)

(defn- sample-expired-keys
  "期限付きキーからランダムに n 件サンプリングし、期限切れのものを返す。
   [サンプル数 期限切れのキー] を返す。"
  [db ^long n ^long t]
  (let [exp-keys (:expires @db)
        total (count exp-keys)]
    (if (zero? total)
      [0 []]
      (let [sampled (if (<= total n)
                      exp-keys
                      (let [v (vec exp-keys)]
                        (repeatedly n #(rand-nth v))))
            d (data db)]
        [(count sampled)
         (into [] (comp (distinct)
                        (filter #(expired? (get d %) t)))
               sampled)]))))

(defn expire-cycle!
  "能動的期限切れを1サイクル実行する。
   サンプリングして期限切れを削除し、期限切れの割合が閾値を超えていれば繰り返す。
   time-limit-ms で打ち切る。削除件数を返す。"
  ([db] (expire-cycle! db 1))
  ([db ^long time-limit-ms]
   (let [deadline (+ (System/nanoTime) (* time-limit-ms 1000000))]
     (loop [total-removed 0]
       (let [t (now)
             [sampled expired-keys] (sample-expired-keys db sample-size t)]
         (if (zero? sampled)
           total-removed
           (do
             (when (seq expired-keys)
               (swap! db (fn [s]
                           (-> s
                               (update :data #(apply dissoc % expired-keys))
                               (update :expires #(apply disj % expired-keys))))))
             (let [removed (count expired-keys)
                   ratio (/ (double removed) sampled)]
               (if (and (>= ratio continue-threshold)
                        (< (System/nanoTime) deadline))
                 (recur (+ total-removed removed))
                 (+ total-removed removed))))))))))
