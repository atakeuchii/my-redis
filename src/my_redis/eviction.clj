(ns my-redis.eviction
  (:require [my-redis.config :as config]
            [my-redis.db :as db]))

(def stats (atom {:evicted 0}))

(defn reset-stats! []
  (reset! stats {:evicted 0}))

(defn- choose-victim [db policy _samples]
  (case policy
    ("allkeys-random")
    (let [cands (db/all-keys-vec db)]
      (when (seq cands) (rand-nth cands)))

    "allkeys-lru"
    (db/oldest-key db nil)

    "volatile-lru"
    (let [cands (db/volatile-keys-vec db)]
      (when (seq cands) (db/oldest-key db cands)))

    nil))

(defn ensure-capacity!
  "上限を超えていたら空きを作る。
   1. 期限切れを回収する
   2. まだ超えていたら policy に従って捨てる
   上限以下になれば true、できなければ false。"
  [db cfg]
  (let [limit (config/get-long cfg "maxkeys" 0)]
    (if-not (pos? limit)
      true
      (do
        (when (>= (db/key-count db) limit)
          (db/expire-cycle! db 5))

        (let [policy (config/get-raw cfg "maxmemory-policy")
              samples (config/get-long cfg "maxmemory-samples" 5)]
          (loop [guard 0]
            (cond
              (< (db/key-count db) limit) true
              (= policy "noeviction") false
              (> guard 10000) false

              :else
              (if-let [victim (choose-victim db policy samples)]
                (do (db/evict! db victim)
                    (swap! stats update :evicted inc)
                    (recur (inc guard)))
                false))))))))
