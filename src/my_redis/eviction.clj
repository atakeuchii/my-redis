(ns my-redis.eviction
  (:require [my-redis.config :as config]
            [my-redis.db :as db]))

(defn- choose-victim [db policy ^long samples]
  (case policy
    ("allkeys-random")
    (first (db/sample-keys db 1 :all))

    "allkeys-lru"
    (db/oldest-of db (db/sample-keys db samples :all))

    "volatile-lru"
    (db/oldest-of db (db/sample-keys db samples (db/volatile-keys-seq db)))

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
                    (recur (inc guard)))
                false))))))))
