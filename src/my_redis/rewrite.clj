(ns my-redis.rewrite
  "キースペースの現在の状態から、それを再現する最小のコマンド列を生成する。
   AOF rewrite（= my-storage の compaction）の中核。"
  (:require [my-redis.types.list :as dlist]
            [my-redis.types.zset :as zset]))

(def ^:count items-per-cmd 64)

(defn- format-score ^String [^double s]
  (cond
    (= s Double/POSITIVE_INFINITY) "inf"
    (= s Double/NEGATIVE_INFINITY) "-inf"
    (and (== s (Math/rint s)) (< (Math/abs s) 1e17)) (str (long s))
    :else (str s)))

(defn- chunked-commands [cmd-name k items]
  (map (fn [chunk] (into [cmd-name k] chunk))
       (partition-all items-per-cmd items)))

(defn entry-commands [k entry]
  (let [{:keys [type value expire-at]} entry
        cmds (case type
               :string [["SET" k value]]
               :list (chunked-commands "RPUSH" k (dlist/to-vec value))
               :hash (chunked-commands "HSET" k (mapcat identity value))
               :set (chunked-commands "SADD" k (seq value))
               :zset (chunked-commands "ZADD" k
                                       (mapcat (fn [[s m]] [(format-score s) m])
                                               (zset/entries value)))
               nil)]
    (if (and (seq cmds) expire-at)
      (concat cmds [["PEXPIREAT" k (str expire-at)]])
      cmds)))

(defn snapshot-commands [snapshot]
  (mapcat (fn [[k entry]] (entry-commands k entry)) snapshot))
