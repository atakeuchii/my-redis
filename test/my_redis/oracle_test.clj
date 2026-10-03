(ns my-redis.oracle-test
  "本物の redis を参照オラクルとして、同じコマンド列を流して応答を比較する。
   本物が 6379 で動いていなければテストをスキップする。

     docker run --rm -d -p 6379:6379 --name redis-oracle redis"
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [my-redis.resp :as resp]
            [my-redis.server :as server])
  (:import [java.net Socket]
           [java.io BufferedInputStream BufferedOutputStream]))

(def ^:private real-port 6379)

;; ---------- 本物への接続 ----------

(defn- connect
  [port]
  (let [sock (doto (Socket. "localhost" (int port)) (.setSoTimeout 2000))]
    {:socket sock
     :in  (BufferedInputStream. (.getInputStream sock))
     :out (BufferedOutputStream. (.getOutputStream sock))}))

(defn- send-cmd!
  [{:keys [in ^BufferedOutputStream out]} cmd]
  (resp/write-reply! out (vec cmd))
  (.flush out)
  (resp/read-reply in))

(defn- close-conn! [{:keys [^Socket socket]}] (.close socket))

(defn- real-available?
  []
  (try
    (let [c (connect real-port)]
      (try (= "PONG" (send-cmd! c ["PING"]))
           (finally (close-conn! c))))
    (catch Exception _ false)))

;; ---------- 応答の正規化 ----------

(def ^:private unordered-commands
  "順序を保証しないコマンド。集合として比較する。"
  #{"KEYS" "SMEMBERS" "HGETALL" "HKEYS" "HVALS" "SINTER" "SUNION" "SDIFF" "PUBSUB"})

(defn- normalize
  "比較用に応答を正規化する。
   エラーはメッセージの文字列、順序不定のコマンドは集合にする。"
  [cmd reply]
  (let [name (str/upper-case (first cmd))]
    (cond
      (resp/error? reply) [:error (:message reply)]
      (instance? my_redis.resp.SimpleString reply) [:simple (:value reply)]
      (and (coll? reply) (unordered-commands name))
      [:set (set (if (= name "HGETALL") (map vec (partition 2 reply)) reply))]
      :else reply)))

;; ---------- 比較 ----------

(defn- run-both
  "同じコマンド列を両方に流し、食い違いのリストを返す。"
  [cmds]
  (let [srv  (server/start! 0 {:appendonly false :verbose? false})
        mine (connect (:port @srv))
        real (connect real-port)]
    (try
      (send-cmd! real ["FLUSHDB"])
      (send-cmd! mine ["FLUSHDB"])
      (doall
       (for [cmd  cmds
             :let [r-real (normalize cmd (send-cmd! real cmd))
                   r-mine (normalize cmd (send-cmd! mine cmd))]
             :when (not= r-real r-mine)]
         {:cmd cmd :real r-real :mine r-mine}))
      (finally
        (close-conn! mine)
        (close-conn! real)
        (server/stop! srv)))))

(defmacro deftest-oracle
  "本物が動いていればテストを実行し、無ければスキップする。"
  [name & body]
  `(deftest ~name
     (if-not (real-available?)
       (println (str "[oracle] skipping " '~name " — 本物の redis (6379) が見つかりません"))
       (do ~@body))))

(defn- check [label cmds]
  (let [diffs (run-both cmds)]
    (is (empty? diffs)
        (str label " で " (count diffs) " 件の差分:\n"
             (str/join "\n" (map pr-str (take 20 diffs)))))))

;; ---------- テスト ----------

(deftest-oracle string-commands
  (check "文字列"
         [["SET" "k" "v"] ["GET" "k"] ["GET" "nokey"]
          ["SET" "k" ""] ["GET" "k"] ["STRLEN" "k"] ["EXISTS" "k"]
          ["SET" "n" "10"] ["INCR" "n"] ["DECR" "n"] ["INCRBY" "n" "5"] ["DECRBY" "n" "3"]
          ["SET" "s" "abc"] ["INCR" "s"]
          ["APPEND" "a" "hello"] ["APPEND" "a" " world"] ["GET" "a"] ["STRLEN" "a"]
          ["GETSET" "a" "new"] ["GETSET" "fresh" "x"]
          ["SETNX" "a" "ignored"] ["SETNX" "brand" "v"]
          ["MSET" "m1" "1" "m2" "2"] ["MGET" "m1" "m2" "nokey"]
          ["SET" "j" "あ"] ["STRLEN" "j"] ["GET" "j"]
          ["SET" "big" "9223372036854775807"] ["INCR" "big"]
          ["DEL" "k"] ["DEL" "k"] ["DEL" "m1" "m2" "nokey"]
          ["TYPE" "a"] ["TYPE" "nokey"]]))

(deftest-oracle error-messages
  (check "エラー文面"
         [["NOSUCHCMD"] ["GET"] ["GET" "a" "b"] ["SET" "k"]
          ["MSET" "a" "1" "b"] ["HSET" "h" "f"]
          ["EXPIRE" "k" "abc"] ["LRANGE" "l" "a" "b"]
          ["SET" "k" "v"] ["LPUSH" "k" "x"] ["GET" "k"]
          ["RPUSH" "l" "a"] ["GET" "l"] ["INCR" "l"]
          ["SET" "k" "v" "EX" "0"] ["SET" "k" "v" "NX" "XX"] ["SET" "k" "v" "FOO"]
          ["ZADD" "z" "nan" "a"] ["ZADD" "z" "abc" "a"]]))

(deftest-oracle list-commands
  (check "リスト"
         [["RPUSH" "l" "a" "b" "c"] ["LRANGE" "l" "0" "-1"]
          ["LPUSH" "l" "x" "y"] ["LRANGE" "l" "0" "-1"]
          ["LLEN" "l"] ["LINDEX" "l" "0"] ["LINDEX" "l" "-1"] ["LINDEX" "l" "99"]
          ["LRANGE" "l" "1" "3"] ["LRANGE" "l" "-2" "-1"]
          ["LRANGE" "l" "0" "999"] ["LRANGE" "l" "10" "20"] ["LRANGE" "l" "3" "1"]
          ["LSET" "l" "0" "Z"] ["LRANGE" "l" "0" "-1"] ["LSET" "l" "99" "x"]
          ["LPOP" "l"] ["RPOP" "l"] ["LRANGE" "l" "0" "-1"]
          ["LREM" "l" "0" "b"] ["LRANGE" "l" "0" "-1"]
          ["DEL" "l"] ["RPUSH" "l" "a" "b" "a" "c" "a"]
          ["LREM" "l" "2" "a"] ["LRANGE" "l" "0" "-1"]
          ["DEL" "l"] ["RPUSH" "l" "a" "b" "a" "c" "a"]
          ["LREM" "l" "-2" "a"] ["LRANGE" "l" "0" "-1"]
          ["LTRIM" "l" "0" "1"] ["LRANGE" "l" "0" "-1"]
          ["DEL" "l"] ["RPUSH" "l" "only"] ["LPOP" "l"] ["EXISTS" "l"]
          ["LLEN" "nokey"] ["LPOP" "nokey"] ["LRANGE" "nokey" "0" "-1"]]))

(deftest-oracle hash-and-set-commands
  (check "Hash/Set"
         [["HSET" "h" "f1" "v1" "f2" "v2"] ["HSET" "h" "f1" "x"]
          ["HGET" "h" "f1"] ["HGET" "h" "nof"] ["HLEN" "h"]
          ["HEXISTS" "h" "f1"] ["HEXISTS" "h" "nof"]
          ["HGETALL" "h"] ["HKEYS" "h"] ["HVALS" "h"]
          ["HINCRBY" "h" "n" "5"] ["HINCRBY" "h" "n" "-2"] ["HINCRBY" "h" "f1" "1"]
          ["HDEL" "h" "f1" "nof"] ["HDEL" "h" "f1"]
          ["HGETALL" "nokey"] ["HLEN" "nokey"]

          ["SADD" "s" "a" "b" "c"] ["SADD" "s" "a" "d"] ["SADD" "s" "a"]
          ["SCARD" "s"] ["SISMEMBER" "s" "a"] ["SISMEMBER" "s" "zz"]
          ["SMEMBERS" "s"] ["SREM" "s" "a" "zz"]
          ["SADD" "s1" "a" "b" "c"] ["SADD" "s2" "b" "c" "d"]
          ["SINTER" "s1" "s2"] ["SINTER" "s2" "s1"]
          ["SUNION" "s1" "s2"] ["SDIFF" "s1" "s2"] ["SDIFF" "s2" "s1"]
          ["SINTER" "s1" "nokey"] ["SUNION" "s1" "nokey"] ["SDIFF" "nokey" "s1"]
          ["SMEMBERS" "nokey"] ["SCARD" "nokey"]]))

(deftest-oracle zset-commands
  (check "ZSet"
         [["ZADD" "z" "1" "a" "2" "b" "3" "c"] ["ZADD" "z" "5" "a"]
          ["ZSCORE" "z" "a"] ["ZSCORE" "z" "nope"] ["ZCARD" "z"]
          ["ZRANGE" "z" "0" "-1"] ["ZRANGE" "z" "0" "-1" "WITHSCORES"]
          ["ZREVRANGE" "z" "0" "-1"] ["ZREVRANGE" "z" "0" "1"]
          ["ZREVRANGE" "z" "-1" "-1"] ["ZREVRANGE" "z" "0" "999"]
          ["ZRANK" "z" "a"] ["ZRANK" "z" "nope"] ["ZREVRANK" "z" "a"]
          ["ZINCRBY" "z" "2.5" "a"] ["ZSCORE" "z" "a"]
          ["ZINCRBY" "z" "1" "fresh"]
          ["ZRANGEBYSCORE" "z" "1" "3"] ["ZRANGEBYSCORE" "z" "(1" "3"]
          ["ZRANGEBYSCORE" "z" "-inf" "+inf"] ["ZRANGEBYSCORE" "z" "3" "1"]
          ["ZCOUNT" "z" "-inf" "+inf"] ["ZCOUNT" "z" "(1" "(3"]
          ["ZREM" "z" "a" "nope"]
          ["DEL" "z"] ["ZADD" "z" "1" "banana" "1" "apple" "1" "cherry"]
          ["ZRANGE" "z" "0" "-1"] ["ZRANK" "z" "cherry"]
          ["DEL" "z"] ["ZADD" "z" "1.5" "a" "2.0" "b" "1e3" "c" "inf" "d" "-inf" "e"]
          ["ZSCORE" "z" "a"] ["ZSCORE" "z" "b"] ["ZSCORE" "z" "c"]
          ["ZSCORE" "z" "d"] ["ZSCORE" "z" "e"]
          ["ZRANGE" "z" "0" "-1" "WITHSCORES"]
          ["ZCARD" "nokey"] ["ZRANGE" "nokey" "0" "-1"] ["ZSCORE" "nokey" "m"]]))

(deftest-oracle ttl-commands
  (check "TTL"
         [["SET" "k" "v"] ["TTL" "k"] ["PTTL" "k"] ["TTL" "nokey"]
          ["EXPIRE" "k" "100"] ["TTL" "k"]
          ["PERSIST" "k"] ["TTL" "k"] ["PERSIST" "k"]
          ["EXPIRE" "nokey" "10"]
          ["SET" "t" "v" "EX" "100"] ["TTL" "t"]
          ["SET" "t" "new"] ["TTL" "t"]
          ["SET" "t" "v" "EX" "100"] ["APPEND" "t" "x"] ["TTL" "t"]
          ["SET" "nx" "v" "NX"] ["SET" "nx" "other" "NX"] ["GET" "nx"]
          ["SET" "xx" "v" "XX"] ["EXISTS" "xx"]
          ["SET" "nx" "updated" "XX"] ["GET" "nx"]]))

(deftest-oracle pubsub-commands
  (check "Pub/Sub"
         [["PUBLISH" "news" "x"]
          ["PUBSUB" "CHANNELS"]
          ["PUBSUB" "NUMSUB" "news"]]))

;; ---------- ランダムなコマンド列 ----------

(def ^:private gen-keys ["k1" "k2" "k3"])

(defn- random-command []
  (let [k (rand-nth gen-keys)]
    (rand-nth
     [["SET" k (str (rand-int 100))]
      ["GET" k]
      ["DEL" k]
      ["EXISTS" k]
      ["TYPE" k]
      ["INCR" k]
      ["APPEND" k "x"]
      ["STRLEN" k]
      ["RPUSH" k (str (rand-int 10))]
      ["LPUSH" k (str (rand-int 10))]
      ["LPOP" k]
      ["RPOP" k]
      ["LLEN" k]
      ["LRANGE" k "0" "-1"]
      ["SADD" k (str (rand-int 10))]
      ["SREM" k (str (rand-int 10))]
      ["SCARD" k]
      ["HSET" k "f" (str (rand-int 10))]
      ["HGET" k "f"]
      ["HDEL" k "f"]
      ["ZADD" k (str (rand-int 10)) "m"]
      ["ZSCORE" k "m"]
      ["ZCARD" k]
      ["EXPIRE" k "1000"]
      ["TTL" k]
      ["PERSIST" k]])))

(deftest-oracle random-command-sequences
  (testing "ランダムな操作列で応答が一致する"
    (dotimes [i 5]
      (check (str "ランダム列 " i) (repeatedly 100 random-command)))))
