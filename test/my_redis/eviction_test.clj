(ns my-redis.eviction-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [my-redis.command :as command]
            [my-redis.config :as config]
            [my-redis.db :as db]
            [my-redis.resp :as resp]))

(defn- ctx []
  {:db (db/create) :config (config/create) :stats (atom {:commands 0})})

(defn- run [c & args] (command/dispatch c (vec args)))
(defn- err-msg [r] (when (resp/error? r) (:message r)))

(defn- setup
  "上限と policy を設定した ctx を返す。"
  [limit policy]
  (let [c (ctx)]
    (run c "CONFIG" "SET" "maxkeys" (str limit))
    (run c "CONFIG" "SET" "maxmemory-policy" policy)
    c))

(def ^:private oom "OOM command not allowed when used memory > 'maxmemory'.")

;; ---------- CONFIG ----------

(deftest config-get-and-set
  (let [c (ctx)]
    (is (= ["maxkeys" "0"] (run c "CONFIG" "GET" "maxkeys")))
    (is (= "OK" (:value (run c "CONFIG" "SET" "maxkeys" "100"))))
    (is (= ["maxkeys" "100"] (run c "CONFIG" "GET" "maxkeys")))))

(deftest config-get-pattern
  (let [c (ctx)
        r (run c "CONFIG" "GET" "max*")]
    (is (= 6 (count r)) "3 つの設定が k v のペアで返る")
    (is (contains? (set (take-nth 2 r)) "maxmemory-policy"))))

(deftest config-rejects-unknown-and-invalid
  (let [c (ctx)]
    (is (some? (err-msg (run c "CONFIG" "SET" "no-such-param" "1"))))
    (is (some? (err-msg (run c "CONFIG" "SET" "maxmemory-policy" "bogus"))))
    (is (some? (err-msg (run c "CONFIG" "SET" "maxkeys" "abc"))))
    (testing "拒否された設定は変わっていない"
      (is (= ["maxmemory-policy" "noeviction"] (run c "CONFIG" "GET" "maxmemory-policy"))))))

;; ---------- 上限なし ----------

(deftest no-limit-by-default
  (let [c (ctx)]
    (dotimes [i 500] (run c "SET" (str "k" i) "v"))
    (is (= 500 (run c "DBSIZE")))))

;; ---------- noeviction ----------

(deftest noeviction-rejects-writes-at-limit
  (let [c (setup 3 "noeviction")]
    (dotimes [i 3] (run c "SET" (str "k" i) "v"))
    (is (= oom (err-msg (run c "SET" "new" "v"))))
    (is (= 3 (run c "DBSIZE")))))

(deftest noeviction-allows-reads-at-limit
  (let [c (setup 3 "noeviction")]
    (dotimes [i 3] (run c "SET" (str "k" i) "v"))
    (is (= "v" (run c "GET" "k0")))
    (is (= 1 (run c "EXISTS" "k0")))
    (is (= 3 (count (run c "KEYS" "*"))))
    (is (= 3 (run c "DBSIZE")))))

(deftest noeviction-allows-shrinking-commands
  (testing "データを減らす／変えないコマンドは上限でも通る"
    (let [c (setup 3 "noeviction")]
      (run c "SET" "a" "v")
      (run c "RPUSH" "l" "x" "y")
      (run c "SET" "b" "v")
      (is (= oom (err-msg (run c "SET" "c" "v"))) "上限に達している")
      (is (= 1 (run c "EXPIRE" "a" "100")) "EXPIRE は通る")
      (is (= 1 (run c "PERSIST" "a")) "PERSIST は通る")
      (is (= "x" (run c "LPOP" "l")) "LPOP は通る")
      (is (= 1 (run c "DEL" "a")) "DEL は通る"))))

(deftest recovers-after-del
  (let [c (setup 3 "noeviction")]
    (dotimes [i 3] (run c "SET" (str "k" i) "v"))
    (is (= oom (err-msg (run c "SET" "new" "v"))))
    (run c "DEL" "k0")
    (is (= "OK" (:value (run c "SET" "new" "v"))))))

(deftest overwrite-is-also-rejected
  (testing "キーが増えない上書きでも、上限に達していれば拒否される"
    (let [c (setup 3 "noeviction")]
      (dotimes [i 3] (run c "SET" (str "k" i) "v"))
      (is (= oom (err-msg (run c "SET" "k0" "newvalue")))))))

;; ---------- 期限切れの回収 ----------

(deftest expired-keys-are-reclaimed-before-oom
  (testing "noeviction でも期限切れは回収される"
    (let [c (setup 3 "noeviction")
          d (:db c)]
      (dotimes [i 3] (run c "SET" (str "k" i) "v" "PX" "50"))
      (Thread/sleep 100)
      (is (= 3 (db/key-count d)) "物理的にはまだ残っている")
      (is (= "OK" (:value (run c "SET" "new" "v"))) "回収されて空きができる")
      (is (= 1 (run c "DBSIZE"))))))

(deftest reclaim-happens-only-at-limit
  (testing "上限に達するまでは回収しない"
    (let [c (setup 3 "noeviction")
          d (:db c)]
      (run c "SET" "a" "v" "PX" "50")
      (run c "SET" "b" "v" "PX" "50")
      (Thread/sleep 100)
      (run c "SET" "c" "v")
      (is (= 3 (db/key-count d)) "期限切れが物理的に残っている")
      (is (= 1 (run c "DBSIZE")) "論理的には 1 件"))))

;; ---------- allkeys-random ----------

(deftest random-keeps-size-at-limit
  (let [c (setup 50 "allkeys-random")]
    (dotimes [i 1000] (run c "SET" (str "k" i) "v"))
    (is (<= 45 (run c "DBSIZE") 50))
    (is (pos? (:evicted (db/stats (:db c)))))))

(deftest random-never-returns-oom
  (let [c (setup 10 "allkeys-random")]
    (dotimes [i 200]
      (is (= "OK" (:value (run c "SET" (str "k" i) "v"))) (str "書き込み " i " で失敗")))))

;; ---------- allkeys-lru ----------

(deftest lru-evicts-least-recently-used
  (let [c (setup 3 "allkeys-lru")]
    (run c "SET" "a" "1") (Thread/sleep 5)
    (run c "SET" "b" "2") (Thread/sleep 5)
    (run c "SET" "c" "3") (Thread/sleep 5)
    (run c "GET" "a")                          ; a を新しくする
    (Thread/sleep 5)
    (run c "SET" "d" "4")
    (is (= ["a" "c" "d"] (sort (run c "KEYS" "*"))) "最も古い b が捨てられる")))

(deftest lru-not-updated-by-metadata-commands
  (testing "EXISTS / TTL / TYPE はアクセスとみなさない"
    (let [c (setup 3 "allkeys-lru")]
      (run c "SET" "a" "1") (Thread/sleep 5)
      (run c "SET" "b" "2") (Thread/sleep 5)
      (run c "SET" "c" "3") (Thread/sleep 5)
      (run c "EXISTS" "a")
      (run c "TTL" "a")
      (run c "TYPE" "a")
      (Thread/sleep 5)
      (run c "SET" "d" "4")
      (is (= ["b" "c" "d"] (sort (run c "KEYS" "*"))) "a は新しくならないので捨てられる"))))

(deftest lru-not-reset-by-keys-command
  (testing "KEYS * で全キーの LRU 情報が壊れない"
    (let [c (setup 3 "allkeys-lru")]
      (run c "SET" "a" "1") (Thread/sleep 5)
      (run c "SET" "b" "2") (Thread/sleep 5)
      (run c "SET" "c" "3") (Thread/sleep 5)
      (run c "GET" "a") (Thread/sleep 5)
      (run c "KEYS" "*")
      (Thread/sleep 5)
      (run c "SET" "d" "4")
      (is (= ["a" "c" "d"] (sort (run c "KEYS" "*")))))))

(deftest lru-updated-by-value-commands
  (testing "値を使うコマンドはアクセスとみなす"
    (let [c (setup 3 "allkeys-lru")]
      (run c "SET" "a" "1") (Thread/sleep 5)
      (run c "SET" "b" "2") (Thread/sleep 5)
      (run c "SET" "c" "3") (Thread/sleep 5)
      (run c "APPEND" "a" "x")                 ; 書き込みもアクセス
      (Thread/sleep 5)
      (run c "SET" "d" "4")
      (is (= ["a" "c" "d"] (sort (run c "KEYS" "*")))))))

(deftest lru-evicts-least-recently-used-in-small-set
  (testing "キー数がサンプル数以下なら、正確な LRU として動く"
    (let [c (setup 5 "allkeys-lru")]
      (run c "CONFIG" "SET" "maxmemory-samples" "10")
      (doseq [k ["a" "b" "c" "d" "e"]]
        (run c "SET" k "v") (Thread/sleep 5))
      (run c "GET" "a") (Thread/sleep 5)
      (run c "SET" "f" "v")
      (is (= ["a" "c" "d" "e" "f"] (sort (run c "KEYS" "*")))
          "最も古い b が捨てられる"))))

;; ---------- volatile-lru ----------

(deftest volatile-lru-only-evicts-keys-with-ttl
  (let [c (setup 4 "volatile-lru")]
    (run c "SET" "keep1" "v")
    (run c "SET" "keep2" "v")
    (run c "SET" "vol1" "v" "EX" "1000") (Thread/sleep 5)
    (run c "SET" "vol2" "v" "EX" "1000") (Thread/sleep 5)
    (run c "SET" "new1" "v")
    (is (= ["keep1" "keep2" "new1" "vol2"] (sort (run c "KEYS" "*")))
        "TTL 付きの古い方が捨てられる")))

(deftest volatile-lru-degenerates-to-noeviction
  (testing "TTL 付きが尽きたら捨てられず OOM になる"
    (let [c (setup 3 "volatile-lru")]
      (run c "SET" "keep1" "v")
      (run c "SET" "keep2" "v")
      (run c "SET" "vol1" "v" "EX" "1000")
      (is (= "OK" (:value (run c "SET" "new1" "v"))) "vol1 を捨てて書ける")
      (is (= oom (err-msg (run c "SET" "new2" "v"))) "候補が尽きた"))))

(deftest volatile-lru-with-no-ttl-keys-at-all
  (testing "TTL 付きが1件も無ければ最初から OOM"
    (let [c (setup 2 "volatile-lru")]
      (run c "SET" "a" "v")
      (run c "SET" "b" "v")
      (is (= oom (err-msg (run c "SET" "c" "v")))))))

;; ---------- 統計と INFO ----------

(deftest stats-count-evictions-and-expirations
  (let [c (setup 10 "allkeys-random")
        d (:db c)]
    (db/reset-stats! d)
    (dotimes [i 100] (run c "SET" (str "k" i) "v"))
    (is (pos? (:evicted (db/stats d))))

    (let [c2 (ctx)
          d2 (:db c2)]
      (command/dispatch c2 ["SET" "x" "v" "PX" "10"])
      (Thread/sleep 50)
      (command/dispatch c2 ["GET" "x"])
      (is (= 1 (:expired (db/stats d2))) "受動的期限切れが数えられる"))))

(deftest info-returns-sections
  (let [c (setup 10 "allkeys-lru")]
    (run c "SET" "a" "v")
    (let [info (run c "INFO" "stats")]
      (is (string? info))
      (is (str/includes? info "# Stats"))
      (is (str/includes? info "evicted_keys:")))
    (let [mem (run c "INFO" "memory")]
      (is (str/includes? mem "maxmemory_policy:allkeys-lru")))
    (testing "セクション指定なしなら全部返る"
      (let [all (run c "INFO")]
        (is (str/includes? all "# Stats"))
        (is (str/includes? all "# Memory"))))))

;; ---------- eviction が他の型でも動く ----------

(deftest eviction-works-with-all-types
  (let [c (setup 4 "allkeys-random")]
    (run c "SET" "s" "v")
    (run c "RPUSH" "l" "a")
    (run c "HSET" "h" "f" "v")
    (run c "SADD" "st" "m")
    (is (= 4 (run c "DBSIZE")))
    (run c "ZADD" "z" "1" "a")
    (is (= 4 (run c "DBSIZE")) "1 つ捨てられて 1 つ増えた")))
