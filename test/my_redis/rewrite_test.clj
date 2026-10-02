(ns my-redis.rewrite-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [my-redis.aof :as aof]
            [my-redis.command :as command]
            [my-redis.config :as config]
            [my-redis.db :as db]
            [my-redis.executor :as executor]
            [my-redis.resp :as resp]
            [my-redis.rewrite :as rewrite]
            [my-redis.server :as server])
  (:import [java.io File BufferedInputStream FileInputStream]))

(defn- ctx []
  {:db (db/create) :config (config/create) :stats (atom {:commands 0})})

(defn- run [c & args] (command/dispatch c (vec args)))

(defn- cmds-for
  "ctx の現在の状態から rewrite コマンド列を生成する。"
  [c]
  (vec (rewrite/snapshot-commands (:data @(:db c)) (db/now))))

(defn- temp-dir ^File []
  (doto (File/createTempFile "myredis-rw" "") (.delete) (.mkdir)))

(defn- delete-tree [^File dir]
  (doseq [^File f (reverse (file-seq dir))] (.delete f)))

;; ---------- コマンド列の生成 ----------

(deftest history-collapses-to-state
  (testing "同じキーへの大量の上書きが 1 コマンドになる"
    (let [c (ctx)]
      (dotimes [i 1000] (run c "SET" "counter" (str i)))
      (is (= [["SET" "counter" "999"]] (cmds-for c))))))

(deftest deleted-keys-produce-nothing
  (let [c (ctx)]
    (dotimes [i 100] (run c "SET" (str "k" i) "v") (run c "DEL" (str "k" i)))
    (is (= [] (cmds-for c)))))

(deftest all-types-produce-commands
  (testing "全ての型が rewrite でコマンドを生成する（型を足したときの漏れ検出）"
    (let [c (ctx)]
      (run c "SET" "s" "v")
      (run c "RPUSH" "l" "a")
      (run c "HSET" "h" "f" "v")
      (run c "SADD" "st" "m")
      (run c "ZADD" "z" "1" "a")
      (let [by-key (into {} (map (fn [cmd] [(second cmd) (first cmd)])) (cmds-for c))]
        (is (= "SET"   (by-key "s")))
        (is (= "RPUSH" (by-key "l")))
        (is (= "HSET"  (by-key "h")))
        (is (= "SADD"  (by-key "st")))
        (is (= "ZADD"  (by-key "z")))))))

(deftest unknown-type-throws
  (testing "未知の型は黙って無視せず例外にする"
    (is (thrown? clojure.lang.ExceptionInfo
                 (rewrite/entry-commands "k" {:type :stream :value []})))))

(deftest list-uses-rpush-not-lpush
  (testing "順序を保つため必ず RPUSH で出力する"
    (let [c (ctx)]
      (run c "LPUSH" "l" "a" "b" "c")          ; 実際の並びは c b a
      (is (= [["RPUSH" "l" "c" "b" "a"]] (cmds-for c))))))

(deftest large-collections-are-chunked
  (let [c (ctx)]
    (command/dispatch c (into ["RPUSH" "big"] (map str (range 200))))
    (let [cmds (cmds-for c)]
      (is (= 4 (count cmds)) "64 要素ずつ 4 コマンドに分かれる")
      (is (every? #(= "RPUSH" (first %)) cmds))
      (is (= [64 64 64 8] (map #(- (count %) 2) cmds))))))

(deftest expiry-is-emitted-as-pexpireat
  (let [c (ctx)]
    (run c "SET" "k" "v" "EX" "100")
    (let [cmds (cmds-for c)]
      (is (= 2 (count cmds)))
      (is (= ["SET" "k" "v"] (first cmds)))
      (is (= "PEXPIREAT" (ffirst (rest cmds)))))))

(deftest expired-keys-are-excluded
  (let [c (ctx)]
    (run c "SET" "live" "v")
    (run c "SET" "dead" "v" "PX" "50")
    (Thread/sleep 100)
    (is (= 2 (db/key-count (:db c))) "物理的には残っている")
    (is (= [["SET" "live" "v"]] (cmds-for c)))))

;; ---------- 生成したコマンドで復元できるか ----------

(deftest generated-commands-restore-state
  (let [c1 (ctx)]
    (run c1 "SET" "s" "hello")
    (run c1 "RPUSH" "l" "a" "b" "c")
    (run c1 "HSET" "h" "f1" "v1" "f2" "v2")
    (run c1 "SADD" "st" "x" "y")
    (run c1 "ZADD" "z" "1" "a" "2.5" "b" "inf" "c")
    (run c1 "SET" "t" "v" "EX" "1000")

    (let [c2 (ctx)]
      (doseq [cmd (cmds-for c1)] (command/dispatch c2 cmd))
      (is (= "hello" (run c2 "GET" "s")))
      (is (= ["a" "b" "c"] (run c2 "LRANGE" "l" "0" "-1")))
      (is (= #{["f1" "v1"] ["f2" "v2"]} (set (partition 2 (run c2 "HGETALL" "h")))))
      (is (= #{"x" "y"} (set (run c2 "SMEMBERS" "st"))))
      (is (= ["a" "b" "c"] (run c2 "ZRANGE" "z" "0" "-1")))
      (is (= "2.5" (run c2 "ZSCORE" "z" "b")))
      (is (= "inf" (run c2 "ZSCORE" "z" "c")))
      (is (<= 999 (run c2 "TTL" "t") 1000))
      (is (= 6 (run c2 "DBSIZE"))))))

(deftest generated-commands-restore-large-list
  (testing "分割されたコマンドでも順序が保たれる"
    (let [c1 (ctx)]
      (command/dispatch c1 (into ["RPUSH" "big"] (map str (range 200))))
      (let [c2 (ctx)]
        (doseq [cmd (cmds-for c1)] (command/dispatch c2 cmd))
        (is (= (mapv str (range 200)) (run c2 "LRANGE" "big" "0" "-1")))))))

;; ---------- サーバ経由の rewrite ----------

(defn- aof-commands-in
  "AOF に記録されているコマンドを全部読む。"
  [^File dir]
  (let [f (File. dir "appendonly.aof")]
    (if-not (.exists f)
      []
      (with-open [in (BufferedInputStream. (FileInputStream. f))]
        (loop [acc []]
          (let [c (try (resp/read-reply in) (catch java.io.EOFException _ nil))]
            (if c (recur (conj acc c)) acc)))))))

(defn- rewrite-in-progress? [ex]
  (->> (str/split (executor/submit! ex ["INFO" "persistence"]) #"\r\n")
       (filter #(str/starts-with? % "aof_rewrite_in_progress"))
       first
       (re-find #"\d+")
       Long/parseLong
       (= 1)))

(defn- wait-for-rewrite [ex]
  (loop [n 0]
    (when (and (rewrite-in-progress? ex) (< n 200))
      (Thread/sleep 25)
      (recur (inc n)))))

(deftest bgrewriteaof-shrinks-file
  (let [dir (temp-dir)]
    (try
      (let [srv (server/start! 0 {:dir (str dir) :appendonly true
                                  :appendfsync :everysec :verbose? false})
            ex  (:executor @srv)
            f   (File. dir "appendonly.aof")]
        (try
          (dotimes [i 3000] (executor/submit! ex ["SET" "counter" (str i)]))
          (let [before (.length f)]
            (executor/submit! ex ["BGREWRITEAOF"])
            (wait-for-rewrite ex)
            (is (< (.length f) (/ before 10)) "10 分の 1 以下に縮む")
            (is (= 1 (count (aof-commands-in dir))) "1 コマンドだけ残る")
            (is (= ["SET" "counter" "2999"] (first (aof-commands-in dir)))))
          (finally (server/stop! srv))))
      (finally (delete-tree dir)))))

(deftest bgrewriteaof-without-aof-is-error
  (let [srv (server/start! 0 {:appendonly false :verbose? false})
        ex  (:executor @srv)]
    (try
      (let [r (executor/submit! ex ["BGREWRITEAOF"])]
        (is (resp/error? r))
        (is (= "ERR AOF is not enabled" (:message r))))
      (finally (server/stop! srv)))))

(deftest rewrite-preserves-concurrent-writes
  (testing "rewrite 中の書き込みが 1 件も失われない"
    (let [dir (temp-dir)
          written (atom 0)]
      (try
        (let [srv (server/start! 0 {:dir (str dir) :appendonly true
                                    :appendfsync :everysec :verbose? false})
              ex  (:executor @srv)]
          (try
            (dotimes [i 20000] (executor/submit! ex ["SET" (str "base" i) "v"]))
            (let [base-count (executor/submit! ex ["DBSIZE"])]
              (executor/submit! ex ["BGREWRITEAOF"])
              (loop []
                (when (rewrite-in-progress? ex)
                  (executor/submit! ex ["SET" (str "during" @written) "v"])
                  (swap! written inc)
                  (recur)))
              (is (pos? @written) "rewrite 中に書き込めた")
              (is (= (+ base-count @written) (executor/submit! ex ["DBSIZE"]))))
            (finally (server/stop! srv))))

        ;; 再起動して、AOF から全件復元できるか
        (let [srv2 (server/start! 0 {:dir (str dir) :appendonly true :verbose? false})
              ex2  (:executor @srv2)]
          (try
            (let [all (executor/submit! ex2 ["KEYS" "*"])]
              (is (= 20000 (count (filter #(str/starts-with? % "base") all))))
              (is (= @written (count (filter #(str/starts-with? % "during") all)))
                  "rewrite 中の書き込みが全件残っている"))
            (finally (server/stop! srv2))))
        (finally (delete-tree dir))))))

(deftest rewrite-result-replays-correctly
  (testing "rewrite 後のファイルから全型が復元できる"
    (let [dir (temp-dir)]
      (try
        (let [srv (server/start! 0 {:dir (str dir) :appendonly true
                                    :appendfsync :everysec :verbose? false})
              ex  (:executor @srv)]
          (try
            (executor/submit! ex ["SET" "s" "v"])
            (executor/submit! ex ["RPUSH" "l" "a" "b" "c"])
            (executor/submit! ex ["HSET" "h" "f" "v"])
            (executor/submit! ex ["SADD" "st" "x"])
            (executor/submit! ex ["ZADD" "z" "1.5" "a"])
            (executor/submit! ex ["SET" "t" "v" "EX" "1000"])
            (dotimes [i 500] (executor/submit! ex ["SET" "churn" (str i)]))
            (executor/submit! ex ["BGREWRITEAOF"])
            (wait-for-rewrite ex)
            (finally (server/stop! srv))))

        (let [srv2 (server/start! 0 {:dir (str dir) :appendonly true :verbose? false})
              ex2  (:executor @srv2)]
          (try
            (is (= "v" (executor/submit! ex2 ["GET" "s"])))
            (is (= ["a" "b" "c"] (executor/submit! ex2 ["LRANGE" "l" "0" "-1"])))
            (is (= "v" (executor/submit! ex2 ["HGET" "h" "f"])))
            (is (= ["x"] (executor/submit! ex2 ["SMEMBERS" "st"])))
            (is (= "1.5" (executor/submit! ex2 ["ZSCORE" "z" "a"])))
            (is (<= 999 (executor/submit! ex2 ["TTL" "t"]) 1000))
            (is (= "499" (executor/submit! ex2 ["GET" "churn"])))
            (is (= 7 (executor/submit! ex2 ["DBSIZE"])))
            (finally (server/stop! srv2))))
        (finally (delete-tree dir))))))

;; ---------- 自動トリガ ----------

(deftest auto-rewrite-triggers-on-growth
  (let [dir (temp-dir)]
    (try
      (let [srv (server/start! 0 {:dir (str dir) :appendonly true
                                  :appendfsync :everysec :verbose? false
                                  :expire-interval-ms 50})
            ex  (:executor @srv)
            f   (File. dir "appendonly.aof")]
        (try
          (executor/submit! ex ["CONFIG" "SET" "auto-aof-rewrite-min-size" "4000"])
          (executor/submit! ex ["CONFIG" "SET" "auto-aof-rewrite-percentage" "100"])
          ;; 同じキーを上書きし続ける → ファイルだけ育つ
          (loop [n 0]
            (when (< n 20)
              (dotimes [_ 500] (executor/submit! ex ["SET" "counter" "xxxxxxxxxx"]))
              (Thread/sleep 100)
              (recur (inc n))))
          (wait-for-rewrite ex)
          (is (< (.length f) 50000) "自動 rewrite が走ってファイルが抑えられている")
          (is (= 1 (executor/submit! ex ["DBSIZE"])))
          (finally (server/stop! srv))))
      (finally (delete-tree dir)))))

(deftest auto-rewrite-respects-min-size
  (testing "min-size 未満では走らない"
    (let [dir (temp-dir)]
      (try
        (let [srv (server/start! 0 {:dir (str dir) :appendonly true
                                    :appendfsync :everysec :verbose? false
                                    :expire-interval-ms 50})
              ex  (:executor @srv)]
          (try
            (executor/submit! ex ["CONFIG" "SET" "auto-aof-rewrite-min-size" "100000000"])
            (dotimes [i 2000] (executor/submit! ex ["SET" "counter" (str i)]))
            (Thread/sleep 300)
            (is (< 1 (count (aof-commands-in dir))) "rewrite されていない")
            (finally (server/stop! srv))))
        (finally (delete-tree dir))))))
