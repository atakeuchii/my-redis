(ns my-redis.rdb-test
  (:require [clojure.test :refer [deftest is testing]]
            [my-redis.command :as command]
            [my-redis.config :as config]
            [my-redis.db :as db]
            [my-redis.executor :as executor]
            [my-redis.rdb :as rdb]
            [my-redis.server :as server])
  (:import [java.io File FileOutputStream]
           [java.nio.file Files OpenOption]))

(defn- ctx []
  {:db (db/create) :config (config/create) :stats (atom {:commands 0})})

(defn- run [c & args] (command/dispatch c (vec args)))

(defn- temp-dir ^File []
  (doto (File/createTempFile "myredis-rdb" "") (.delete) (.mkdir)))

(defn- delete-tree [^File dir]
  (doseq [^File f (reverse (file-seq dir))] (.delete f)))

(defn- roundtrip
  "ctx の状態を RDB に書いて読み戻し、新しい ctx を返す。"
  [c ^File f]
  (rdb/write-file! f (:data @(:db c)) (db/now))
  (let [c2 (ctx)]
    (doseq [cmd (rdb/load-commands f)] (command/dispatch c2 cmd))
    c2))

;; ---------- 形式 ----------

(deftest file-starts-with-magic
  (let [c (ctx)]
    (run c "SET" "k" "v")
    (let [bs (rdb/serialize (:data @(:db c)) (db/now))]
      (is (= "MYRDB001" (String. (java.util.Arrays/copyOfRange bs 0 8) "UTF-8"))))))

(deftest empty-keyspace-serializes
  (let [c (ctx)
        f (File/createTempFile "empty" ".rdb")]
    (rdb/write-file! f (:data @(:db c)) (db/now))
    (is (= [] (rdb/load-commands f)))
    (.delete f)))

(deftest load-of-missing-file-is-empty
  (let [f (File. "/tmp/definitely-does-not-exist-12345.rdb")]
    (is (= [] (rdb/load-commands f)))))

;; ---------- 往復 ----------

(deftest roundtrip-all-types
  (let [c (ctx)
        f (File/createTempFile "rtrip" ".rdb")]
    (try
      (run c "SET" "s" "hello")
      (run c "RPUSH" "l" "a" "b" "c")
      (run c "HSET" "h" "f1" "v1" "f2" "v2")
      (run c "SADD" "st" "x" "y")
      (run c "ZADD" "z" "1" "a" "2.5" "b" "inf" "c" "-inf" "d")

      (let [c2 (roundtrip c f)]
        (is (= 5 (run c2 "DBSIZE")))
        (is (= "hello" (run c2 "GET" "s")))
        (is (= ["a" "b" "c"] (run c2 "LRANGE" "l" "0" "-1")) "リストの順序が保たれる")
        (is (= #{["f1" "v1"] ["f2" "v2"]} (set (partition 2 (run c2 "HGETALL" "h")))))
        (is (= #{"x" "y"} (set (run c2 "SMEMBERS" "st"))))
        (is (= ["d" "a" "b" "c"] (run c2 "ZRANGE" "z" "0" "-1")))
        (is (= "2.5" (run c2 "ZSCORE" "z" "b")))
        (is (= "inf" (run c2 "ZSCORE" "z" "c")))
        (is (= "-inf" (run c2 "ZSCORE" "z" "d"))))
      (finally (.delete f)))))

(deftest roundtrip-preserves-ttl
  (let [c (ctx)
        f (File/createTempFile "ttl" ".rdb")]
    (try
      (run c "SET" "t" "v" "EX" "1000")
      (run c "SET" "nottl" "v")
      (Thread/sleep 1100)
      (let [c2 (roundtrip c f)]
        (is (<= 998 (run c2 "TTL" "t") 999) "期限が延びていない")
        (is (= -1 (run c2 "TTL" "nottl"))))
      (finally (.delete f)))))

(deftest expired-keys-are-not-saved
  (let [c (ctx)
        f (File/createTempFile "exp" ".rdb")]
    (try
      (run c "SET" "live" "v")
      (run c "SET" "dead" "v" "PX" "50")
      (Thread/sleep 100)
      (is (= 2 (db/key-count (:db c))) "物理的には残っている")
      (let [c2 (roundtrip c f)]
        (is (= 1 (run c2 "DBSIZE")))
        (is (nil? (run c2 "GET" "dead"))))
      (finally (.delete f)))))

(deftest roundtrip-with-special-characters
  (testing "マルチバイト・空文字列・改行を含む値"
    (let [c (ctx)
          f (File/createTempFile "utf" ".rdb")]
      (try
        (run c "SET" "日本語キー" "日本語の値")
        (run c "SET" "empty" "")
        (run c "SET" "newline" "a\r\nb")
        (let [c2 (roundtrip c f)]
          (is (= "日本語の値" (run c2 "GET" "日本語キー")))
          (is (= "" (run c2 "GET" "empty")))
          (is (= 1 (run c2 "EXISTS" "empty")) "空文字列と不在が区別される")
          (is (= "a\r\nb" (run c2 "GET" "newline"))))
        (finally (.delete f))))))

(deftest roundtrip-large-collection
  (let [c (ctx)
        f (File/createTempFile "big" ".rdb")]
    (try
      (command/dispatch c (into ["RPUSH" "big"] (map str (range 5000))))
      (let [c2 (roundtrip c f)]
        (is (= 5000 (run c2 "LLEN" "big")))
        (is (= (mapv str (range 5000)) (run c2 "LRANGE" "big" "0" "-1"))))
      (finally (.delete f)))))

(deftest unknown-type-throws-on-write
  (is (thrown? clojure.lang.ExceptionInfo
               (rdb/serialize {"k" {:type :stream :value []}} (db/now)))))

;; ---------- 破損の検出 ----------

(defn- write-bytes!
  [^File f ^bytes bs]
  (with-open [out (FileOutputStream. f)]
    (.write out bs)))

(defn- corrupt-byte! [^File src ^File dest ^long idx]
  (let [bs (Files/readAllBytes (.toPath src))]
    (aset-byte bs idx (if (zero? (aget bs idx)) (byte 1) (byte 0)))
    (write-bytes! dest bs)))

(deftest corrupted-file-is-rejected
  (let [c (ctx)
        f (File/createTempFile "good" ".rdb")
        b (File/createTempFile "bad" ".rdb")]
    (try
      (dotimes [i 20] (run c "SET" (str "k" i) (str "value" i)))
      (rdb/write-file! f (:data @(:db c)) (db/now))
      (testing "正常なファイルは読める"
        (is (pos? (count (rdb/load-commands f)))))
      (testing "1 バイト壊すと checksum mismatch になる"
        (corrupt-byte! f b 20)
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"checksum mismatch"
                              (rdb/load-commands b))))
      (finally (.delete f) (.delete b)))))

(deftest bad-magic-is-rejected
  (let [f (File/createTempFile "notrdb" ".rdb")]
    (try
      (write-bytes! f (.getBytes "this is not an rdb file at all" "UTF-8"))
      (is (thrown? clojure.lang.ExceptionInfo (rdb/load-commands f)))
      (finally (.delete f)))))

(deftest truncated-file-is-rejected
  (let [c (ctx)
        f (File/createTempFile "trunc" ".rdb")
        t (File/createTempFile "trunc2" ".rdb")]
    (try
      (dotimes [i 20] (run c "SET" (str "k" i) "v"))
      (rdb/write-file! f (:data @(:db c)) (db/now))
      (let [bs (Files/readAllBytes (.toPath f))]
        (write-bytes! t (java.util.Arrays/copyOfRange bs 0 (- (alength bs) 20))))
      (is (thrown? clojure.lang.ExceptionInfo (rdb/load-commands t)))
      (finally (.delete f) (.delete t)))))

;; ---------- サーバ経由 ----------

(deftest save-and-restart
  (let [dir (temp-dir)]
    (try
      (let [srv (server/start! 0 {:dir (str dir) :appendonly false :verbose? false})
            ex  (:executor @srv)]
        (try
          (executor/submit! ex ["SET" "k" "v"])
          (executor/submit! ex ["RPUSH" "l" "a" "b"])
          (executor/submit! ex ["ZADD" "z" "1.5" "m"])
          (executor/submit! ex ["SET" "t" "v" "EX" "1000"])
          (is (= "OK" (:value (executor/submit! ex ["SAVE"]))))
          (is (.exists (File. dir "dump.rdb")))
          (finally (server/stop! srv))))

      (let [srv2 (server/start! 0 {:dir (str dir) :appendonly false :verbose? false})
            ex2  (:executor @srv2)]
        (try
          (is (= 4 (executor/submit! ex2 ["DBSIZE"])))
          (is (= "v" (executor/submit! ex2 ["GET" "k"])))
          (is (= ["a" "b"] (executor/submit! ex2 ["LRANGE" "l" "0" "-1"])))
          (is (= "1.5" (executor/submit! ex2 ["ZSCORE" "z" "m"])))
          (is (<= 999 (executor/submit! ex2 ["TTL" "t"]) 1000))
          (finally (server/stop! srv2))))
      (finally (delete-tree dir)))))

(deftest bgsave-writes-file
  (let [dir (temp-dir)]
    (try
      (let [srv (server/start! 0 {:dir (str dir) :appendonly false :verbose? false})
            ex  (:executor @srv)]
        (try
          (dotimes [i 1000] (executor/submit! ex ["SET" (str "k" i) "v"]))
          (is (= "Background saving started" (:value (executor/submit! ex ["BGSAVE"]))))
          (loop [n 0]
            (when (and (not (.exists (File. dir "dump.rdb"))) (< n 100))
              (Thread/sleep 50)
              (recur (inc n))))
          (Thread/sleep 200)
          (is (.exists (File. dir "dump.rdb")))
          (is (pos? (executor/submit! ex ["LASTSAVE"])))
          (finally (server/stop! srv))))
      (finally (delete-tree dir)))))

(deftest aof-takes-precedence-over-rdb
  (testing "appendonly 有効なら RDB は読まれない"
    (let [dir (temp-dir)]
      (try
        ;; RDB だけに "from-rdb" を残す
        (let [srv (server/start! 0 {:dir (str dir) :appendonly false :verbose? false})
              ex  (:executor @srv)]
          (executor/submit! ex ["SET" "from-rdb" "v"])
          (executor/submit! ex ["SAVE"])
          (server/stop! srv))

        ;; AOF 有効で起動し、別のキーを書く
        (let [srv (server/start! 0 {:dir (str dir) :appendonly true
                                    :appendfsync :always :verbose? false})
              ex  (:executor @srv)]
          (is (zero? (executor/submit! ex ["DBSIZE"])) "RDB は読まれていない")
          (executor/submit! ex ["SET" "from-aof" "v"])
          (server/stop! srv))

        ;; 再起動しても AOF 側だけ
        (let [srv (server/start! 0 {:dir (str dir) :appendonly true :verbose? false})
              ex  (:executor @srv)]
          (try
            (is (= 1 (executor/submit! ex ["DBSIZE"])))
            (is (= "v" (executor/submit! ex ["GET" "from-aof"])))
            (is (nil? (executor/submit! ex ["GET" "from-rdb"])))
            (finally (server/stop! srv))))
        (finally (delete-tree dir))))))

(deftest save-is-atomic
  (testing "書き出し中に落ちても中途半端なファイルが残らない"
    (let [dir (temp-dir)]
      (try
        (let [srv (server/start! 0 {:dir (str dir) :appendonly false :verbose? false})
              ex  (:executor @srv)]
          (try
            (dotimes [i 100] (executor/submit! ex ["SET" (str "k" i) "v"]))
            (executor/submit! ex ["SAVE"])
            ;; 一時ファイルが残っていない
            (is (not (.exists (File. dir "dump.rdb.tmp"))))
            ;; 2 回目の SAVE でも壊れない
            (executor/submit! ex ["SAVE"])
            (is (= 100 (count (filter #(= "SET" (first %))
                                      (rdb/load-commands (File. dir "dump.rdb"))))))
            (finally (server/stop! srv))))
        (finally (delete-tree dir))))))
