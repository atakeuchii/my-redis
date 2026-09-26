(ns my-redis.expiry-test
  (:require [clojure.test :refer [deftest is testing]]
            [my-redis.command :as command]
            [my-redis.db :as db]
            [my-redis.resp :as resp]))

(defn- ctx [] {:db (db/create)})
(defn- run [c & args] (command/dispatch c (vec args)))
(defn- err-msg [r] (when (resp/error? r) (:message r)))

;; 時刻を進めるヘルパ。db/now を差し替える
(defmacro after-ms [ms & body]
  `(let [base# (System/currentTimeMillis)]
     (with-redefs [db/now (fn [] (+ base# ~ms))]
       ~@body)))

;; ---------- TTL コマンド ----------

(deftest ttl-on-key-without-expire
  (let [c (ctx)]
    (run c "SET" "k" "v")
    (is (= -1 (run c "TTL" "k")))
    (is (= -1 (run c "PTTL" "k")))))

(deftest ttl-on-missing-key
  (is (= -2 (run (ctx) "TTL" "nokey"))))

(deftest expire-sets-ttl
  (let [c (ctx)]
    (run c "SET" "k" "v")
    (is (= 1 (run c "EXPIRE" "k" "10")))
    (is (= 10 (run c "TTL" "k")))
    (is (<= 9000 (run c "PTTL" "k") 10000))))

(deftest expire-on-missing-key
  (is (= 0 (run (ctx) "EXPIRE" "nokey" "10"))))

(deftest pexpire-uses-milliseconds
  (let [c (ctx)]
    (run c "SET" "k" "v")
    (run c "PEXPIRE" "k" "5000")
    (is (= 5 (run c "TTL" "k")))))

(deftest expireat-uses-absolute-time
  (let [c (ctx)
        target (+ (quot (System/currentTimeMillis) 1000) 100)]
    (run c "SET" "k" "v")
    (is (= 1 (run c "EXPIREAT" "k" (str target))))
    (is (<= 99 (run c "TTL" "k") 100))))

(deftest persist-removes-expire
  (let [c (ctx)]
    (run c "SET" "k" "v" "EX" "100")
    (is (= 1 (run c "PERSIST" "k")))
    (is (= -1 (run c "TTL" "k")))
    (testing "既に期限が無ければ 0"
      (is (= 0 (run c "PERSIST" "k"))))))

(deftest ttl-non-integer
  (is (= "ERR value is not an integer or out of range"
         (err-msg (run (ctx) "EXPIRE" "k" "abc")))))

;; ---------- 受動的期限切れ ----------

(deftest expired-key-is-invisible
  (let [c (ctx)]
    (run c "SET" "k" "v" "EX" "10")
    (after-ms 11000
      (is (nil? (run c "GET" "k")))
      (is (= 0 (run c "EXISTS" "k")))
      (is (= -2 (run c "TTL" "k")))
      (is (= "none" (:value (run c "TYPE" "k"))))
      (is (= [] (run c "KEYS" "*")))
      (is (= 0 (run c "DBSIZE"))))))

(deftest expired-key-is-removed-on-read
  (testing "読み取りが削除を引き起こす"
    (let [c (ctx)
          d (:db c)]
      (run c "SET" "k" "v" "EX" "10")
      (after-ms 11000
        (is (= 1 (count (:data @d))) "まだ物理的には残っている")
        (run c "GET" "k")
        (is (= 0 (count (:data @d))) "読み取りで削除された")
        (is (= 0 (count (:expires @d))) ":expires からも消えた")))))

(deftest expired-key-not-visible-in-collections
  (let [c (ctx)]
    (run c "SET" "a" "1")
    (run c "SET" "b" "2" "EX" "10")
    (after-ms 11000
      (is (= ["a"] (run c "KEYS" "*")))
      (is (= ["1" nil] (run c "MGET" "a" "b")))
      (is (= 1 (run c "DBSIZE"))))))

(deftest expired-key-can-be-recreated
  (let [c (ctx)]
    (run c "SET" "k" "old" "EX" "10")
    (after-ms 11000
      (run c "SET" "k" "new")
      (is (= "new" (run c "GET" "k")))
      (is (= -1 (run c "TTL" "k"))))))

(deftest expired-list-is-gone
  (testing "String 以外の型も期限切れになる"
    (let [c (ctx)]
      (run c "RPUSH" "l" "a" "b")
      (run c "EXPIRE" "l" "10")
      (after-ms 11000
        (is (= [] (run c "LRANGE" "l" "0" "-1")))
        (is (= 0 (run c "LLEN" "l")))
        (is (= 0 (run c "EXISTS" "l")))))))

;; ---------- 期限の継承 ----------

(deftest set-clears-expire
  (let [c (ctx)]
    (run c "SET" "k" "v" "EX" "100")
    (run c "SET" "k" "new")
    (is (= -1 (run c "TTL" "k")))))

(deftest append-keeps-expire
  (let [c (ctx)]
    (run c "SET" "k" "hello" "EX" "100")
    (run c "APPEND" "k" " world")
    (is (<= 99 (run c "TTL" "k") 100))
    (is (= "hello world" (run c "GET" "k")))))

(deftest incr-keeps-expire
  (let [c (ctx)]
    (run c "SET" "n" "1" "EX" "100")
    (run c "INCR" "n")
    (is (<= 99 (run c "TTL" "n") 100))))

(deftest rpush-keeps-expire
  (let [c (ctx)]
    (run c "RPUSH" "l" "a")
    (run c "EXPIRE" "l" "100")
    (run c "RPUSH" "l" "b")
    (is (<= 99 (run c "TTL" "l") 100))))

(deftest getset-keeps-expire
  (testing "GETSET は本物では期限を消すが、今回は引き継ぐ（既知の差分）"
    (let [c (ctx)]
      (run c "SET" "k" "old" "EX" "100")
      (run c "GETSET" "k" "new")
      (is (some? (run c "TTL" "k"))))))

;; ---------- :expires の整合性 ----------

(deftest expires-index-is-maintained
  (let [c (ctx)
        d (:db c)]
    (run c "SET" "a" "1")
    (is (zero? (db/expires-count d)))

    (run c "EXPIRE" "a" "100")
    (is (= 1 (db/expires-count d)))

    (run c "PERSIST" "a")
    (is (zero? (db/expires-count d)))

    (run c "SET" "b" "2" "EX" "100")
    (is (= 1 (db/expires-count d)))

    (run c "DEL" "b")
    (is (zero? (db/expires-count d)) "削除で :expires からも消える")))

(deftest set-without-ttl-clears-expires-index
  (let [c (ctx)
        d (:db c)]
    (run c "SET" "k" "v" "EX" "100")
    (is (= 1 (db/expires-count d)))
    (run c "SET" "k" "new")
    (is (zero? (db/expires-count d)))))

;; ---------- 能動的期限切れ ----------

(deftest expire-cycle-removes-unreferenced-keys
  (testing "参照しなくても回収される"
    (let [c (ctx)
          d (:db c)]
      (dotimes [i 50] (run c "SET" (str "k" i) "v" "EX" "10"))
      (after-ms 11000
        (is (= 50 (count (:data @d))) "まだ残っている")
        ;; 十分な回数サイクルを回す
        (dotimes [_ 20] (db/expire-cycle! d 10))
        (is (zero? (count (:data @d))))
        (is (zero? (db/expires-count d)))))))

(deftest expire-cycle-skips-live-keys
  (let [c (ctx)
        d (:db c)]
    (dotimes [i 50] (run c "SET" (str "k" i) "v" "EX" "1000"))
    (dotimes [_ 20] (db/expire-cycle! d 10))
    (is (= 50 (count (:data @d))) "期限内のキーは消えない")))

(deftest expire-cycle-is-cheap-without-expiring-keys
  (testing "期限付きキーが無ければ、キー数に関わらず即座に終わる"
    (let [c (ctx)
          d (:db c)]
      (dotimes [i 20000] (run c "SET" (str "k" i) "v"))
      (let [start (System/nanoTime)]
        (dotimes [_ 100] (db/expire-cycle! d 1))
        (let [ms (/ (- (System/nanoTime) start) 1e6)]
          (is (< ms 100) (str "100 サイクルで " ms "ms かかった")))))))

(deftest expire-cycle-respects-time-limit
  (testing "大量の期限切れがあっても1サイクルの時間が有界"
    (let [c (ctx)
          d (:db c)]
      (dotimes [i 10000] (run c "SET" (str "k" i) "v" "EX" "10"))
      (after-ms 11000
        (let [start (System/nanoTime)]
          (db/expire-cycle! d 5)
          (let [ms (/ (- (System/nanoTime) start) 1e6)]
            (is (< ms 100) (str "1 サイクルに " ms "ms かかった"))))
        (is (pos? (count (:data @d))) "一度では消えきらない")))))

(deftest expire-cycle-eventually-drains
  (testing "繰り返せば最終的に全部消える"
    (let [c (ctx)
          d (:db c)]
      (dotimes [i 2000] (run c "SET" (str "k" i) "v" "EX" "10"))
      (after-ms 11000
        (loop [n 0]
          (when (and (pos? (db/expires-count d)) (< n 1000))
            (db/expire-cycle! d 5)
            (recur (inc n))))
        (is (zero? (count (:data @d))))))))
