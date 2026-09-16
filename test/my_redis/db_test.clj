(ns my-redis.db-test
  (:require [clojure.test :refer [deftest is testing]]
            [my-redis.db :as db]))

(deftest set-and-get
  (let [d (db/create)]
    (db/set-value! d "k" :string "v")
    (is (= "v" (db/get-value d "k")))
    (is (= {:type :string :value "v"} (db/get-entry d "k")))))

(deftest missing-key-returns-nil
  (let [d (db/create)]
    (is (nil? (db/get-value d "nope")))
    (is (nil? (db/get-entry d "nope")))
    (is (nil? (db/key-type d "nope")))
    (is (false? (db/exists? d "nope")))))

(deftest empty-string-is-not-nil
  (testing "空文字列は値として保持され、キーは存在する"
    (let [d (db/create)]
      (db/set-value! d "k" :string "")
      (is (= "" (db/get-value d "k")))
      (is (true? (db/exists? d "k"))))))

(deftest type-tag-is-preserved
  (let [d (db/create)]
    (db/set-value! d "s" :string "v")
    (db/set-value! d "l" :list ["a"])
    (is (= :string (db/key-type d "s")))
    (is (= :list (db/key-type d "l")))))

(deftest delete-returns-whether-existed
  (let [d (db/create)]
    (db/set-value! d "k" :string "v")
    (is (true? (db/delete! d "k")))
    (is (false? (db/delete! d "k")))))

(deftest delete-many-counts-distinct-keys
  (testing "重複した引数は1回だけ数える"
    (let [d (db/create)]
      (db/set-value! d "k" :string "v")
      (is (= 1 (db/delete-many! d ["k" "k" "k"]))))))

(deftest delete-many-counts-only-existing
  (let [d (db/create)]
    (db/set-value! d "a" :string "1")
    (db/set-value! d "b" :string "2")
    (is (= 2 (db/delete-many! d ["a" "b" "nope"])))
    (is (zero? (db/size d)))))

(deftest update-value-nil-deletes-key
  (testing "f が nil を返したらキーごと削除される"
    (let [d (db/create)]
      (db/set-value! d "l" :list ["a"])
      (db/update-value! d "l" :list #(not-empty (vec (rest %))))
      (is (false? (db/exists? d "l"))))))

(deftest update-value-keeps-key-when-non-nil
  (let [d (db/create)]
    (db/set-value! d "l" :list ["a" "b"])
    (is (= ["b"] (db/update-value! d "l" :list #(not-empty (vec (rest %))))))
    (is (true? (db/exists? d "l")))))

(deftest update-value-on-missing-key
  (testing "存在しないキーへの更新は f に nil が渡る"
    (let [d (db/create)]
      (is (= ["x"] (db/update-value! d "l" :list (fn [v] (conj (vec v) "x")))))
      (is (= :list (db/key-type d "l"))))))

;; ---------- 並行性 ----------

(deftest concurrent-updates-do-not-lose-writes
  (testing "複数スレッドからの更新が失われない"
    (let [d (db/create)
          n 100
          per-thread 100]
      (db/set-value! d "c" :string "0")
      (->> (range n)
           (map (fn [_]
                  (future
                    (dotimes [_ per-thread]
                      (db/update-value! d "c" :string
                                        #(str (inc (Long/parseLong %))))))))
           doall
           (run! deref))
      (is (= (str (* n per-thread)) (db/get-value d "c"))))))

(deftest delete-many-is-atomic
  (testing "DEL の途中経過が観測されない"
    (let [d (db/create)
          ks (mapv #(str "k" %) (range 20))
          observed (atom #{})
          watching (atom true)]
      (doseq [k ks] (db/set-value! d k :string k))
      (let [watcher (future (while @watching (swap! observed conj (db/size d))))]
        (db/delete-many! d ks)
        (Thread/sleep 30)
        (reset! watching false)
        @watcher)
      (is (empty? (disj @observed 20 0))
          (str "中間状態が観測された: " @observed)))))
