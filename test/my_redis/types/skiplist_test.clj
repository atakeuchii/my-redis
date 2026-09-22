(ns my-redis.types.skiplist-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [my-redis.types.skiplist :as sl]))

;; ---------- プロパティテスト ----------

;; 同点が起きやすいよう、スコアと member の候補を絞る
(def gen-pair
  (gen/tuple (gen/elements [-1.0 0.0 1.0 1.0 2.0 3.5])
             (gen/elements (map str "abcdefgh"))))

(def gen-op
  (gen/tuple (gen/elements [:insert :insert :delete]) gen-pair))

(defn- run-ops
  "操作列を skiplist と参照モデル（sorted-set）の両方に適用する。
   skiplist は重複を防がないので、モデルにある組の insert は飛ばす。"
  [ops]
  (reduce (fn [[s model ok?] [op [sc m :as pair]]]
            (case op
              :insert (if (contains? model pair)
                        [s model ok?]
                        (do (sl/insert! s sc m)
                            [s (conj model pair) ok?]))
              :delete (let [deleted? (sl/delete! s sc m)]
                        [s (disj model pair)
                         (and ok? (= deleted? (contains? model pair)))])))
          [(sl/create) (sorted-set) true]
          ops))

(deftest skiplist-matches-model
  (let [result
        (tc/quick-check
         300
         (prop/for-all [ops (gen/vector gen-op 0 80)]
                       (let [[s model ok?] (run-ops ops)
                             es (vec model)]
                         (and ok?
                              (= es (sl/entries s))
                              (= (count es) (sl/length s))
                              (sl/check-spans s)
                              (every? true? (map-indexed (fn [i [sc m]] (= (inc i) (sl/rank s sc m))) es))
                              (every? true? (map-indexed (fn [i e] (= [e] (sl/range-by-rank s (inc i) (inc i)))) es))))))]
    (is (:pass? result) (pr-str (:shrunk result)))))

;; ---------- 個別のケース ----------

(defn- build [pairs]
  (let [s (sl/create)]
    (doseq [[sc m] pairs] (sl/insert! s (double sc) m))
    s))

(deftest ties-ordered-by-member
  (let [s (build [[1 "banana"] [1 "apple"] [1 "cherry"]])]
    (is (= [[1.0 "apple"] [1.0 "banana"] [1.0 "cherry"]] (sl/entries s)))
    (is (= 3 (sl/rank s 1.0 "cherry")))))

(deftest find-node-and-missing
  (let [s (build [[10 "a"] [20 "b"]])]
    (is (some? (sl/find-node s 20.0 "b")))
    (is (nil? (sl/find-node s 20.0 "zz")))
    (is (nil? (sl/rank s 99.0 "nope")))
    (is (false? (sl/delete! s 99.0 "nope")))))

(deftest range-by-rank-edges
  (let [s (build [[0 "m0"] [1 "m1"] [2 "m2"] [3 "m3"] [4 "m4"]])]
    (is (= [[1.0 "m1"] [2.0 "m2"]] (sl/range-by-rank s 2 3)))
    (testing "順位 0 は header を指さず空になる"
      (is (= [] (sl/range-by-rank s 0 2))))
    (testing "末尾を超えた分は切り詰められる"
      (is (= [[4.0 "m4"]] (sl/range-by-rank s 5 99))))
    (testing "範囲外は空"
      (is (= [] (sl/range-by-rank s 6 9))))))

(deftest range-by-score-bounds
  (let [s (build [[10 "a"] [20 "b"] [30 "c"] [40 "d"]])]
    (is (= [[20.0 "b"] [30.0 "c"]] (sl/range-by-score s 20 false 30 false)))
    (is (= [[30.0 "c"]]            (sl/range-by-score s 20 true 30 false)))
    (is (= []                      (sl/range-by-score s 20 true 30 true)))
    (is (= []                      (sl/range-by-score s 30 false 10 false)))
    (is (= 4 (count (sl/range-by-score s Double/NEGATIVE_INFINITY false
                                       Double/POSITIVE_INFINITY false))))
    (is (= [] (sl/range-by-score (sl/create) 0 false 100 false)))))

(deftest levels-collapse-after-deleting-all
  (let [s (sl/create)]
    (dotimes [i 1000] (sl/insert! s (double i) (str "m" i)))
    (is (> (count (sl/level-counts s)) 1))
    (dotimes [i 1000] (sl/delete! s (double i) (str "m" i)))
    (is (= [0] (sl/level-counts s)))
    (is (zero? (sl/length s)))
    (testing "空にした後も再利用できる"
      (sl/insert! s 1.0 "x")
      (is (= [[1.0 "x"]] (sl/entries s)))
      (is (sl/check-spans s)))))
