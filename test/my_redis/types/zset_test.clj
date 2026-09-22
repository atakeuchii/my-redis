(ns my-redis.types.zset-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [my-redis.types.zset :as zset]))

;; 参照モデル: member→score のマップ。問い合わせのたびにソートする。
(defn- model-sorted [m]
  (sort-by (juxt val key) m))

(defn- model-rank [m member]
  (when (contains? m member)
    (count (take-while #(not= member (key %)) (model-sorted m)))))

(def gen-member (gen/elements (map str "abcdefghij")))
(def gen-score  (gen/elements [-2.0 -1.0 0.0 1.0 1.0 2.0 2.5 3.0]))  ; 同点が起きやすいように

(def gen-op
  (gen/one-of
   [(gen/tuple (gen/return :add) gen-member gen-score)
    (gen/tuple (gen/return :remove) gen-member)]))

(defn- apply-op [[z m] [op member s]]
  (case op
    :add    [(zset/add z member s) (assoc m member s)]
    :remove [(zset/remove z member) (dissoc m member)]))

(deftest zset-matches-model
  (let [result
        (tc/quick-check
         300
         (prop/for-all [ops (gen/vector gen-op 0 60)]
                       (let [[z m] (reduce apply-op [zset/empty-zset {}] ops)
                             members (keys m)]
                         (and (= (count m) (zset/card z))
                              (= (mapv (fn [[k v]] [v k]) (model-sorted m))
                                 (vec (zset/entries z)))
                              (every? #(= (model-rank m %) (zset/rank z %)) members)
                              (every? #(= (get m %) (zset/score z %)) members)))))]
    (is (:pass? result) (pr-str (:shrunk result)))))
