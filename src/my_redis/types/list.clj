(ns my-redis.types.list
  (:refer-clojure :exclude [count empty? nth seq]))

(defrecord DList [front back])

(def empty-list (->DList [] []))

(defn count ^long [^DList l]
  (+ (clojure.core/count (:front l))
     (clojure.core/count (:back l))))

(defn empty? [^DList l]
  (zero? (count l)))

(defn push-left [^DList l x]
  (update l :front conj x))

(defn push-right [^DList l x]
  (update l :back conj x))

(defn- rebalance-front [^DList l]
  (let [b (:back l)
        n (clojure.core/count b)]
    (if (zero? n)
      l
      (let [half (quot (inc n) 2)
            moved (into [] (reverse (subvec b 0 half)))]
        (->DList moved (subvec b half))))))

(defn- rebalance-back [^DList l]
  (let [f (:front l)
        n (clojure.core/count f)]
    (if (zero? n)
      l
      (let [half (quot (inc n) 2)
            moved (into [] (reverse (subvec f 0 half)))]
        (->DList (subvec f half) moved)))))

(defn peek-left [^DList l]
  (if (clojure.core/seq (:front l))
    (peek (:front l))
    (first (:back l))))

(defn peek-right [^DList l]
  (if (clojure.core/seq (:back l))
    (peek (:back l))
    (first (:front l))))

(defn pop-left [^DList l]
  (cond
    (clojure.core/seq (:front l)) (update l :front pop)
    (clojure.core/seq (:back l))  (-> l rebalance-front (update :front pop))
    :else l))

(defn pop-right [^DList l]
  (cond
    (clojure.core/seq (:back l))  (update l :back pop)
    (clojure.core/seq (:front l)) (-> l rebalance-back (update :back pop))
    :else l))

(defn nth [^DList l ^long i]
  (let [fc (clojure.core/count (:front l))]
    (cond
      (neg? i) nil
      (< i fc) (clojure.core/nth (:front l) (- fc 1 i))
      :else    (clojure.core/nth (:back l) (- i fc) nil))))

(defn seq [^DList l]
  (concat (reverse (:front l)) (:back l)))

(defn to-vec [^DList l]
  (into [] (seq l)))

(defn from-seq [xs]
  (->DList [] (vec xs)))

(defn assoc-nth [^DList l ^long i x]
  (let [fc (clojure.core/count (:front l))]
    (cond
      (neg? i) l
      (< i fc) (update l :front assoc (- fc 1 i) x)
      (< (- i fc) (clojure.core/count (:back l))) (update l :back assoc (- i fc) x)
      :else l)))

(defn subrange [^DList l ^long from ^long to]
  (into [] (map #(nth l %)) (range from (inc to))))

(defn remove-value
  "value に等しい要素を削除した新しいリストと、削除した個数を [list n] で返す。
   n-limit が 0 なら全件、正なら先頭から、負なら末尾から n-limit 件まで。"
  [^DList l value ^long n-limit]
  (let [xs      (vec (seq l))
        reverse? (neg? n-limit)
        limit   (if (zero? n-limit) Long/MAX_VALUE (Math/abs n-limit))
        ordered (if reverse? (rseq xs) xs)
        [kept removed]
        (reduce (fn [[acc n] x]
                  (if (and (< n limit) (= x value))
                    [acc (inc n)]
                    [(conj acc x) n]))
                [[] 0]
                ordered)]
    [(from-seq (if reverse? (rseq kept) kept)) removed]))

(defn trim
  "順位 [from to]（両端含む、正規化済み）だけを残した新しいリストを返す。"
  [^DList l ^long from ^long to]
  (from-seq (subrange l from to)))
