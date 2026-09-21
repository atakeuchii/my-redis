(ns my-redis.types.zset
  (:refer-clojure :exclude [remove]))

(defn- entry-compare [[s1 m1] [s2 m2]]
  (let [c (Double/compare s1 s2)]
    (if (zero? c)
      (compare m1 m2)
      c)))

(defrecord ZSet [scores sorted])

(def empty-zset
  (->ZSet {} (sorted-set-by entry-compare)))

(defn card
  ^long [^ZSet z]
  (count (:scores z)))

(defn score
  [^ZSet z member]
  (get (:scores z) member))

(defn add
  [^ZSet z member ^double s]
  (let [old (get (:scores z) member)]
    (->ZSet (assoc (:scores z) member s)
            (-> (:sorted z)
                (cond-> old (disj [old member]))
                (conj [s member])))))

(defn remove
  [^ZSet z member]
  (if-let [old (get (:scores z) member)]
    (->ZSet (dissoc (:scores z) member)
            (disj (:sorted z) [old member]))
    z))

(defn empty-zset?
  [^ZSet z]
  (zero? (card z)))

(defn entries
  [^ZSet z]
  (seq (:sorted z)))

(defn range-by-rank
  [^ZSet z ^long from ^long to]
  (->> (:sorted z)
       (drop from)
       (take (inc (- to from)))
       (into [])))

(defn rank
  [^ZSet z member]
  (when-let [s (get (:scores z) member)]
    (let [target [s member]]
      (count (take-while #(neg? (entry-compare % target)) (:sorted z))))))

(defn range-by-score
  [^ZSet z min min-excl? max max-excl?]
  (->> (subseq (:sorted z) >= [min nil])
       (drop-while (fn [[s _]] (and min-excl? (== s min))))
       (take-while (fn [[s _]] (if max-excl? (< s max) (<= s max))))
       (into [])))

