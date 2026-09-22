(ns my-redis.types.zset
  (:refer-clojure :exclude [remove])
  (:require [my-redis.types.skiplist :as sl]))

(defrecord ZSet [scores sorted])

(def empty-zset (->ZSet {} nil))

(defn card ^long [^ZSet z]
  (count (:scores z)))

(defn score [^ZSet z member]
  (get (:scores z) member))

(defn add [^ZSet z member ^double s]
  (let [old (get (:scores z) member)]
    (if (and old (== old s))
      z
      (let [skl (or (:sorted z) (sl/create))]
        (when old (sl/delete! skl old member))
        (sl/insert! skl s member)
        (->ZSet (assoc (:scores z) member s) skl)))))

(defn remove [^ZSet z member]
  (if-let [old (get (:scores z) member)]
    (do (sl/delete! (:sorted z) old member)
        (->ZSet (dissoc (:scores z) member)
                (:sorted z)))
    z))

(defn empty-zset? [^ZSet z]
  (zero? (card z)))

(defn entries [^ZSet z]
  (if-let [skl (:sorted z)]
    (sl/entries skl)
    []))

(defn range-by-rank [^ZSet z ^long from ^long to]
  (if-let [skl (:sorted z)]
    (sl/range-by-rank skl (inc from) (inc to))
    []))

(defn rank [^ZSet z member]
  (when-let [s (score z member)]
    (dec (sl/rank (:sorted z) s member))))

(defn range-by-score [^ZSet z min min-excl? max max-excl?]
  (if-let [skl (:sorted z)]
    (sl/range-by-score skl min min-excl? max max-excl?)
    []))
