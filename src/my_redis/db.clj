(ns my-redis.db
  (:refer-clojure :exclude [keys]))

(defn create 
  []
  (atom {}))

(defn entry
  [type value]
  {:type type :value value})

(defn get-entry
  [db k]
  (get @db k))

(defn get-value
  [db k]
  (:value (get-entry db k)))

(defn exists?
  [db k]
  (contains? @db k))

(defn key-type
  [db k]
  (:type (get-entry db k)))

(defn keys
  [db]
  (into [] (clojure.core/keys @db)))

(defn size
  [db]
  (count @db))

(defn set-entry!
  [db k entry]
  (swap! db assoc k entry)
  nil)

(defn set-value!
  [db k type value]
  (set-entry! db k (entry type value))
  nil)

(defn delete!
  [db k]
  (let [[old _] (swap-vals! db dissoc k)]
    (contains? old k)))

(defn delete-many!
  [db ks]
  (let [uniq (distinct ks)
        [old _] (swap-vals! db #(apply dissoc % uniq))]
    (count (filter #(contains? old %) uniq))))

(defn clear!
  [db]
  (reset! db {})
  nil)

(defn update-entry!
  [db k f]
  (let [[_ new-db] (swap-vals! db
                               (fn [m]
                                 (let [next (f (get m k))]
                                   (if (nil? next)
                                     (dissoc m k)
                                     (assoc m k next)))))]
    (get new-db k)))

(defn update-value!
  [db k type f]
  (:value (update-entry! db k
                         (fn [e]
                           (let [next (f (:value e))]
                             (when (some? next)
                               (entry type next)))))))
