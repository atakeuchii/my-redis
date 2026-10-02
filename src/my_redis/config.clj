(ns my-redis.config)

(def defaults
  {"maxkeys"          "0"
   "maxmemory-policy" "noeviction"
   "maxmemory-samples" "5"
   "auto-aof-rewrite-percentage" "100"
   "auto-aof-rewrite-min-size"   "67108864"})

(def valid-policies
  #{"noeviction" "allkeys-random" "allkeys-lru" "volatile-lru"})

(defn create []
  (atom defaults))

(defn get-raw [config k]
  (get @config k))

(defn get-long ^long [config k ^long default]
  (if-let [v (get-raw config k)]
    (try (Long/parseLong v) 
         (catch NumberFormatException _ default))
    default))

(defn set-raw! [config k v]
  (swap! config assoc k v)
  nil)

(defn known? [config k]
  (contains? @config k))

(defn matching [config pattern-fn]
  (into [] (mapcat (fn [[k v]]
                     (when (pattern-fn k) [k v])))
        @config))
