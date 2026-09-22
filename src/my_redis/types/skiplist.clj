(ns my-redis.types.skiplist
  "Redis の zskiplist を模した可変 skiplist。順序は (score, member) の辞書順。
   可変構造であり、スレッド安全ではない。"
  (:import [java.util IdentityHashMap]))

(def ^:const max-level 32)
(def ^:const p 0.25)

(deftype Node [^double score ^String member ^objects forward ^longs span])

(deftype SkipList [^Node header ^longs state])

(defn- new-node ^Node [^double s ^String m ^long lvl]
  (->Node s m (object-array lvl) (long-array lvl)))

(defn create []
  (->SkipList (new-node 0.0 nil max-level)
              (long-array [1 0])))

(defn- level ^long [^SkipList sl]
  (aget ^longs (.state sl) 0))

(defn length ^long [^SkipList sl]
  (aget ^longs (.state sl) 1))

(defn- next-at ^Node [^Node n ^long i]
  (aget ^objects (.forward n) i))

(defn- span-at ^long [^Node n ^long i]
  (aget ^longs (.span n) i))

(defn- set-next! [^Node n ^long i x]
  (aset ^objects (.forward n) i x))

(defn- set-span! [^Node n ^long i ^long v]
  (aset ^longs (.span n) i v))

(defn- node-before? [^Node n ^double s ^String m]
  (let [c (Double/compare (.score n) s)]
    (or (neg? c)
        (and (zero? c) (neg? (.compareTo ^String (.member n) m))))))

(defn- node-at-or-before?
  "ノード n が (s, m) と同じか前か。対象そのものに着地したいとき（rank）に使う。"
  [^Node n ^double s ^String m]
  (let [c (Double/compare (.score n) s)]
    (or (neg? c)
        (and (zero? c) (<= (.compareTo ^String (.member n) m) 0)))))

(defn- random-level ^long []
  (loop [lvl 1]
    (if (and (< lvl max-level) (< (rand) p))
      (recur (inc lvl))
      lvl)))

(defn- predecessors
  "各段で (s, m) の直前にあるノードを update[i] に記録し、最下段の直前ノードを返す。"
  ^Node [^SkipList sl ^double s ^String m ^objects update]
  (loop [i (dec (level sl))
         x (.header sl)]
    (if (neg? i)
      x
      (let [x (loop [^Node x x]
                (let [nxt (next-at x i)]
                  (if (and nxt (node-before? nxt s m))
                    (recur nxt)
                    x)))]
        (aset update i x)
        (recur (dec i) x)))))

(defn- search-path [^SkipList sl ^double s ^String m]
  (let [^objects update (object-array max-level)
        ^longs rank (long-array max-level)
        top (dec (level sl))]
    (loop [i top
           x (.header sl)]
      (when-not (neg? i)
        (aset rank i (if (= i top) 0 (aget rank (inc i))))
        (let [x (loop [^Node x x]
                  (let [nxt (next-at x i)]
                    (if (and nxt (node-before? nxt s m))
                      (do (aset rank i (+ (aget rank i) (span-at x i)))
                          (recur nxt))
                      x)))]
          (aset update i x)
          (recur (dec i) x))))
    [update rank]))

;; (defn find-node
;;   "(s, m) のノードを返す。無ければ nil。"
;;   ^Node [^SkipList sl ^double s ^String m]
;;   (let [x (predecessors sl s m (object-array max-level))
;;         cand (next-at x 0)]
;;     (when (and cand (== (.score cand) s) (= (.member cand) m))
;;       cand)))
(defn find-node
  "(s, m) のノードを返す。無ければ nil。"
  ^Node [^SkipList sl ^double s ^String m]
  (let [[^objects update] (search-path sl s m)
        ^Node cand (next-at (aget update 0) 0)]
    (when (and cand (== (.score cand) s) (= (.member cand) m))
      cand)))

(defn insert!
  "(s, m) を挿入する。重複しないことは呼び出し側が保証する。"
  [^SkipList sl ^double s ^String m]
  (let [[^objects update ^longs rank] (search-path sl s m)
        lvl (random-level)
        cur (level sl)
        len (length sl)]
    (when (> lvl cur)
      (doseq [i (range cur lvl)]
        (aset rank (int i) 0)
        (aset update (int i) (.header sl))
        (set-span! (.header sl) i len))
      (aset ^longs (.state sl) 0 lvl))
    (let [node (new-node s m lvl)
          r0 (aget rank 0)]
      (dotimes [i lvl]
        (let [^Node prev (aget update i)
              d (- r0 (aget rank i))]
          (set-next! node i (next-at prev i))
          (set-next! prev i node)
          (set-span! node i (- (span-at prev i) d))
          (set-span! prev i (inc d))))
      (doseq [i (range lvl (level sl))]
        (let [^Node prev (aget update (int i))]
          (set-span! prev i (inc (span-at prev i)))))
      (aset ^longs (.state sl) 1 (inc len))
      node)))

(defn delete!
  "(s, m) を削除する。削除したら true、無ければ false。"
  [^SkipList sl ^double s ^String m]
  (let [[^objects update] (search-path sl s m)
        ^Node x (next-at (aget update 0) 0)]
    (if-not (and x (== (.score x) s) (= (.member x) m))
      false
      (do
        (dotimes [i (level sl)]
          (let [^Node prev (aget update i)]
            (if (identical? (next-at prev i) x)
              (do (set-span! prev i (+ (span-at prev i) (span-at x i) -1))
                  (set-next! prev i (next-at x i)))
              (set-span! prev i (dec (span-at prev i))))))
        (loop []
          (when (and (> (level sl) 1)
                     (nil? (next-at (.header sl) (dec (level sl)))))
            (aset ^longs (.state sl) 0 (dec (level sl)))
            (recur)))
        (aset ^longs (.state sl) 1 (dec (length sl)))
        true))))

(defn rank
  "(s, m) の順位（1始まり）。無ければ nil。
   「同じか前なら進む」で対象そのものに着地し、通った span を合計する。"
  [^SkipList sl ^double s ^String m]
  (loop [i (dec (level sl))
         ^Node x (.header sl)
         r 0]
    (when-not (neg? i)
      (let [nxt (next-at x i)]
        (cond
          (and nxt (node-at-or-before? nxt s m))
          (recur i nxt (+ r (span-at x i)))

          (and (some? (.member x)) (== (.score x) s) (= (.member x) m))
          r

          :else
          (recur (dec i) x r))))))

(defn node-at-rank
  "順位 r（1始まり）のノード。範囲外なら nil。"
  ^Node [^SkipList sl ^long r]
  (loop [i (dec (level sl))
         ^Node x (.header sl)
         traversed 0]
    (when-not (neg? i)
      (let [nxt (next-at x i)]
        (cond
          (and nxt (<= (+ traversed (span-at x i)) r))
          (recur i nxt (+ traversed (span-at x i)))

          (== traversed r)
          (when (pos? r) x)
          ;; x

          :else
          (recur (dec i) x traversed))))))

(defn range-by-rank
  "順位 [from to]（1始まり、両端含む）の [score member] をベクタで返す。
   開始位置まで O(log N)、そこから最下段を M 個歩く。"
  [^SkipList sl ^long from ^long to]
  (loop [^Node x (node-at-rank sl from)
         n (inc (- to from))
         acc (transient [])]
    (if (and x (pos? n))
      (recur (next-at x 0) (dec n) (conj! acc [(.score x) (.member x)]))
      (persistent! acc))))

(defn range-by-score
  "スコアが範囲内の [score member] を昇順で返す。開始位置まで O(log N)。"
  [^SkipList sl mn min-excl? mx max-excl?]
  (let [mn (double mn)
        mx (double mx)
        below-min? (fn [^double s] (if min-excl? (<= s mn) (< s mn)))
        within-max? (fn [^double s] (if max-excl? (< s mx) (<= s mx)))
        start (loop [i (dec (level sl))
                     ^Node x (.header sl)]
                (if (neg? i)
                  (next-at x 0)
                  (let [^Node nxt (next-at x i)]
                    (if (and nxt (below-min? (.score nxt)))
                      (recur i nxt)
                      (recur (dec i) x)))))]
    (loop [^Node x start
           acc (transient [])]
      (if (and x (within-max? (.score x)))
        (recur (next-at x 0) (conj! acc [(.score x) (.member x)]))
        (persistent! acc)))))

(defn entries
  "全要素を [score member] のベクタで昇順に返す。"
  [^SkipList sl]
  (loop [^Node x (next-at (.header sl) 0)
         acc (transient [])]
    (if x
      (recur (next-at x 0) (conj! acc [(.score x) (.member x)]))
      (persistent! acc))))

(defn level-counts
  [^SkipList sl]
  (vec (for [i (range (level sl))]
         (loop [x (next-at (.header sl) i) n 0]
           (if x
             (recur (next-at x i) (inc n))
             n)))))

(defn check-spans
  "全段の全ポインタについて、span が最下段で数えた距離と一致するか検査する。"
  [^SkipList sl]
  (let [ranks (IdentityHashMap.)
        len   (length sl)]
    (.put ranks (.header sl) 0)
    (loop [^Node x (next-at (.header sl) 0)
           r 1]
      (when x
        (.put ranks x r)
        (recur (next-at x 0) (inc r))))
    (every? true?
            (for [i (range (level sl))]
              (loop [^Node x (.header sl)]
                (let [nxt      (next-at x i)
                      rx       (long (.get ranks x))
                      expected (if nxt (- (long (.get ranks nxt)) rx) (- len rx))]
                  (cond
                    (not= expected (span-at x i)) false
                    (nil? nxt) true
                    :else (recur nxt))))))))
