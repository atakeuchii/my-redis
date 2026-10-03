(ns my-redis.pubsub)

(defprotocol IClient
  "購読者への配信先。ソケットを直接持たず、送る手段だけを抽象化する。
   テストでは関数に差し替えられる。"
  (send-push! [this payload] "RESP の値をこのクライアントに送る")
  (client-id [this] "レジストリ上の識別子"))

(defn create
  "空のレジストリを作る。
   :channels   チャンネル -> 購読者の集合（配信に使う）
   :subscribed 購読者 -> チャンネルの集合（切断時の掃除に使う）"
  []
  (atom {:channels {} :subscribed {}}))

(defn subscribe! [registry client channel]
  (let [id (client-id client)
        new-state (swap! registry
                         (fn [s]
                           (-> s
                               (assoc-in [:channels channel id] client)
                               (update-in [:subscribed id] (fnil conj #{}) channel))))]
    (count (get-in new-state [:subscribed id]))))

(defn unsubscribe-by-id! [registry id channel]
  (let [new-state (swap! registry
                         (fn [s]
                           (let [subs  (dissoc (get-in s [:channels channel] {}) id)
                                 chans (disj (get-in s [:subscribed id] #{}) channel)]
                             (-> s
                                 (update :channels #(if (seq subs)
                                                      (assoc % channel subs)
                                                      (dissoc % channel)))
                                 (update :subscribed #(if (seq chans)
                                                        (assoc % id chans)
                                                        (dissoc % id)))))))]
    (count (get-in new-state [:subscribed id]))))

(defn unsubscribe! [registry client channel]
  (unsubscribe-by-id! registry (client-id client) channel))

(defn channels-of-id [registry id]
  (get-in @registry [:subscribed id] #{}))

(defn channels-of [registry client]
  (channels-of-id registry (client-id client)))

(defn subscribed? [registry client]
  (boolean (seq (channels-of registry client))))

(defn unsubscribe-all-by-id! [registry id]
  (let [chans (vec (channels-of-id registry id))]
    (doseq [ch chans]
      (unsubscribe-by-id! registry id ch))
    chans))

(defn unsubscribe-all! [registry client]
  (unsubscribe-all-by-id! registry (client-id client)))

(defn subscribers-of [registry channel]
  (vals (get-in @registry [:channels channel] #{})))

(defn channel-count ^long [registry]
  (count (:channels @registry)))
