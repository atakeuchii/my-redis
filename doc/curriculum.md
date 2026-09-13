# Redis 自作カリキュラム（Clojure / 実装13日）

> **ゴール**: 本物の `redis-cli` がそのまま繋がるインメモリデータストアを Clojure で実装し、「Redis がなぜ速いのか」「キャッシュとして何を保証し、何を保証しないのか」を手で理解する。
> **対象**: DBエンジニアとして検索・収集を担当。LSM-tree ストレージエンジン・全文検索エンジン・正規表現エンジン・Git を自作済み。Redis は「速いキャッシュ」として使う側だった人。

---

## なぜこれをやるのか（最初に読む）

Redis を「使う側」から見ると、`SET`/`GET` が速い箱でしかない。中で起きているのは次の5つ：

1. **テキストに近いシンプルなプロトコル（RESP）**でコマンドを受け取る
2. **1つのハッシュテーブル（キースペース）**に、型付きの値を持つ
3. 型ごとに**用途特化のデータ構造**（skiplist, listpack, intset…）を使い分ける
4. **有効期限と eviction** で「消えてよいデータ」を管理する
5. インメモリなのに落ちても困らないよう、**AOF / RDB で永続化**する

自分で書くと、次が腹落ちする：

- なぜ Redis は**シングルスレッド**なのに速いのか（＝ロックが要らない代わりに、1コマンドが長いと全員が待つ）
- なぜ `ZRANGEBYSCORE` が速いのか（skiplist が**ソート順とランクの両方**を O(log N) で持つから）
- なぜ `TTL` が切れたキーがメモリを即座に返さないのか（受動的期限切れ＋サンプリング）
- なぜ「キャッシュとして使うなら `maxmemory-policy` を設定しろ」と言われるのか
- AOF rewrite が、my-storage で書いた**コンパクションとまったく同じ構図**であること

### 過去プロジェクトからの接続

| 今回のテーマ | 既に作ったもの |
|---|---|
| AOF 追記＋リプレイ | my-storage の WAL 追記＋リプレイ |
| AOF rewrite | my-storage の compaction（不要な履歴を捨てて最小状態に畳む） |
| RDB スナップショット | my-storage の SSTable 書き出し（フッター＋マジックナンバー） |
| RESP パーサ | my-git の DIRC / オブジェクトのバイナリパース |
| 参照オラクルテスト | my-git vs 本物 git、regex vs `java.util.regex` |

**新規性が高いのは Day 1–9**（プロトコル・サーバ・データ構造・有効期限）。Day 10–12 は既知の型で入れるので、そこは速度を出してよい。

### Clojure でやる旨味

- **キースペース = `(atom {})`**。Redis 本体がシングルスレッドで回避している並行性の問題を、イミュータブルな値＋`swap!` で素直に扱える。
- **コマンド = データ**。`{"GET" {:arity 2 :write? false :fn ...}}` というテーブルを作るだけでディスパッチが終わる。Redis の `commands.def` と同じ発想。
- **RESP パーサ**は純粋関数に切り出せる。`test.check` で「エンコード→デコードで元に戻る」ラウンドトリップ検査ができる。
- **skiplist だけは可変**。ここは意図的に `deftype` + 可変配列で書き、「なぜイミュータブルでない方が良い箇所があるか」を体験する。
- 参照オラクルが完璧（Docker の本物 redis）。**同じコマンド列を両方に流して差分を見る**テストが書ける。

---

## 全体アーキテクチャ

```
        redis-cli / TCP client
                │  RESP バイト列
                ▼
   ┌──────────────────────────┐
   │ server（接続ごとのループ） │  Socket ごとに1スレッド
   └──────────────────────────┘
                │ ["SET" "k" "v"]
                ▼
   ┌──────────────────────────┐
   │ command（ディスパッチ表）  │  arity / write? / handler
   └──────────────────────────┘
         │                    │ 書き込みコマンドなら
         ▼                    ▼
   ┌──────────┐        ┌──────────────┐
   │ db       │        │ persistence  │
   │ keyspace │        │  AOF / RDB   │
   │ + expires│        └──────────────┘
   └──────────┘
         │
         ▼
   ┌──────────────────────────┐
   │ types: list / hash / set  │
   │        / zset(skiplist)   │
   └──────────────────────────┘
```

### 名前空間とレイヤ順（一方向依存を厳守）

```
my-redis.resp          ; バイト列 ↔ Clojure データ。何にも依存しない純粋層
my-redis.types.skiplist; 単体で完結する可変データ構造
my-redis.types.zset    ; skiplist + score dict
my-redis.db            ; キースペース・型付き値・TTL。types に依存
my-redis.commands.*    ; string / list / hash / set / zset / server。db に依存
my-redis.persistence.aof
my-redis.persistence.rdb ; db と resp に依存
my-redis.pubsub        ; 接続レジストリ
my-redis.server        ; resp + commands + persistence + pubsub を束ねる最上位
my-redis.core          ; エントリポイント
```

**上の行は下の行を知らない。** my-git の `object → tree → commit → ref → index → repo` と同じ規律。

---

## フェーズ構成

| フェーズ | 日 | テーマ |
|---|---|---|
| 0. 準備 | Day 0 | 概念・RESP 概観・スコープ確定・redis-cli で実物を触る |
| 1. プロトコルとサーバ | Day 1–4 | RESP パース → TCP サーバ → ディスパッチ → 文字列コマンド |
| 2. データ構造 | Day 5–8 | List/Hash/Set → Sorted Set / skiplist |
| 3. 有効期限 | Day 9 | TTL（受動的＋能動的）と eviction |
| 4. 永続化 | Day 10–12 | AOF 追記＋リプレイ → AOF rewrite → RDB |
| 5. 仕上げ | Day 13 | Pub/Sub + 参照オラクルテスト + README |

各日は **目標 / やること / 概念 / Clojureヒント / チェックポイント**。チェックポイントがその日の完了条件（DoD）。
**各日の実装前に、その日のテーマの小テストを行う。**

---

## Day 0 — 準備・概念・スコープ確定（今日）

**目標**: 「Redis＝速いキャッシュ」という解像度を、「型付きキースペース＋プロトコル＋期限管理＋永続化の集合体」まで上げる。

**やること**
- `lein new my-redis` 相当でプロジェクト作成（`project.clj`、依存は `org.clojure/clojure` + dev に `test.check`）。
- 本物を触る：
  ```bash
  docker run --rm -d -p 6379:6379 --name redis-oracle redis
  docker exec -it redis-oracle redis-cli
  ```
  最低限これを打って挙動を見る：
  ```
  SET k v / GET k / TYPE k / DEL k
  RPUSH l a b c / LRANGE l 0 -1 / TYPE l
  GET l            ← WRONGTYPE エラーを自分の目で見る
  ZADD z 1 a 2 b / ZRANGE z 0 -1 WITHSCORES / ZRANK z b
  EXPIRE k 5 / TTL k
  INFO keyspace / OBJECT ENCODING l
  COMMAND DOCS GET   ← Redis 自身がコマンド表を持っていることを確認
  ```
- **生の RESP を見る**（ここが Day 1 への橋）：
  ```bash
  # サーバ側に届くバイト列を見る
  docker exec -it redis-oracle redis-cli --no-raw
  printf 'PING\r\n' | nc localhost 6379          # inline command
  printf '*2\r\n$3\r\nGET\r\n$1\r\nk\r\n' | nc localhost 6379
  ```
  「クライアントが送るのは常に Bulk String の配列」「サーバの返しは型が混ざる」ことを確認する。

**概念（読む）**
- Redis Docs の *Protocol specification (RESP)* — **Day 1 の仕様書そのもの**。最優先。
- Redis Docs の *Data types* 概要（String/List/Hash/Set/Sorted Set）。
- Redis Docs の *Key eviction* と *Persistence*（AOF/RDB の違い）をざっと。

**スコープ確定（今回作らないものを明示する）**
- 作る：RESP2、TCP（スレッド/接続）、String/List/Hash/Set/ZSet、TTL/eviction、AOF/RDB、Pub/Sub
- **作らない**：RESP3、レプリケーション、Cluster、Lua スクリプト、MULTI/EXEC、io multiplexing（epoll 相当）、listpack 等のメモリ最適化エンコーディング、Streams、ACL
- 作らないものは「知らない」ではなく「概念だけ理解してストレッチに置く」

**チェックポイント**
- [ ] `lein repl` が起動する
- [ ] Docker の redis に redis-cli で繋がり、上のコマンドを打った
- [ ] `nc` で生の RESP を投げてレスポンスのバイト列を見た
- [ ] **カリキュラム全体の理解度テストに合格**（この後やる）

---

## フェーズ1：プロトコルとサーバ（Day 1–4）

### Day 1 — RESP パーサ / シリアライザ

**目標**: バイト列と Clojure データの相互変換を、サーバとは独立した純粋層として完成させる。

**やること**
- RESP2 の5種類を扱う：
  | 記号 | 型 | 例 |
  |---|---|---|
  | `+` | Simple String | `+OK\r\n` |
  | `-` | Error | `-ERR unknown command\r\n` |
  | `:` | Integer | `:1000\r\n` |
  | `$` | Bulk String | `$3\r\nfoo\r\n` / Null は `$-1\r\n` |
  | `*` | Array | `*2\r\n$3\r\nGET\r\n$1\r\nk\r\n` / Null は `*-1\r\n` |
- `read-reply`（InputStream → Clojure データ）と `encode`（Clojure データ → bytes）の両方向。
- Null Bulk String と空 Bulk String（`$0\r\n\r\n`）を**区別**する設計を決める（`nil` と `""`）。
- inline command（`PING\r\n` のような素のテキスト）も受け付けるかを決める → **受け付ける**（Day 2 で telnet デバッグが効くため）。

**概念**
- なぜ長さプレフィックス方式なのか。区切り文字だけの方式（`\r\n` 探し）だと何が壊れるか（バイナリセーフでなくなる）。
- 「クライアント→サーバは常に Bulk String の配列」という非対称性。サーバ側パーサが単純になる理由。
- Bulk String は**バイナリセーフ**。したがって内部表現は `String` ではなく `byte[]` が正しい。ただし今回は**キーと値を `String`（UTF-8）に決め打ち**し、境界だけ意識する（my-storage の Day 1 と同じ判断）。

**Clojureヒント**
- `java.io.PushbackInputStream` か `DataInputStream` で1バイトずつ型記号を読む。行読みは自前の `read-line-bytes`（`\r\n` まで読む）を書く。`BufferedReader#readLine` は `\n` 単独でも切れるので RESP には使わない。
- 出力は `ByteArrayOutputStream` + `DataOutputStream`、最後に `.getBytes "UTF-8"`。
- エラーは `{:type :error :msg "ERR ..."}` のようなマップか、専用の `defrecord`。`nil` と区別できることが重要。

**チェックポイント**
- [ ] `*2\r\n$3\r\nGET\r\n$1\r\nk\r\n` → `["GET" "k"]` にパースできる
- [ ] `test.check` で「任意のデータ → encode → read-reply」が元に戻る（ラウンドトリップ）
- [ ] Null Bulk String と空文字列が区別できる
- [ ] 不完全な入力（途中で切れたバイト列）で例外が出ても、サーバを巻き込まない形になっている

---

### Day 2 — TCP サーバと接続ハンドリング

**目標**: 本物の `redis-cli` が自作サーバに繋がり、`PING` が返る。**最初の山。**

**やること**
- `ServerSocket` を 6380 番（本物と衝突しないように）で listen。
- 接続ごとに1スレッド（`future` or `Thread`）。接続ループ：**RESP を読む → 処理 → RESP を書く → flush → 繰り返す**。
- この時点のコマンドは `PING` / `ECHO` / `COMMAND`（redis-cli が接続時に投げることがある）だけでよい。
- クライアント切断（`read` が -1 / `EOFException`）でスレッドを綺麗に終える。1接続の例外が全体を落とさないこと。
- `stop!` でサーバを止められるようにする（REPL 駆動のため必須）。

**概念**
- 本物 Redis は**シングルスレッド＋イベントループ（epoll/kqueue）**。今回は**接続ごとスレッド**にする。この違いが持つ意味：
  - Redis：ロック不要・コマンドの原子性がタダ。ただし遅いコマンド（`KEYS *`）が全員をブロックする。
  - 自作：接続数に比例してスレッドが増える。キースペースへの並行アクセスを `atom` で守る必要がある。
- `flush` を忘れると「クライアントが固まる」。バッファリングと応答性のトレードオフ。

**Clojureヒント**
- `(.setTcpNoDelay socket true)` を入れておくと小さい応答の遅延が減る。
- 接続スレッドは `(future ...)` で十分。ただし例外が握り潰されるので `try/catch` でログを出す。
- REPL でサーバを起動しっぱなしにするため、`(defonce server (atom nil))` パターン。

**チェックポイント**
- [ ] `redis-cli -p 6380 PING` → `PONG` ← **最初の大きな達成感ポイント**
- [ ] `redis-cli -p 6380` の対話モードに入って複数コマンドを続けて打てる
- [ ] クライアントを切断してもサーバが生きている
- [ ] REPL からサーバを起動・停止できる

---

### Day 3 — コマンドディスパッチとキースペース

**目標**: 「コマンド表」を作り、キースペースを初めて持つ。`SET`/`GET`/`DEL`/`EXISTS`/`KEYS` が動く。

**やること**
- コマンドテーブルを**データとして**定義：
  ```clojure
  {"GET" {:arity 2 :write? false :handler get-handler}
   "SET" {:arity -3 :write? true  :handler set-handler}}
  ```
  arity は Redis 同様「正数＝厳密、負数＝最低個数」の規約にする（後で可変長引数が来るため）。
- ディスパッチ層の責務を確定：
  1. コマンド名を**大文字化**（Redis はコマンド名が case-insensitive、キーは case-sensitive）
  2. 未知のコマンド → `-ERR unknown command 'XXX'`
  3. arity 不一致 → `-ERR wrong number of arguments for 'xxx' command`
  4. ハンドラを呼ぶ
- キースペース `my-redis.db`：`(atom {})`。値は**型情報を持たせる**形にする：
  ```clojure
  {"k" {:type :string :value "v"}
   "l" {:type :list   :value [...]}}
  ```
- `SET k v` / `GET k` / `DEL k [k ...]`（削除件数を返す）/ `EXISTS` / `KEYS pattern` / `TYPE k` / `FLUSHDB`。

**概念**
- **なぜ値に型タグが要るのか**。Day 4 の `WRONGTYPE` エラーの土台。Redis の `redisObject` が `type` と `encoding` を持っているのと同じ。
- コマンドを「表」にすることの意味：ディスパッチ、arity チェック、`write?` フラグ（Day 10 の AOF で「どのコマンドを記録するか」に直結）、将来の `COMMAND` 実装がすべて1つのデータから導ける。
- **`KEYS *` が本番で禁じられる理由**をここで理解する（O(N) 走査＋シングルスレッド）。

**Clojureヒント**
- ハンドラのシグネチャを最初に決めて固定する。例：`(fn [ctx args] -> reply)`。`ctx` に `{:db ... :conn ...}` を入れておくと、Day 10 以降で AOF や Pub/Sub を足すときに**シグネチャを変えずに済む**。ここは設計判断として最初に握る。
- `swap!` は関数が複数回呼ばれうる。**副作用をクロージャに入れない**。返り値が必要なら `swap-vals!`。

**チェックポイント**
- [ ] `SET`/`GET`/`DEL`/`EXISTS`/`TYPE` が redis-cli から動く
- [ ] 未知コマンド・引数不足で本物と**同じ文面のエラー**が返る
- [ ] コマンド表に1行足すだけで新コマンドが追加できる構造になっている

---

### Day 4 — 文字列コマンドとエラーの型

**目標**: String 型を実用レベルにし、Redis のエラー体系を自分の実装に持ち込む。

**やること**
- `INCR` / `DECR` / `INCRBY` / `DECRBY`：値を整数としてパースし、失敗したら `-ERR value is not an integer or out of range`。
- `APPEND` / `STRLEN` / `GETSET` / `SETNX` / `MSET` / `MGET`。
- `SET` のオプション：`EX seconds` / `PX ms` / `NX` / `XX`（TTL の器だけ用意し、実際の期限切れ判定は Day 9）。
- **`WRONGTYPE` の実装**：`GET` をリストに対して呼んだら
  `-WRONGTYPE Operation against a key holding the wrong kind of value`。
  型チェックを**全ハンドラに散らさず**、`db` 層の `fetch-typed` のような1箇所に集約する。

**概念**
- Redis の文字列は**バイナリセーフで最大512MB**、かつ「整数として扱える文字列」は内部で int エンコードされる。`INCR` が O(1) なのはそのため。
- `INCR` の原子性。今回は `swap!` で自然に得られるが、本物ではシングルスレッドが保証している。「read-modify-write をクライアント側でやると壊れる」という、キャッシュ運用の典型的な罠がここ。
- エラーは**プロトコル上の第一級の型**（`-`）であり、例外ではない。ハンドラは例外を投げず、エラー値を返す設計にすると後が楽。

**Clojureヒント**
- 整数パースは `Long/parseLong` を `try` で包み、失敗を error 値に変換する小さなヘルパにまとめる。
- `MSET` は「全部成功」が原子的であるべき → `swap!` 1回で複数キーを更新する。

**チェックポイント**
- [ ] `INCR` が非整数値・存在しないキー・オーバーフローで本物と同じ挙動
- [ ] リストに `GET` して `WRONGTYPE` が返る（型チェックが1箇所に集約されている）
- [ ] **フェーズ1完了**：redis-cli から文字列系の実用的な操作が一通りできる

---

## フェーズ2：データ構造（Day 5–8）

### Day 5 — List

**目標**: 両端キューとしての List を実装し、Redis のインデックス規約を正確に再現する。

**やること**
- `LPUSH` / `RPUSH`（可変長、返り値は push 後の長さ）/ `LPOP` / `RPOP` / `LLEN` / `LINDEX` / `LRANGE` / `LSET` / `LREM` / `LTRIM`。
- **負のインデックス**（`-1` は末尾）、**範囲のクランプ**（`LRANGE l 0 -1`、範囲外は空配列でエラーではない）を正確に。
- 空になったリストの**キー自体を削除**する（Redis の規約。`EXISTS` が 0 になる）。

**概念**
- Redis の List は **quicklist**（listpack のリンクリスト）。両端 O(1)、中間アクセス O(N)。
- 今回 Clojure で何を使うか、**自分で決めて理由を言えるようにする**：
  - `clojure.lang.PersistentVector`：右端 O(1)、左端 O(N) ← `LPUSH` が弱い
  - 2本のベクタ（front/back）による amortized deque
  - `clojure.lang.PersistentQueue`、あるいは可変 `java.util.ArrayDeque`
  → 「両端 O(1) が Redis List の売り」を壊さない選択をする。
- 空キー削除の規約がなぜ必要か（`TYPE` の一貫性、メモリ、`EXISTS` の意味）。

**チェックポイント**
- [ ] `LRANGE l 0 -1` / `LRANGE l -3 -1` / 範囲外が本物と一致
- [ ] `LPOP` で空になったキーが `EXISTS` 0 になる
- [ ] `LPUSH`/`RPUSH` が O(1) である（選んだ構造の根拠を説明できる）

---

### Day 6 — Hash と Set

**目標**: 残る2つの基本型を入れ、キースペースが「マップのマップ」になる構造を体感する。

**やること**
- Hash：`HSET`（可変長 field-value ペア）/ `HGET` / `HDEL` / `HGETALL` / `HKEYS` / `HVALS` / `HLEN` / `HEXISTS` / `HINCRBY`。
- Set：`SADD` / `SREM` / `SMEMBERS` / `SISMEMBER` / `SCARD` / `SPOP` / `SINTER` / `SUNION` / `SDIFF`。
- `HGETALL` の返りが**フラットな配列**（field, value, field, value …）である点に注意。
- 集合演算は**小さい集合から回す**最適化を意識して書く（`SINTER` の計算量が `O(N*M)` になるかどうかの分かれ目）。

**概念**
- **エンコーディング**：Redis は小さい Hash/Set を listpack や intset で持ち、閾値（`hash-max-listpack-entries` など）を超えるとハッシュテーブルへ昇格する。実物で `OBJECT ENCODING` を叩いて確認する。
  - なぜメモリ最適化のために O(N) 線形探索を許すのか（N が小さければ、ポインタとハッシュのオーバーヘッドの方が高い／キャッシュ局所性）
- 今回は `{}` と `#{}` をそのまま使う。**「作らない」と決めた部分だが、なぜ本物にはあるかを説明できること**が Day 6 の実質的なゴール。
- `SPOP` のランダム性と、それが決定的なテストをどう難しくするか。

**チェックポイント**
- [ ] Hash/Set の主要コマンドが redis-cli から動く
- [ ] `HGETALL` のフラット配列形式が本物と一致
- [ ] `OBJECT ENCODING` を本物で叩き、listpack → hashtable の昇格を観測した
- [ ] エンコーディング切り替えの理由を自分の言葉で説明できる

---

### Day 7 — Sorted Set（API とモデル）

**目標**: ZSet の意味論を先に固め、まずは素朴な実装で全コマンドを通す。

**やること**
- `ZADD` / `ZSCORE` / `ZCARD` / `ZINCRBY` / `ZREM` / `ZRANGE`（`WITHSCORES`）/ `ZREVRANGE` / `ZRANK` / `ZREVRANK` / `ZRANGEBYSCORE` / `ZCOUNT`。
- **順序の規約を正確に**：スコア昇順、**同スコアなら member の辞書順**。
- `ZRANGEBYSCORE` の境界指定：`(5`（排他）、`-inf` / `+inf`。
- この日はまず「member→score の map」＋「[score member] の sorted-set」の2本立てで実装する（skiplist はまだ使わない）。

**概念**
- **ZSet が2つの索引を持つ理由**：`ZSCORE`（member 指定）には辞書が、`ZRANGE`（順序・範囲）にはソート構造が要る。片方だけでは片方が O(N) になる。Redis 実物も `dict` + `skiplist` の2本立て。
  → my-storage の「memtable（ソート済み）＋ SSTable のスパースインデックス」と同じ、**用途に応じた索引の使い分け**。
- `ZADD` は member が既存なら**スコア更新＝ソート構造からの削除＋再挿入**。ここが skiplist 実装で最も間違えやすい箇所になる（Day 8 の伏線）。
- スコアは double。整数と浮動小数の境界、`nan` の扱い。

**Clojureヒント**
- `(sorted-set-by (fn [[s1 m1] [s2 m2]] (compare [s1 m1] [s2 m2])))` で「スコア→member」の複合キー順が作れる。
- `ZRANK` はこの素朴実装だと O(N)（`take-while` で数える）。**これを Day 8 で O(log N) にする**のが翌日の動機になる。測っておくとよい。

**チェックポイント**
- [ ] ZSet の主要コマンドが本物と一致する（同スコア時の辞書順も）
- [ ] `ZRANGEBYSCORE` の排他境界・`-inf`/`+inf` が動く
- [ ] `ZRANK` が現状 O(N) であることを自覚している

---

### Day 8 — skiplist 自作

**目標**: Day 7 のソート構造を自作 skiplist に差し替え、`ZRANK` を O(log N) にする。**今回の技術的な山。**

**やること**
- skiplist を `deftype` + 可変ノードで実装：
  - ノードは `[score member forwards[] spans[] backward]`
  - `insert!` / `delete!` / `rank-of` / `nth-by-rank` / `range-by-score`
  - レベル決定は確率的（Redis は p=0.25、最大32レベル）
- **span（各 forward ポインタが何ノード飛ぶか）**を保持し、探索経路の span を足し合わせて `ZRANK` を O(log N) にする。ここが skiplist の一番面白い部分。
- ZSet を「member→score の `{}`」＋「自作 skiplist」に差し替える。**外から見た挙動は Day 7 と1ビットも変えない**（Day 7 のテストがそのまま回帰テストになる）。

**概念**
- skiplist が**確率的にバランスする**理由。なぜ平衡二分木（AVL/赤黒木）ではなく skiplist なのか：
  - 実装が単純（回転が無い）
  - 範囲スキャンが最下層リンクリストの走査でそのまま書ける
  - ロックしやすい／並行性に向く
- **span がランク索引になる**という発想。B-tree に件数を持たせる「order statistic tree」と同じ考え方。
- ここだけ**可変**にする理由：ノードごとの forward 配列の破壊的更新が本質で、永続データ構造にすると空間・時間の両方で割に合わない。**イミュータブルが常に正解ではない**ことを手で確認する。

**Clojureヒント**
- `deftype` + `^objects` 配列。`aset`/`aget` を使う。型ヒントが無いとリフレクションで劇的に遅くなるので `*warn-on-reflection*` を有効にする。
- レベル生成：`(loop [lvl 1] (if (and (< lvl 32) (< (rand) 0.25)) (recur (inc lvl)) lvl))`
- テスト戦略：**`test.check` でランダムな insert/delete 列を流し、素の `sorted-set` と一致するか**を検査。my-storage の Day 12 とまったく同じ「実装 vs 参照モデル」の型。

**チェックポイント**
- [ ] skiplist 単体のプロパティテストが通る（sorted-set と等価）
- [ ] ZSet を差し替えても Day 7 のテストが全部グリーン ← **リファクタが安全である証明**
- [ ] `ZRANK` が大量データで劣化しないことを実測した（Day 7 の O(N) と比較）
- [ ] **フェーズ2完了**：5つの型が揃った

---

## フェーズ3：有効期限（Day 9）

### Day 9 — TTL と eviction

**目標**: 「Redis がキャッシュである」という性質の中身を実装する。

**やること**
- 期限テーブルを**キースペースとは別に**持つ：`{:data {...} :expires {"k" 1726200000000}}`。
- コマンド：`EXPIRE` / `PEXPIRE` / `EXPIREAT` / `TTL` / `PTTL` / `PERSIST` / `SET ... EX`。
  - `TTL`：存在しないキーは `-2`、期限なしは `-1`。この区別を正確に。
- **受動的期限切れ（lazy）**：キー参照時に期限を確認し、切れていたら削除して「無い」として扱う。**全コマンドの入口の1箇所**で行う。
- **能動的期限切れ（active）**：バックグラウンドスレッドで定期的に、`expires` からランダムに N 件サンプリング → 切れていたら削除 → 切れていた割合が閾値（Redis は25%）を超えたら即座にもう一周。
- **eviction**：`maxmemory` 相当（今回は**キー数の上限**で代用）と `maxmemory-policy` を最低2つ実装：
  - `noeviction`（書き込みを `-OOM command not allowed ...` で拒否）
  - `allkeys-random` または `allkeys-lru`（近似 LRU：ランダムに数件サンプリングし、最終アクセス時刻が最古のものを捨てる）

**概念**
- **なぜ受動的だけでは足りないのか**：二度と参照されないキーは永久にメモリに残る。
- **なぜ能動的が「全走査」ではなくサンプリングなのか**：シングルスレッドで全走査すると、その間サービスが止まる。**確率的に、割り切って、少しずつ**。
  - これは「正確さより応答性」を選ぶ設計判断。my-storage の compaction を止めずに回した話と同じ系譜。
- **なぜ Redis の LRU は近似なのか**：正確な LRU は全キーの連結リスト＋各アクセスでのリンク付け替えが要る。Redis は各オブジェクトに時刻フィールドを持ち、**数件サンプリングして一番古いものを捨てる**（`maxmemory-samples`、既定5）。精度とメモリのトレードオフ。
- `volatile-*` と `allkeys-*` の違い、`noeviction` が既定であることの意味（**キャッシュとして使うなら明示設定が必要**）。
- ここまでで「Redis をキャッシュとして使うときに何を設定すべきか」を**実務の言葉で説明できる**ようになるのが本当のゴール。

**Clojureヒント**
- 現在時刻は `(System/currentTimeMillis)` を**関数経由**で取る（`(defn now [] ...)`）。テストで差し替えられるようにするため。`with-redefs` で時間を進めるテストが書ける。
- 能動的期限切れは `future` + ループ + `Thread/sleep 100`。停止フラグを `atom` で持ち、`close!` で確実に止める。
- LRU の時刻は値のマップに `:atime` を持たせ、読み取り時に更新する。**読み取りが書き込みになる**（＝`swap!` が必要）ことに注意。

**チェックポイント**
- [ ] `EXPIRE k 1` → 1秒後に `GET k` が nil、`TTL` が `-2`
- [ ] 参照しないまま期限切れしたキーが、能動的期限切れで実際に消える（ログで確認）← **効果が目で見える**
- [ ] キー数上限に達したとき、policy に応じて拒否 or 追い出しが起きる
- [ ] 「なぜ近似 LRU なのか」を自分の言葉で説明できる

---

## フェーズ4：永続化（Day 10–12）

### Day 10 — AOF 追記とリプレイ

**目標**: 再起動してもデータが残る。**my-storage の WAL とまったく同じ構図**なので、既知の型で速く行ける。

**やること**
- **書き込みコマンドのみ**を AOF に追記する。Day 3 で入れた `:write? true` フラグがここで効く。
- 形式は**受け取った RESP コマンドをそのまま**（`*3\r\n$3\r\nSET\r\n...`）。独自形式にしない。
  → 理由：`redis-cli --pipe` で流し込める、デバッグで `cat` できる、エンコーダを再利用できる。
- **順序を厳守**：コマンド実行 → AOF 追記。
  （注：Redis は**実行後に追記**する。WAL（先に書く）とは逆。**なぜ逆でよいのかを説明できること**が今日の肝。）
- `appendfsync` ポリシーを3つ用意：`always` / `everysec`（バックグラウンドで1秒ごと `force`）/ `no`。
- 起動時：AOF を先頭から読み、**コマンドとして再実行**してキースペースを復元。
- **末尾の壊れたコマンド**（書き込み途中で落ちた）を安全に切り捨てる。my-storage の WAL truncate と同じ。

**概念**
- **なぜ Redis は実行後にログを書くのか**：Redis が保証するのは「fsync 済みのものは残る」だけで、レスポンスを返したコマンドの耐久性を（`always` 以外では）保証していない。DB の WAL（コミット応答前に fsync）との**保証レベルの違い**。「キャッシュだから」で済ませず、**何を失いうるか**を言語化する。
- `everysec` で失うのは最大何秒分か。
- AOF が「状態のスナップショット」ではなく「操作の履歴」であること → 必然的に**無限に伸びる** → Day 11 へ。

**Clojureヒント**
- `my-storage` の WAL 実装（`append!` / `replay` / `truncate!`）をそのまま参照してよい。構造は同じ。
- リプレイは「AOF を RESP のシーケンスとして読む → 各コマンドをディスパッチャに流す」。**ネットワークを介さずディスパッチャを再利用できる設計**になっているかがここで試される（Day 3 でハンドラのシグネチャを固定した理由）。
- リプレイ中は AOF への再追記を止める（フラグ or ctx で制御）。

**チェックポイント**
- [ ] 書く → サーバ停止 → 再起動 → 読める
- [ ] AOF ファイルを `cat` するとコマンドが読める
- [ ] 末尾にゴミを足しても起動が落ちず、健全な部分まで復元する
- [ ] 「Redis の AOF が WAL と保証レベルで違う点」を説明できる

---

### Day 11 — AOF rewrite（＝コンパクション）

**目標**: 履歴を現在の状態から作り直し、AOF を縮める。**my-storage の compaction の再演。**

**やること**
- `BGREWRITEAOF`：現在のキースペースを走査し、**各キーを再現する最小のコマンド列**を新ファイルに書く。
  - String → `SET k v`（TTL があれば `PEXPIREAT`）
  - List → `RPUSH k e1 e2 ...`（要素が多ければ分割）
  - Hash → `HSET k f1 v1 f2 v2 ...`
  - Set → `SADD k m1 m2 ...`
  - ZSet → `ZADD k s1 m1 s2 m2 ...`
- **リライト中も書き込みを受け付ける**：リライト開始時にキースペースのスナップショットを取り（Clojure のイミュータブルマップなのでこれが**タダ**）、その間の新規書き込みをバッファに溜め、完了時に新ファイルへ追記してからアトミックに差し替える（`File#renameTo` / `Files/move` with `ATOMIC_MOVE`）。
- 自動トリガ：AOF サイズが前回リライト時の N 倍（Redis 既定100%）かつ最小サイズ超過。

**概念**
- **これは my-storage の compaction と同じ**：`SET k 1` を100万回した履歴が `SET k 1000000` の1行に畳まれる。tombstone の物理削除に相当するのが「削除済みキーがそもそも出力されない」こと。
- **本物 Redis は fork() してコピーオンライトで子プロセスに書かせる**。Clojure のイミュータブルスナップショットは**それを言語レベルで、プロセスを増やさずに実現している**。ここが今回いちばん気持ちのいい対比。
  - fork+COW の代償（メモリが最悪2倍、ページコピーのコスト）も理解しておく。
- 「新ファイルを作ってからアトミックに差し替える」が壊れない理由。差し替えの瞬間に落ちたらどうなるか。

**チェックポイント**
- [ ] 同じキーを大量更新 → rewrite → **ファイルサイズが劇的に減る** ← **達成感ポイント**
- [ ] rewrite 中に書き込みを続けても、再起動後のデータが正しい
- [ ] 全5型 + TTL が rewrite 後に復元される
- [ ] fork+COW とイミュータブルスナップショットの対応関係を説明できる

---

### Day 12 — RDB スナップショット

**目標**: 状態そのものをバイナリで保存する、もう一方の永続化を作る。

**やること**
- 自前のバイナリ形式を設計（本物の RDB 形式への互換は**目標にしない**）。my-storage の SSTable で書いた作法を流用：
  ```
  [マジック "MYRDB001"][バージョン]
  [エントリ数]
  各キー: [型タグ(1byte)][keyLen][key][有効期限(8byte, 0=なし)][型ごとのペイロード]
  [チェックサム][フッター]
  ```
- `SAVE`（同期）/ `BGSAVE`（`future` でスナップショットを書き出す）。
- 起動時のロード順：**RDB を読む → その後 AOF があれば適用**（本物は AOF 有効なら AOF 優先）。どちらの規約にするか決めて README に書く。
- 書き込みは**一時ファイル → アトミック rename**。

**概念**
- **AOF（操作履歴）と RDB（状態スナップショット）のトレードオフ**：
  | | AOF | RDB |
  |---|---|---|
  | 復旧のデータ損失 | 小さい（everysec なら1秒） | 大きい（前回スナップショット以降） |
  | 起動速度 | 遅い（全コマンド再実行） | 速い（そのまま読む） |
  | ファイルサイズ | 大きい | 小さい（圧縮も効く） |
  | 用途 | 耐久性重視 | バックアップ／レプリカ初期同期 |
- my-storage で言えば **AOF = WAL、RDB = SSTable**。同じ二分法が別の名前で再登場していることを確認する。
- なぜ本物は両方持ち、さらに「混合モード」（RDB ヘッダ + 以降 AOF）があるのか。

**チェックポイント**
- [ ] `BGSAVE` → 停止 → 起動でデータが復元する
- [ ] 全5型 + TTL が RDB 経由で往復する
- [ ] RDB と AOF のサイズ・起動時間を**実測して比較した**（数字を捏造しないこと）
- [ ] **フェーズ4完了**

---

## フェーズ5：仕上げ（Day 13）

### Day 13 — Pub/Sub・テスト・README

**目標**: 「リクエスト/レスポンス」から外れる通信モデルを1つ入れ、人に見せられる状態にする。

**やること**
- **Pub/Sub**：
  - `SUBSCRIBE ch [ch ...]` / `UNSUBSCRIBE` / `PUBLISH ch msg` / `PSUBSCRIBE`（余裕があれば）
  - 購読レジストリ `{channel #{connection}}` を `atom` で持つ。
  - `PUBLISH` は購読者の**ソケットに直接書き込む**。ここで初めて「自分宛でないレスポンスが飛んでくる」形になる。
  - 購読中の接続は**限られたコマンドしか受け付けない**（Redis の規約）。
  - 接続が切れたら全チャンネルから購読解除する（**リークしやすい箇所**）。
- **参照オラクルテスト**：
  - 本物 redis（6379）と自作（6380）に**同じコマンド列を流し、レスポンスを比較**するテストハーネスを書く。
  - `test.check` でランダムなコマンド列を生成し、差分が出たら縮小して表示。my-git で本物 git と比較したのと同じ型。
  - 非決定的なコマンド（`SPOP`、`RANDOMKEY`、`TTL` の秒境界）は生成対象から除外する。
- **README**：アーキ図、レイヤ構成、対応コマンド一覧、AOF/RDB のバイト形式、設計判断とトレードオフ（接続ごとスレッド vs イベントループ、可変 skiplist を選んだ理由、AOF を実行後に書く判断）、実測値、作らなかったものとその理由。

**概念**
- Pub/Sub が**至上主義的にファイア・アンド・フォーゲット**であること：購読者がいなければメッセージは消える、配信保証も永続化もない。**だから Streams（や Kafka）が別に存在する**。次の学習対象（Kafka）への橋。
- 複数の接続に跨って書き込む設計が持つ危うさ（遅い購読者、出力バッファ、`client-output-buffer-limit`）。

**チェックポイント**
- [ ] 2つの redis-cli で `SUBSCRIBE` / `PUBLISH` が動く
- [ ] 購読者が切断してもサーバが壊れず、レジストリにゴミが残らない
- [ ] 本物との差分テストが数百ケース通る（差分が出たバグを最低1つ潰した）
- [ ] README だけ読めば設計が分かる
- [ ] **完成**

---

## 完成の定義（Definition of Done）

- [ ] 本物の `redis-cli` がそのまま接続でき、対話モードで一通り操作できる
- [ ] String / List / Hash / Set / Sorted Set の主要コマンドが本物と同じ挙動（エラー文面・境界・空キー削除を含む）
- [ ] Sorted Set が自作 skiplist で動き、`ZRANK` が O(log N)
- [ ] TTL が受動的・能動的の両方で回収され、eviction policy が2つ以上動く
- [ ] AOF で再起動後もデータが残り、末尾破損から回復する
- [ ] AOF rewrite でファイルが縮み、リライト中の書き込みも失われない
- [ ] RDB スナップショットで保存・復元できる
- [ ] Pub/Sub が動く
- [ ] 本物 redis を参照オラクルにした差分テストがある
- [ ] 実測値付きの README がある

---

## リファレンス

- **Redis Docs — Protocol specification (RESP)**：Day 1 の仕様書。最重要。
- **Redis Docs — Data types / Key eviction / Persistence**：各フェーズの入口。
- **Redis ソース**（読むなら該当箇所だけ）：`t_zset.c`（skiplist）、`expire.c`、`aof.c`、`rdb.c`、`commands.def`。
- **skiplist 原論文**：Pugh, "Skip Lists: A Probabilistic Alternative to Balanced Trees", 1990.
- **DDIA 第3章・第7章**：ログ構造と耐久性の議論は今回もそのまま効く。
- Clojure: `java.net.ServerSocket`, `java.io.PushbackInputStream`, `deftype` + 配列, `org.clojure/test.check`。

---

## ストレッチ目標（13日で終わって余ったら）

- **MULTI / EXEC / DISCARD / WATCH**：トランザクションと楽観ロック。`WATCH` が CAS そのものなので Clojure の `atom` と相性が良い。
- **RESP3**：map / set / double / push 型。`HELLO 3` でのネゴシエーション。
- **io multiplexing**：`java.nio` の `Selector` で単一スレッド化し、本物の設計に寄せる。スレッド版とベンチ比較すると面白い。
- **レプリケーション**：`REPLICAOF`、RDB 全同期 + コマンドストリーム。**Kafka の前哨戦**。
- **Streams（XADD/XREAD）**：Pub/Sub の弱点を埋める構造。radix tree。
- **listpack / intset**：小サイズ時のメモリ最適化エンコーディングを実装し、`OBJECT ENCODING` で観測できるようにする。
