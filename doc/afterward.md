# my-redis 振り返り

Redis 互換のインメモリデータストアを Clojure で実装したもの。本物の `redis-cli` がそのまま接続できる。

学習目的の実装であり、本番利用は想定していない。「Redis が内部で何をしているか」を手で確かめることが目的。

---

## できること

```bash
lein run                      # 6380 番で起動
redis-cli -p 6380
```

```
127.0.0.1:6380> SET user:1 "Aki"
OK
127.0.0.1:6380> ZADD ranking 100 user:1 250 user:2
(integer) 2
127.0.0.1:6380> ZREVRANGE ranking 0 9 WITHSCORES
1) "user:2"
2) "250"
3) "user:1"
4) "100"
127.0.0.1:6380> SET session:abc "token" EX 1800
OK
127.0.0.1:6380> TTL session:abc
(integer) 1800
```

- RESP2 プロトコル（inline command を含む）
- String / List / Hash / Set / Sorted Set
- TTL（受動的・能動的期限切れ）と eviction
- AOF（追記・リプレイ・rewrite）と RDB スナップショット
- Pub/Sub

---

## アーキテクチャ

```
        redis-cli / TCP クライアント
                │  RESP バイト列
                ▼
   ┌──────────────────────────┐
   │ server（接続ごとに1スレッド）│  読み書き・パースは並行
   └──────────────────────────┘
                │  {:cmd [...] :client conn}
                ▼
   ┌──────────────────────────┐
   │ executor（1本のスレッド）   │  ← ここだけ直列。原子性の源
   └──────────────────────────┘
                │
                ▼
   ┌──────────────────────────┐
   │ command（コマンド表）       │  arity / write? / denyoom?
   └──────────────────────────┘
         │           │            │
         ▼           ▼            ▼
   ┌──────────┐ ┌─────────┐ ┌──────────┐
   │ db       │ │ aof     │ │ pubsub   │
   │ keyspace │ │ rdb     │ │ registry │
   └──────────┘ └─────────┘ └──────────┘
         │
         ▼
   ┌──────────────────────────┐
   │ types: list / zset        │
   │        skiplist           │
   └──────────────────────────┘
```

### 名前空間と依存の向き

上の行は下の行を知らない。一方向を厳守している。

```
my-redis.resp             バイト列 ↔ Clojure データ。何にも依存しない
my-redis.types.list       両端キュー
my-redis.types.skiplist   span 付き skiplist
my-redis.types.zset       skiplist + score 辞書
my-redis.db               キースペース・TTL・eviction 用の索引
my-redis.config           サーバ設定
my-redis.eviction         maxkeys に達したときの退避
my-redis.rewrite          状態 → コマンド列（AOF rewrite）
my-redis.aof / my-redis.rdb   永続化
my-redis.pubsub           購読レジストリ
my-redis.executor         コマンド実行の直列化
my-redis.command          コマンド表とディスパッチ
my-redis.server           ソケット。全体を束ねる
```

**ハンドラはソケットを知らない。** `(fn [ctx args] -> reply)` という形を 13 日間固定した。おかげで AOF リプレイと RDB ロードが、ネットワークを介さず `dispatch` を直接呼べる。唯一の例外が `PUBLISH`（後述）。

---

## 設計判断

### 実行の直列化

**I/O とパースは接続ごとのスレッドで並行、コマンド実行だけが 1 本のスレッドで直列。**

Redis 6 以降の `io-threads` と同じ構造。原子性を担保しているのは実行部分の直列性であって、プロセス全体が 1 スレッドであることではない。

当初は「接続ごとに 1 スレッド」で実行も並行にしていたが、ZSet の skiplist（可変構造）を導入した時点で破綻した。`swap!` のリトライで同じ要素が二重に挿入され、複数スレッドが同じ skiplist を同時に書き換えて壊れた。**素朴な実装で破綻を体験してから設計変更した。**

直列化後も `atom` は外していない。CAS のリトライは起きなくなるが、期限切れスレッド・AOF rewrite スレッド・デバッグからの読みが残るため。

### 直列化の内側と外側

**「終わりが読めない処理」を直列化の内側に入れない**という規律を貫いた。これが実装の随所に現れている。

| 処理 | どこで実行するか |
|---|---|
| ソケットの読み書き | 接続スレッド（相手次第でいくらでもブロックする） |
| RESP のパース・生成 | 接続スレッド（共有状態に触らない） |
| コマンド実行 | 実行スレッド |
| 能動的期限切れ | 実行スレッドに投入。ただし 1 サイクル 1ms で打ち切る |
| AOF rewrite のファイル生成 | 別スレッド |
| AOF rewrite の差し替え | 実行スレッド（一瞬で終わる） |
| RDB の `BGSAVE` | 別スレッド |
| RDB の `SAVE` | 実行スレッド（**止まる**。本物も同じ） |

実測で、`SAVE` 中の `PING` は 37 倍遅くなり、`BGSAVE` 中は 1.0 倍だった。

### `nil` に意味を 1 つしか持たせない

同じ種類のバグを 5 回踏んだ。

- `db` 層の値に `nil` を入れない → `get` の `nil` は「キーが無い」だけを意味する
- `dispatch` の返り値で「応答不要」を `:no-reply` にする → `nil` は Null Bulk String 専用
- 型違いを `::wrong-type` センチネルで表す → `nil`（キーが無い）と区別する
- `volatile-keys-seq` が空集合に `nil` を返し、`sample-keys` が「指定なし」と解釈した → `:all` という明示的な値に変更
- プロパティテストで `()` と `nil` がずれた

**「無い」と「空」と「指定されていない」は別物。** 1 つの値に重ねると必ず壊れる。

### 2 本の索引を同時に更新する

同じ構造が 4 箇所に出てきた。

| 用途 | 索引 1 | 索引 2 |
|---|---|---|
| ZSet | member → score（`ZSCORE`） | score 順の skiplist（`ZRANGE`） |
| TTL | `:data`（全キー） | `:expires`（期限付きのみ。サンプリング用） |
| Pub/Sub | channel → 購読者（配信） | 購読者 → channel（切断時の掃除） |

**2 本持つと、常に同時に更新する責任が生まれる。** 片方だけ更新すると「幽霊エントリ」が生まれ、どちらを見るコマンドかで答えが食い違う。ZSet の `ZADD`（既存メンバーのスコア更新）で実際に踏んだ。

### コマンド表の属性

`case` による分岐ではなく、データとして持つ。

```clojure
"SET" {:arity -3 :write? true :denyoom? true :handler cmd-set}
```

- `:arity` — 正数=厳密、負数=最低。**コマンド名を含めて数える**（本物の `COMMAND INFO` と同じ規約）
- `:write?` — AOF に記録するか
- `:denyoom?` — メモリ上限時に拒否するか

**`:write?` と `:denyoom?` は別の軸。** `DEL` と `EXPIRE` は書き込みだがデータを減らす／変えないので、上限時も通す。拒否すると上限から復帰できなくなる。

### サンプリングによる近似

正確さを捨てて応答性を取る判断が 2 箇所にある。どちらも本物と同じ思想。

**能動的期限切れ**：20 件サンプリング → 期限切れが 25% 以上なら繰り返す → 時間上限 1ms で打ち切る。全走査にすると、100 万キーで 1 サイクル数百 ms かかり、その間全クライアントが待つ（実測済み）。

**近似 LRU**：数件サンプリングして最終アクセス時刻が最古のものを捨てる。正確な LRU は全キーの連結リストとアクセスごとのリンク付け替えが必要で、メモリも時間も割に合わない。

`:atime` を更新するのは**値を使う操作だけ**。`EXISTS` / `TTL` / `TYPE` / `KEYS` では更新しない。`KEYS *` で全キーの LRU 情報がリセットされたら、管理コマンドが eviction の判断を壊してしまう。

### AOF は「何をしたか」ではなく「何が起きたか」を記録する

実行時と再生時で結果が変わるコマンドは書き換える。

| 元のコマンド | AOF に書く形 | 理由 |
|---|---|---|
| `SPOP s` | `SREM s <消えた要素>` | ランダムなので再生で別の要素が消える |
| `EXPIRE k 10` | `PEXPIREAT k <絶対時刻>` | 相対時間だと再生で期限が延びる |
| `SET k v EX 10` | `SET k v` + `PEXPIREAT k <絶対時刻>` | 同上 |

**サーバ自身の削除（期限切れ・eviction）も `DEL` として記録する。** 記録しないと、消えたはずのキーがリプレイで復活する。

状態が変わらなかったコマンド（`nil` やエラーを返したもの）は記録しない。

### 実行 → 応答 → AOF の順

my-storage（LSM-tree）の WAL とは**逆**にしている。

```
WAL:   ログに fsync → メモリに反映 → OK を返す
Redis: 実行 → +OK を返す → AOF に write
```

**Redis の `+OK` は「ディスクに残った」を意味しない。** `appendfsync always` にしても、応答を返してから fsync するので電源断では失われる。性能のために耐久性の保証レベルを下げる、という意識的な判断。

実測：`everysec` のコストは +4%、`always` は 76 倍遅い（秒間 105 コマンド）。**コストを払っているのに保証は得られていない**ので、`always` を選ぶ理由は薄い。

### スナップショットがタダであること

AOF rewrite と RDB の両方で、「ある時点の状態」を別スレッドに渡す必要がある。

**本物は `fork()` + copy-on-write。** ページテーブルを複製し、親が書き込んだページだけが物理的にコピーされる。書き込みが激しいと最悪メモリ 2 倍、fork 自体も巨大データでレイテンシスパイクを起こす。

**今回はイミュータブルなマップの参照を取るだけ。** 実測で 0.0002ms。取得後に実行スレッドが書き込んでも、手元のマップは変わらない。構造共有なので、増えるのは差分ぶんだけ。

**同じ発想が OS の層と言語の層の両方に実装されている。**

### rewrite の境界は 2 箇所ある

「長い処理は外、短い切り替えは中」にしたぶん、取りこぼしうる境界が 2 箇所できた。

```
prepare（実行スレッド）  オフセットとスナップショットを不可分に取る
   ↓
build!（別スレッド）     一時ファイル生成 + 旧 AOF の offset 以降をコピー
   ↓
commit!（実行スレッド）  build! 以降に追記されたぶんを再コピー → rename → ハンドル差し替え
```

`commit!` の再コピーを忘れて、rewrite 中の最後の 1 件が失われるバグを実際に出した。rename 後に旧ハンドルへ書くと、**どこからも参照されない inode に書かれて消える**（Unix では開いているファイルを rename してもディスクリプタは元の inode を指し続ける）。

---

## 対応データ型

| 型 | 内部表現 | 主なコマンド | ユースケース |
|---|---|---|---|
| String | `String`（UTF-8） | `SET` `GET` `INCR` `APPEND` `MSET` `MGET` 等 | キャッシュ、カウンタ、レートリミッタ、分散ロック |
| List | 2 本のベクタによる両端キュー | `LPUSH` `RPUSH` `LPOP` `RPOP` `LRANGE` `LREM` `LTRIM` 等 | ジョブキュー、直近 N 件のログ、タイムライン |
| Hash | `PersistentHashMap` | `HSET` `HGET` `HGETALL` `HINCRBY` 等 | セッション、オブジェクトのフィールド単位の操作 |
| Set | `PersistentHashSet` | `SADD` `SMEMBERS` `SINTER` `SUNION` `SDIFF` 等 | タグ、ユニーク判定、集合演算 |
| Sorted Set | score 辞書 + span 付き skiplist | `ZADD` `ZRANGE` `ZRANK` `ZRANGEBYSCORE` 等 | ランキング、優先度付きキュー |

型を問わないコマンド：`EXISTS` `TYPE` `KEYS` `DBSIZE` `FLUSHDB` `OBJECT` `INFO` `CONFIG` `PING` `ECHO` `QUIT`

### List：2 本のベクタ

```
論理リスト:  1 2 3 4 5 6
front: [3 2 1]     ← 前半を逆順で保持
back:  [4 5 6]     ← 後半を正順で保持
```

**先頭側を逆順で持つ**ことで、両端の操作がどちらもベクタの末尾操作（`conj` / `peek` / `pop`、すべて O(1)）になる。片側が空になったら、もう片方の半分を移す（償却 O(1)）。

素朴にベクタ 1 本で実装すると `LPUSH` が O(N) になる。実測で 16000 要素のとき 2903ms → 39ms（74 倍）。

本物の quicklist と違い、**中間アクセス（`LINDEX` / `LSET`）も O(1)**。仕様上は本物より速い。

### Sorted Set：span 付き skiplist

```
level = 3, length = 4

Lv2:  header ─────────────(4)──────────────▶ d(40) ─(0)─▶ NIL
Lv1:  header ─────(3)─────▶ c(30) ─(1)─▶ d(40) ─(0)─▶ NIL
Lv0:  header ─(1)─▶ a(10) ─(1)─▶ b(20) ─(1)─▶ c(30) ─(1)─▶ d(40) ─(0)─▶ NIL
```

各ポインタが「最下段で何ノード分を跨ぐか」（span）を持つ。**探索しながら通ったポインタの span を合計すれば、それが順位**になる。

`ZRANK c` なら、`header → c`（Lv1、span 3）で到達して合計 3。1 始まりの順位なので `ZRANK` は 2 を返す。最下段を端から数えないので、要素数ではなく段数（≒ log N）に比例する。

実測：10 万要素の `ZRANK` が **53ms → 0.01ms**（素朴な sorted-set 実装との比較）。ランキング上位を取る `ZREVRANGE 0 9` も同様に改善した。

skiplist だけ可変構造にしている。各ノードの forward 配列を破壊的に更新するのが本質で、永続化すると空間・時間の両方で割に合わないため。**イミュータブルが常に正解ではない**ことの実例。

---

## 永続化

### AOF と RDB の比較（実測）

| | AOF | RDB |
|---|---|---|
| 中身 | 操作の履歴（RESP のテキスト） | 状態のスナップショット（バイナリ） |
| サイズ | RDB の 1.08〜1.35 倍 | 基準 |
| 起動（10 万キー） | 2620 ms | **530 ms** |
| 書き出し | 追記のたび（`everysec` で +4%） | 10 万キーで 200ms を一括 |
| 損失 | 最大 1 秒（`everysec`） | 前回保存以降すべて |

サイズの差は**ほぼ固定のオーバーヘッド**（1 キーあたり約 10 バイト）。RESP の型記号と改行のぶん。値が長くなると相対的に薄まる。

起動時間の差は、AOF が 10 万回 RESP パース → コマンド表の引き → arity チェックを通るのに対し、RDB は長さプレフィックスを読むだけだから。**本物はさらに速い**（RDB から直接キースペースを構築し、コマンドを経由しない）。

### AOF rewrite

キースペースの現在の状態から、それを再現する最小のコマンド列を生成する。

**履歴を畳む処理は無い。** `SET counter` を 10000 回実行しても、メモリ上には最後の値しか残っていない。rewrite はそれを見て 1 コマンドを出すだけ。my-storage の compaction が複数の SSTable を k-way マージして「新しい方を採用」していたのとは違う。インメモリなので、現在の状態がそのまま答え。

巨大なキーは 64 要素ずつに分割する。**必ず `RPUSH` で出力する**（`LPUSH` だと分割した塊の中も塊同士も逆順になる）。

自動トリガは「前回 rewrite 直後のサイズの N% 以上」かつ「min-size 以上」の両方。基準が「前回 rewrite 直後」なので、**畳めるものが無ければ次の rewrite までの猶予が自然に延びる**。

### RDB 形式

本物との互換は目標にしていない。my-storage の SSTable と同じ作法。

```
[マジック "MYRDB001"]   8 bytes
[エントリ数]             4 bytes
各エントリ:
  [型タグ]              1 byte
  [キー]                長さ付き文字列
  [期限]                8 bytes（0 = 期限なし）
  [ペイロード]           String は長さ付き文字列、他は個数付きのその並び
[チェックサム CRC32]     8 bytes
```

**「長さ付き文字列」と「個数付きその並び」の 2 つで全型を表現できる。** Day 1 の RESP（Bulk String と Array）と同じ構造。Hash と ZSet は平坦化する。

**チェックサムは読む前に検証する。** RDB はファイル全体が 1 つの状態なので、「半分だけ復元する」に意味がない。全部読めるか何も読めないかの二択。AOF が末尾のレコード単位で切り捨てるのとは対照的（追記ログなので壊れるのは常に末尾）。

### 起動時のロード順

`appendonly` が有効なら AOF、無効なら RDB。本物と同じ優先順位。

---

## Pub/Sub

**13 日間守ってきた「ハンドラはソケットに依存しない」という規律の、唯一の例外。**

`PUBLISH` は、実行した接続とは**別の接続のソケットに書く**必要がある。返り値だけでは表現できない。

例外を 1 箇所に閉じ込めるため、`IClient` プロトコルで「送る手段」だけを抽象化した。ソケットそのものは渡さない。テストでは関数に差し替えられる。

```clojure
(defprotocol IClient
  (send-push! [this payload])
  (client-id [this]))
```

接続ごとの状態が初めて必要になったので、executor へのタスクを `{:cmd [...] :client conn}` にした。

### 既知の制約：遅い購読者

`PUBLISH` は**実行スレッドから購読者のソケットに直接書く**。`flush` は相手のネットワーク次第でブロックしうるので、**1 人の遅い購読者が全クライアントを止めうる**。

本物も同じ問題を持ち、購読者ごとに出力バッファを持って**溢れたら接続を切る**（`client-output-buffer-limit pubsub`）。今回はそこまで実装していない。

### Pub/Sub の性質

配信保証も永続化もない。購読者がいなければメッセージは消える。**だから Streams や Kafka が別に存在する。**

---

## 本物との差分

### 意図的に実装していないもの

- **バイナリセーフでない。** キーと値を UTF-8 文字列に限定した。本物の String は任意のバイト列（最大 512MB）
- **エンコーディング最適化（listpack / intset / quicklist）。** 小さいコレクションを連続メモリに詰める最適化。`OBJECT ENCODING` は常に非圧縮側の実装名を返す
- **`maxmemory` はバイト数ではなくキー数**（`maxkeys`）。JVM で実サイズを測るのは本題から外れるため、名前を変えて嘘をつかない形にした
- RESP3、レプリケーション、Cluster、Lua、MULTI/EXEC、io multiplexing、Streams、ACL
- `SCAN` 系、`PSUBSCRIBE`、`LINSERT`、`BLPOP`、`HMGET`、`SRANDMEMBER`、`ZADD` のオプション、`ZRANGE` の新書式
- 設定の永続化（`CONFIG REWRITE`）、RDB の自動保存（`save N M`）

### 実装上の制約

- **`maxkeys` はソフトリミット。** `MSET` のような複数キー操作は事前に増加量を予測できないので、一時的に超える。本物の `maxmemory` も同じ
- **LRU のサンプリングは一様でない。** Clojure の永続マップから O(1) でランダムなキーを取る手段がないため、内部順の先頭から一定数を取っている。キー数がサンプル数を大きく超えると、LRU の精度はランダムに近づく。本物は dict のバケット配列を直接引く
- **`MGET` と `SINTER` は `:atime` を更新しない**（スナップショット経由のため）
- **RDB の `serialize` はバイト配列全体をメモリに作る。** 本物はストリーミングで書きながら CRC を更新する
- **RDB の復元もコマンド列を経由する。** 本物は直接キースペースを構築する
- **`current-size` が `flush` を伴う。** サイズ測定が副作用として書き込みを進めている。`AOF` にバイト数カウンタを持たせるのが本来の形
- **AOF リプレイ中のエラーは中断せず続行する。** 本物より寛容
- **`GETSET` が期限を引き継ぐ。** 本物は消す
- **`command/dispatch` は直列化された単一スレッドからの呼び出しを前提とする。** ZSet の skiplist が可変構造のため、複数スレッドから同時に呼ぶと壊れる

### 共通の規約（本物と一致）

- **不在キーの扱いは返り値の型が決める。** Integer を返すコマンドは 0（`LLEN` `STRLEN`）、Array は空配列（`LRANGE` `SMEMBERS`）、Bulk String は Null（`GET` `LPOP`）。Integer に Null を表現する方法がないため
- **コレクションが空になるとキーごと消える**
- **型違いの操作は `WRONGTYPE`。** ただし中身を見ないコマンド（`EXISTS` `TYPE` `DEL` `SETNX` `SET`）は型を問わない
- **順序を保証するのは `LRANGE` と `ZRANGE` 系のみ。** `HGETALL` `SMEMBERS` `KEYS` の並び順は不定

---

## テスト

```bash
lein test
```

### 参照オラクルテスト

本物の redis と自作に同じコマンド列を流し、応答を比較する。

```bash
docker run --rm -d -p 6379:6379 --name redis-oracle redis
lein test my-redis.oracle-test
```

本物が無ければスキップされる（`Ran N tests containing 0 assertions` で判別できる）。

順序が不定なコマンドは集合として比較し、非決定的なもの（`SPOP`）と実装差分があるもの（`OBJECT` `INFO`）は除外している。

### プロパティテスト

- `resp`：任意のデータについて encode → decode が元に戻る
- `skiplist`：ランダムな insert/delete 列で、素の `sorted-set` と一致し、span が全数正しい
- `zset`：ランダムな操作列で、素朴な参照モデル（毎回ソートするマップ）と一致する

**「実装 vs 単純なモデル」の等価検査**が、ステートフルなシステムのテストとして極めて強力だった。失敗ケースを最小まで縮小してくれるのも効いた（空の ZSet で `()` と `nil` がずれたバグが、`:smallest [[]]` の一言で分かった）。

---

## 計測

| 項目 | 結果 |
|---|---|
| `LPUSH`（16000 要素） | 2903ms → 39ms（2 本のベクタ化） |
| `ZRANK`（10 万要素・最下位） | 53ms → 0.01ms（span 付き skiplist） |
| AOF `appendfsync always` | 2.99 ms/write（335 writes/sec） |
| AOF `appendfsync everysec` | 0.0039 ms/write（254,000 writes/sec） |
| サーバ経由・AOF なし | 0.1248 ms/write |
| サーバ経由・`everysec` | 0.1300 ms/write（+4%） |
| サーバ経由・`always` | 9.5327 ms/write（76 倍） |
| 起動（10 万キー・AOF） | 2620 ms |
| 起動（10 万キー・RDB） | 530 ms |
| `SAVE` 中の `PING` | 通常の 37 倍 |
| `BGSAVE` 中の `PING` | 通常の 1.0 倍 |
| AOF rewrite 中の `PING` | 通常の 1.1 倍 |
| スナップショット取得（50 万キー） | 0.0002 ms |

---

## 参考

### 一次資料

- Redis Docs — Protocol specification (RESP)
- Redis Docs — Data types / Key eviction / Persistence

### 本物のソース（github.com/redis/redis の `src/`）

- `t_zset.c` の `zslInsert` / `zslGetRank` — skiplist。`update[]` `rank[]` という変数名まで共通
- `expire.c` の `activeExpireCycle` — 能動的期限切れのサンプリングと時間上限
- `aof.c` / `rdb.c` — 永続化
- `commands.def` — コマンド表

### 論文・書籍

- Pugh, W. "Skip Lists: A Probabilistic Alternative to Balanced Trees" (1990)
- 『Designing Data-Intensive Applications』第 3 章・第 7 章

### 参照オラクル

```bash
docker run --rm -d -p 6379:6379 --name redis-oracle redis
```

**仕様の記憶より実物が正。** `MSET` のエラー文面、`hash-max-listpack-entries` の既定値、`DEL k k k` の返り値、`Double/parseDouble` の寛容さ——すべて実物を叩いて初めて正しい挙動が分かった。
