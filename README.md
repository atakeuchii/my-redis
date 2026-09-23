# my-redis

Clojure で書いた、redis-cli がそのまま繋がるインメモリデータストア（学習用）。

[![Build & Test](https://github.com/atakeuchii/my-redis/actions/workflows/build.yml/badge.svg)](https://github.com/atakeuchii/my-redis/actions/workflows/build.yml)

## クイックスタート

### 起動

#### dev
```clojure
(require '[my-redis.server :as server])
(def d (server/start! 6380))
...
(server/stop! d)
```

#### server
```sh
PORT=6380 lein run server
```

## 対応データ型

| 型 | 内部表現 | 代表的なユースケース |
|---|---|---|
| String | `String`（UTF-8） | キャッシュ、カウンタ、レートリミッタ、分散ロック |
| List | 2 本のベクタによる両端キュー（`types/list.clj`） | ジョブキュー、直近 N 件のログ、タイムライン |
| Hash | `PersistentHashMap` | セッション、オブジェクトのフィールド単位の読み書き |
| Set | `PersistentHashSet` | タグ、ユニーク判定、フォロー関係の積・和・差 |
| Sorted Set | member→score の辞書 + skiplist | ランキング、優先度付きキュー、時系列インデックス |

### 使い方の例

```
# String：カウンタとレートリミッタ
SET page:views 0
INCR page:views                 # → 1。read-modify-write が 1 コマンドで原子的
SET ratelimit:user:42 1 EX 60   # 60 秒で失効（TTL は Day 9 で有効化）

# List：直近 5 件だけ保持するログ
LPUSH log:42 "entry"
LTRIM log:42 0 4                # 冪等。現在の長さを知らなくてよい
LRANGE log:42 0 -1

# List：ジョブキュー
RPUSH queue:email "{...}"       # 生産者
LPOP queue:email                # 消費者。1 ジョブは 1 ワーカーにしか渡らない

# Hash：セッション
HSET session:abc user_id 42 login_at 1699000000
HGET session:abc user_id
HGETALL session:abc             # field, value, field, value のフラット配列

# Set：タグと集合演算
SADD tags:post1 clojure redis
SADD tags:post2 redis database
SINTER tags:post1 tags:post2    # → redis（最小の集合から走査する）

# Sorted Set：ランキング
ZADD ranking 100 user:1 250 user:2
ZINCRBY ranking 10 user:1       # → "110"
ZREVRANGE ranking 0 9 WITHSCORES  # 上位 10 人
ZRANK ranking user:1            # 自分の順位。O(log N)
```

### 対応コマンド

see `src/my_redis/command.clj/command-table`

## 実装のポイント

### db/update-entry!
- 前提: データは全て{k {:type type :value value}}の形式で保存されている。type=:stringならvalueはstringで、type=:listならvalueがDList、... となる
- データを更新するときはupdate-entry!を使う
  - 中でswap-vals!を使うことで、read-modify-writeが確実に行える。fが複数呼ばれても壊れない
  - ただしfには副作用を書かないこと

### quicklist(dlist)
- 両端キューを自作
  - front、backをおよそ半分に分けてそれぞれget/setする
    - frontは逆順、backは昇順に詰める。pop, first, peek を使うことで高速化
    - ex. {:front [3 2 1] :back [4 5 6]}
  - 両端O(1), 中間アクセスO(N)

### ZSet
- score 順に並び替え可能なリスト
- score の辞書とskiplist を二重で持っており、同時に更新する
  - 値を追加・更新するたびにスコア順に整列する
  - データの追加・削除・検索を高速に行える

### skiplist
- zsetで使うためのlist
- 32段の高さのリストを持ち、上から該当場所を探していく。
  - 自分より小さい値が見つかるまで右に走査し、見つかったら段を1つ降りる、を繰り返す
  - 挿入時は、場所が決まった後ランダムな高さに配置する

### 具体例：a(10) b(20) c(30) d(40) が入っている状態

高さが a=1, b=1, c=2, d=3 になった場合。

```
level = 3, length = 4

Lv2:  header ─────────────(4)──────────────▶ d(40) ─(0)─▶ NIL
Lv1:  header ─────(3)─────▶ c(30) ─(1)─▶ d(40) ─(0)─▶ NIL
Lv0:  header ─(1)─▶ a(10) ─(1)─▶ b(20) ─(1)─▶ c(30) ─(1)─▶ d(40) ─(0)─▶ NIL
```

| ノード | 高さ | forward | span |
|---|---|---|---|
| header | 32 | `[a, c, d, nil, ...]` | `[1, 3, 4, 4, ...]` |
| a(10) | 1 | `[b]` | `[1]` |
| b(20) | 1 | `[c]` | `[1]` |
| c(30) | 2 | `[d, d]` | `[1, 1]` |
| d(40) | 3 | `[nil, nil, nil]` | `[0, 0, 0]` |

読み方：

- `header.forward[0] = a` — Lv0 で最初に来るのは a。`span[0] = 1` は 1 個先へ進む
- `header.forward[1] = c` — Lv1 では a と b を飛ばして c へ。跨いだぶんを含めて `span[1] = 3`
- `header.forward[2] = d` — Lv2 では 4 個先の d へ一気に飛ぶ
- `c.forward = [d, d]` — 同じノードでも段ごとに別のポインタを持つ（ここでは両方 d）
- `d.span = [0, 0, 0]` — d の先は末尾なので、どの段も距離 0

## ベンチマーク

| ケース | my-redis | Redis | 備考 |
|---|---|---|---|

## 実装状況

- [x] RESP パーサ / シリアライザ
- [x] TCP サーバと接続ハンドリング
- [x] コマンドディスパッチとキースペース
- [x] String コマンド
- [x] List / Hash / Set
- [x] Sorted Set（自作 skiplist）
- [ ] TTL と eviction
- [ ] AOF（追記・リプレイ・rewrite）
- [ ] RDB スナップショット
- [ ] Pub/Sub

## 本家 Redis との差異・制限

### 未実装

- `LINSERT` `BLPOP` `LPOP count` `HMGET` `HSETNX` `SRANDMEMBER` `SMOVE` `ZADD` のオプション（`NX` `XX` `GT` `LT` `CH` `INCR`）、`ZRANGE` の新書式（`BYSCORE` `REV` `LIMIT`）、`ZREVRANGEBYSCORE` `SCAN` 系

### 差分

- キーと値は UTF-8 文字列に限定（バイナリセーフではない）
- エンコーディング最適化（listpack / intset）は未実装。`OBJECT ENCODING` は常に非圧縮側の名前を返す
- List の `LINDEX` / `LSET` は本物が O(N) のところ O(1)（2 本のベクタによる実装のため）

## 参考資料

### 文献

- [**Redis Docs**](https://redis.io/docs/latest/)
- **Pugh, W. "Skip Lists: A Probabilistic Alternative to Balanced Trees" (1990)** — skiplist の原論文。確率的にバランスさせる発想、回転が不要な理由
- **Kirsch & Mitzenmacher, "Less Hashing, Same Performance"** — ダブルハッシュ（Bloom filter 用。前プロジェクトで使用）
- **『Designing Data-Intensive Applications』** — 3章, 7章

### ソースコード
- [redis/redis](https://github.com/redis/redis)
  - `t_zset.c` — zskiplist の挿入・削除・span の更新。今回の `skiplist.clj` はこれを模したもの
  - `t_string.c` / `t_list.c` / `t_hash.c` / `t_set.c` — 各型のコマンド実装
  - `expire.c` — 能動的期限切れのサンプリングと時間上限（Day 9-10）
  - `aof.c` / `rdb.c` — 永続化（Day 11-13）
  - `commands.def` — コマンド表。`:arity` `:write?` を持つ今回の設計の元ネタ

### 動作確認

```bash
docker run --rm -d -p 6379:6379 --name redis-oracle redis
docker exec -it redis-oracle redis-cli
```
