# Proposal: support `pinyin` order for `utf8mb4` charset

- Author(s):     [xiongjiwei](https://github.com/xiongjiwei)
- Last updated:  2020-11-06
- Discussion at: https://github.com/pingcap/tidb/issues/19747

## Abstract
This proposal introduces a new feature that supports `pinyin` order for Chinese characters.

## Background
It's unable now to order by a column based on its pinyin order. For example:

```sql
create table t(
	a varchar(100)
)
charset = 'utf8mb4' collate = 'utf8mb4_zh_0900_as_cs';

# insert some data (ASCII placeholders for Chinese words):
insert into t values ("zh_text"), ("a_zh_text");

# a query requires to order by column a in its pinyin order:
select * from t order by a;
+-----------+
| a         |
+-----------+
| a_zh_text |
| zh_text   |
+-----------+
2 rows in set (0.00 sec)
```

## Proposal

`pinyin` order for Chinese characters supported by this proposal adds a new collation named `utf8mb4_zh_pinyin_tidb_as_cs`. It supports all Unicode and sorts Chinese characters according to the PINYIN collation in the zh.xml file of [CLDR24](http://unicode.org/Public/cldr/24/core.zip). It only supports Chinese characters with `pinyin` in zh.xml; it does not support CJK characters whose Unicode category is Symbol with a similar shape to Chinese characters, nor PINYIN characters themselves. In `utf8mb4_zh_pinyin_tidb_as_cs`, `utf8mb4` is the charset, `zh` means Chinese language, `pinyin` means pinyin order, `tidb` is a TiDB-specific variant, and `as_cs` means accent-sensitive and case-sensitive.

### Advantages

It's a lot of work to implement `utf8mb4_zh_0900_as_cs`. The MySQL implementation looks complicated with weight reorders, magic numbers, and tricks. Implementing `utf8mb4_zh_pinyin_tidb_as_cs` is much easier. It supports Chinese characters and sorts them in pinyin order. It is good enough.

### Disadvantages

It is not compatible with MySQL. MySQL does not have a collation named `utf8mb4_zh_pinyin_tidb_as_cs`.

## Rationale

### How to implement

#### Compare and Key

- For any Chinese character, which has non-zero seq NO. defined in zh.xml according to its gb18030 code, the final weight shall be 0xFFA00000+(seq No.)
- For any non-Chinese gb18030 character 2 bytes C, the final weight shall be C itself.
- For any non-Chinese gb18030 character 4 bytes C, the final weight shall be 0xFF000000+diff(C)(we get diff by Algorithm).

### Parser

Choose collation ID `2048` for `utf8mb4_zh_pinyin_tidb_as_cs` and add it into parser.

> MySQL supports two-byte collation IDs. The range of IDs from 1024 to 2047 is reserved for user-defined collations. [see also](https://dev.mysql.com/doc/refman/8.0/en/adding-collation-choosing-id.html)

### Compatibility with current collations

`utf8mb4_zh_pinyin_tidb_as_cs` has same priority with `utf8mb4_unicode_ci` and `utf8mb4_general_ci`, which means these three collations incompatible with each other.

### Alternative
MySQL has a lot of language specific collations, for `pinyin` order, MySQL uses collation `utf8mb4_zh_0900_as_cs`.

## Compatibility and Migration Plan

### Compatibility issues with MySQL

There is no `utf8mb4_zh_pinyin_tidb_as_cs` collation in MySQL. We can comment `utf8mb4_zh_pinyin_tidb_as_cs` when users need to replicate their data from TiDB to MySQL.

## Open issues (if applicable)

https://github.com/pingcap/tidb/issues/19747

https://github.com/pingcap/tidb/issues/10192
