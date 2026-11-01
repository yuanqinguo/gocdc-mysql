# 10 · 表级字段过滤与脱敏

> 每表可声明排除列（整列不下发）与脱敏列（值变换），对 MQ 与 MySQL sink、全量与增量行为一致——给脱敏供数、合规下发提供管线内建能力。

## 1. 配置模型（每规则）

```jsonc
{
  "schema": "edu.mall.account",
  "table": "user_auth",
  "exclude_columns": ["password", "id_card"],        // 整列不下发（任何 sink 都收不到）
  "mask_columns": {                                   // 值脱敏（列仍下发，值被替换）
    "phone":   "phone",                               // 内置算法名
    "remark":  { "type": "truncate", "keep": 32 },
    "email":   { "type": "hash_sha256", "salt_env": "GOCDC_MASK_SALT" }
  }
}
```

### 算法集（内置，可扩）

| type | 行为 |
|------|------|
| `fixed:<value>` | 替换为固定值（默认 `***`） |
| `hash_md5` / `hash_sha256` | hex 摘要；可选 `salt_env` 加盐（防彩虹表反查） |
| `phone` | 保留前 3 后 4：`138****1234` |
| `email` | 保留首字符与域名：`a***@example.com` |
| `truncate:{keep}` | 截断保留前 keep 字符 + `...` |
| `null` | 置 null |

## 2. 语义与生效位置

- 处理链位置：`值转换 → exclude/mask → sink 载荷构建`（transform 层单点实现），**增量与全量自动一致**（全量复用同一转换器）。
- `exclude_columns`：列从事件载荷中物理移除——MQ 消息 `after/before` 无此列、MySQL sink INSERT/UPDATE 列清单不含此列。
- `mask_columns`：值替换后正常下发；消息 `columns` 元信息标注 `"masked": "<type>"`，消费方可识别（05 §2.1）。

## 3. MySQL sink 的约束（与「同名同表」的交叉）

| 场景 | 行为 | 校验 |
|------|------|------|
| exclude 列，目标表无该列 | ✅ 直接工作（INSERT 省略列） | 启动结构核对通过 |
| exclude 列，目标表仍有该列 | 目标列取 DEFAULT/NULL；启动**告警**（提示目标表也应删列，避免语义混乱） | 不阻塞 |
| exclude 主键/唯一键列 | ❌ 拒绝启动（破坏幂等寻址） | fail-fast |
| exclude 目标表 NOT NULL 无默认值列 | ❌ 拒绝启动（写入必失败） | fail-fast |
| mask 列 | ✅ 写入脱敏值；若该列参与唯一约束会告警（脱敏碰撞） | 启动核对 |

## 4. MQ 消息中的体现

- exclude：字段不出现。
- mask：字段出现但值已脱敏 + 元信息标注。
- 主键值**永不脱敏/排除**（配置校验拦截），保证下游幂等与寻址。

## 5. 安全实现要点

- 脱敏在进程内完成，**日志与指标中不输出未脱敏的敏感列值**：日志打印行数据时统一走同一 exclude/mask 视图（logging 层强制）。
- 加盐哈希的 salt 从环境变量注入（不进 etcd 配置明文）。
- 脱敏不可逆是**明示语义**：需要可逆加密的场景不在本期范围（预留 `mask_columns` 的 type 扩展点）。
- include_columns 不提供：管线内建场景是「排除/脱敏」而非「白名单重建表」；MySQL sink 同表约束下 include 会导致列集非空缺失引发写入失败，语义容易踩坑——反向用 `exclude_columns` 表达。

## 6. 校验与测试

- 启动校验：列存在性、主键保护、NOT NULL 冲突（结合目标表结构核对）。
- 单测：每算法黄金样本；增量/全量输出一致性；`before` 像同样脱敏（前像不漏敏）。
- 集成：配置脱敏后端到端比对 MQ 消息与目标表内容。
