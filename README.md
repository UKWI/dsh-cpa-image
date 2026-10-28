# dsh-cpa-image

**让任何 `llm/stream` 调用都能收下 CPA 风格的图片**——不改任何 provider 的适配器，
也不新增 provider id。

## 问题

CPA（以及它代理的一些上游）把生成的图片放在 OpenAI 协议里**不存在**的字段：

| 传输方式 | 图片位置 | `message.content` |
|---|---|---|
| 流式 | `choices[0].delta.images[]` | 空 |
| 一次性 | `choices[0].message.images[]` | 空 |

pi-ai 的 openai-completions 适配器只读 `delta.content`，于是整条消息组装出零个内容块，
harness 判为 `EMPTY_RESPONSE`，默认重试 5 次（每轮 6 次请求，每次重新生成、重新计费），
最后报错。而 `gpt-image-*` 这类模型在 `chat/completions` 上直接 503，只在
`/v1/images/generations` 可用。

## 为什么是中间件而不是适配器

harness 里**一个 provider 路由只能有一个适配器**，而那个适配器承载着该 provider 的配置、
凭据、重试、replay、工具调用等全部逻辑。为了修一个字段去接管一条路由，等于以后每遇到
一个上游就要复制一份适配器。

`llm/stream` 是 harness 在**适配器选择之上**给出的瀑布式拦截点：

```ts
'llm/stream'(options, next): AsyncIterable<StreamChunk>
```

监听它就能包裹**每一次**模型调用，无论背后是哪个 provider、哪种协议。所以本插件：

- **不注册任何 provider**，Models 设置页、凭据、重试策略全部保持原样；
- 对 `gpt-image-*` 直接短路到 `/v1/images/generations`；
- 其余调用原样透传给原适配器，同时观察它收到的 HTTP 响应，把 `delta.images` /
  `message.images` 收成持久化附件，追加成图片块；
- 若原适配器因为"内容为空"而报了 `EMPTY_RESPONSE`，在图片补上之后改写为正常完成。

## 工作原理

```
llm/stream 拦截器
   ├─ 图片模型 → POST /v1/images/generations → image 块
   └─ 其他模型 → next()（原适配器，什么都不变）
                   │
                   └─ 适配器内部 fetch
                        │  AsyncLocalStorage 把这次调用与响应绑定
                        ▼
                   被包裹的 fetch：读出响应字节 → 原样转交 → 顺带扫描
```

两个关键实现细节，都是踩坑换来的：

1. **不能用 `response.clone()`**。克隆一条正在传输的网络流、只读副本，会让 Node/undici
   的原始读取端卡死——实测表现为 pi-ai 的 `stream idle timeout after 300000ms`。
   所以 tap 自己持有唯一的 reader，把同样的字节重新包装成新流交给适配器。
2. **解析必须同步、不背压**。字节经过时只做切行 + `JSON.parse`，图片的解码与落盘是
   投递到后台队列的异步任务，绝不阻塞 `pull`。

## 安装

```bash
./install.sh                             # 拷到 $DSH_HOME 的 profile 并写 bundle 清单
# 重启 dsh web
```

`install.sh` 只负责 bundle 本身。**profile 的 `llm-pi-ai` 保持原样即可**——本插件不需要
任何 provider 让位；如果你之前为了接管路由把 `providers` 清空过，恢复原配置即可。

`./install.sh --dry-run` 只打印计划；`./install.sh --uninstall` 反向删除。

## 配置

`cordis.patch.yml` 里的 `config:`，全部可选：

| 键 | 默认 | 含义 |
|---|---|---|
| `baseURL` | `http://127.0.0.1:8317/v1` | 仅 `/images/generations` 短路使用 |
| `apiKey` / `apiKeyEnv` | – / `CPA_API_KEY` | 同上；走 credentials 缝合点读取 |
| `hosts` | `[]` | 只观察这些主机的响应；空 = 所有主机 |
| `imageModels` | `gpt-image-*` 五个 | 走 images 端点的模型 |
| `autoImageModels` | `true` | 另外把名字含 `image` 的模型也当图片模型 |
| `imagesOptions` | `{}` | 透传给 `/images/generations`（`size`、`quality`…） |
| `maxImageBytes` | 32 MiB | 超过则跳过该图，不报错 |

聊天请求的 baseURL、模型、密钥**不来自这里**——它们仍然由各 provider 自己的配置决定。

## 验证

```bash
cd dsh-cpa-image
node --test test/                              # 7 个离线用例，不联网
DSH_CPA_DEBUG=1 dsh --profile web headless ... # 打印 tap 过程
```

真实链路验证方式：在 headless profile 里让 **pi-ai 照常持有 `cpa`**，只加载本插件，
然后让它画一张图——会话里应当出现 `image` 块。

## 已知限制

- tap 包裹的是全局 `fetch`。作用域由 `hosts` 与 content-type（SSE / JSON）双重收窄；
  不在范围内的请求原样放行，不做任何缓冲。
- 只识别 **OpenAI 形状**的响应体。别的协议（Anthropic messages 等）本来也不会返回这种
  字段，拦截是无害的空操作。
- 图片的解码发生在字节经过时，**不额外请求上游**，因此不会重复计费。
- `gpt-image-*` 的图生图（`/images/edits`，multipart）尚未接入。
