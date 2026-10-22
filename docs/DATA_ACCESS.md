# 数据取得与平台边界

## 淘宝

候选官方来源：https://tianchi.aliyun.com/dataset/649 。按原始页面要求取得UserBehavior原始CSV及数据说明，确认许可和覆盖时间。当前环境此前访问官方页返回代理403；本项目没有自动绕过登录、下载限制或数据许可。

将CSV放置为 `data/raw/taobao/UserBehavior.csv`，无表头五列：

```text
user_id,item_id,category_id,behavior_type,timestamp
```

行为`pv/fav/cart/buy`，时间为Unix秒。不要把加工后的推荐序列Parquet或其他平台数据重命名后冒充原始日志。不得假定匿名商品ID可以关联当下线上商品。

先核实 `configs/taobao.yaml` 的来源版本、完整覆盖范围和时间切分，再执行：

```bash
ecommerce-research run --config configs/taobao.yaml --run-id taobao-inspect-v1 --stage inspect
ecommerce-research run --config configs/taobao.yaml --run-id taobao-inspect-v1 --stage clean
```

检查manifest、quality.json、隔离记录、缺失日期和重复报告。确认来源实际完整后再运行其余阶段。日期中存在事件只能检查明显缺日，不能证明全部日志均已取得。操作者声明与程序验证的区别保留在manifest中。

清洗将范围外记录隔离；正式研究前需要核对范围外原因，必要时修订配置并使用新run-id。缺少覆盖日期时流水线会停止，不能把缺少的购买记录当成负标签。

## 京东

已有代码实现JData行为CSV适配：必需表头 `user_id,sku_id,time,type`，可选`cate`等列。初步映射：1浏览、2加购、3删购、4下单、5关注、6点击。具体版本仍须与原始字典核对。

参考公开代码：https://github.com/falcomfanslz/JData 。数据代码公开不代表完整数据可下载，也不授权再次分发。

`configs/jd.yaml`刻意不预填覆盖和日期：取得真实文件后填写，配置验收通过后才能执行。下单`order`与淘宝`purchase`分别保存。用户和商品按平台隔离。

当前实现的是**行为日志适配和共同特征流程**；京东用户表、商品维表、评论历史快照的As-of关联尚未接入第一版。不要将第一版写成已完成京东多表研究。

## 拼多多

`configs/pdd.template.yaml`保持禁用。目前没有经核实的行为日志和事件字典。公开商品页或评论无法还原用户浏览、加购和购买路径。程序明确拒绝执行，界面显示未接入，不产生虚构指标。

## 人工演示数据

`ecommerce-research demo`按固定种子生成淘宝格式的虚构事件，并写入 `.source.json`。数据包含缺失身份、无效时间、未知动作、范围外事件、未知类目和疑似重复等质量探针。

演示运行必须使用`kind: synthetic`。来源侧边文件标记为synthetic时，程序拒绝配置为public或authorized。此机制是防误用检查，不是针对恶意改写文件的真实性认证。

所有演示指标只验证程序，不得作为真实平台实验结果。真实数据配置还需要数据持有者对来源、版本、许可与覆盖负责。
