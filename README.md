# ACB Calculator

加拿大股票交易的 ACB（调整成本基础）与资本利得计算器，支持 Wealthsimple 和 Questrade 双券商。

---

## 目录结构

```
src/
├── data_container/          ← 所有输入数据 + 中间产物
│   ├── questrade.csv        ← ⚠️ 需要手动放入
│   ├── combined_trades.csv  ← main.py 生成，process.py 读取
│   └── output_problematic_entries.csv  ← 同日跨券商交易警告
├── result/                  ← 最终输出
│   ├── annual_pl.csv        ← 年度汇总
│   ├── realized_trades.csv  ← 每笔卖出明细
│   └── augmented_with_acb.csv  ← 含实时 ACB/股 的完整流水
├── Wealthsimple/
│   ├── data_container/      ← HAR 文件 + WS 对账单 CSV
│   └── result/              ← WS 脚本输出
├── main.py                  ← 数据清洗与合并
└── process.py               ← ACB 计算与资本利得
```

---

## 使用流程

### 第一步：准备 Questrade 数据

⚠️ **必须手动操作**：从 Questrade 导出交易记录，将文件命名为 `questrade.csv`，放入 `src/data_container/`。

- 导出时选择**非注册账户**的交易记录（TFSA / RRSP 的交易不需要报资本利得，不要包含进去）
- 确保导出格式包含以下列：`Transaction Date`、`Action`、`Symbol`、`Quantity`、`Gross Amount`、`Commission`、`Currency`

---

### 第二步：准备 Wealthsimple 数据

Wealthsimple 的数据需要从两个来源合并，需要运行两个脚本（顺序无关）。

#### 2a. 从 HAR 文件提取详细交易时间

1. 打开 Wealthsimple 网页端，打开浏览器开发者工具（F12）→ Network 标签页
2. 在 Activity 页面运行以下 JS，自动点击「Load more」加载所有记录：
```javascript
const interval = setInterval(() => {
  const btn = [...document.querySelectorAll('button')]
    .find(b => b.textContent.trim() === 'Load more');
  if (!btn) {
    console.log('Load more 按钮没了，停止');
    clearInterval(interval);
    return;
  }
  btn.click();
}, 500);
```

3. 再运行以下 JS，自动点开每一条记录（注意：每次 WS 更新后 class name 会变，需要自己在开发者工具里找到对应的 class）：
```javascript
let buttons_all_list = document.querySelectorAll("button.你的class名");
buttons_all_list.forEach((btn, i) => {
  setTimeout(() => { btn.click(); }, i * 200);
});
```

4. 在 Network 标签页过滤 `graphql`，将所有请求导出为 `.har` 文件
5. 将 `.har` 文件放入 `src/Wealthsimple/data_container/`
6. 运行脚本：
```bash
cd src/Wealthsimple
python har_file_process.py
```
输出：`Wealthsimple/result/wealthsimple_detailed.csv`

#### 2b. 合并 Wealthsimple 对账单

1. 从 Wealthsimple 下载月度/年度对账单 CSV（非注册账户）
2. 将所有 CSV 放入 `src/Wealthsimple/data_container/ws_statements/`
3. 运行脚本：
```bash
cd src/Wealthsimple
python merge_statements.py
```
输出：`Wealthsimple/result/merged_wealthsimple.csv`

---

### 第三步：合并清洗所有数据

```bash
cd src
python main.py
```

输出：
- `data_container/combined_trades.csv` — 合并后的所有交易，供下一步使用
- `data_container/output_problematic_entries.csv` — ⚠️ 同日同股跨券商交易警告，建议手动核查

---

### 第四步：计算 ACB 与资本利得

```bash
cd src
python process.py
```

输出（位于 `result/`）：
- `annual_pl.csv`
- `realized_trades.csv` — 每笔卖出的详细计算过程
- `augmented_with_acb.csv` — 完整交易流水 + 每步实时 ACB/股（核查用）

---

## 报税：如何填写 Wealthsimple Tax

打开 `result/annual_pl.csv`，按年份填入以下字段：

| Wealthsimple Tax 字段 | 对应列 |
|---|---|
| Proceeds（卖出所得） | `proceeds_CAD` |
| Cost base（调整成本基础） | `adjusted_cost_basis_CAD` |
| Outlays and expenses（费用） | `expenses_CAD` |

> `adjusted_cost_basis_CAD` = `raw_cost_basis_CAD` − `denied_superficial_loss_CAD`
>
> Wealthsimple Tax 会自动计算：Proceeds − Cost base − Expenses = 资本利得（损失），结果应等于 `realized_pl_CAD`。

### 关于 Superficial Loss（表面亏损）

如果你在亏损卖出后 30 天内买回了相同股票，CRA 会拒绝该亏损（superficial loss）。该金额不会消失，而是自动加入替代股票的 ACB，在未来卖出时自然体现为更低的利得。本工具已自动处理这一逻辑。

---

## 注意事项

- 涉及关联人（配偶、受控公司等）的 superficial loss 需手动处理
- 股票拆股、资本返还、分拆等企业行动需手动调整 ACB
