<div align="center">

<img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
<img src="https://img.shields.io/badge/PySpark-RDD%20API-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
<img src="https://img.shields.io/badge/Google%20Colab-Ready-F9AB00?style=for-the-badge&logo=googlecolab&logoColor=white"/>

# 🏦 PySpark RDD Analytics — Banking Transactions Dataset

**Domain:** Banking Analytics &nbsp;|&nbsp; **Engine:** Apache Spark (RDD API) &nbsp;|&nbsp; **Language:** Python 3

</div>

---

## 📌 Overview

This notebook applies the **Apache Spark RDD API** to a simulated banking transactions dataset, covering **69 analytical exercises** across core Spark primitives — `map`, `filter`, `flatMap`, `reduceByKey`, `sortBy`, `sortByKey`, `distinct`, `join`, `mapValues`, and `takeOrdered`. The dataset captures customer-level Deposit, Withdrawal, and Transfer transactions across Indian cities, and all transformations are built using **low-level RDD operations** (no DataFrames or SQL) to build a strong foundational understanding of Spark internals.

---

## 🗂️ Repository Structure

```
pyspark-rdd-banking-analytics/
│
├── notebook/
│   └── pyspark_rdd_banking_analytics.ipynb   # Main notebook (69 RDD exercises)
├── data/
│   └── banking_rdd_dataset.csv               # Source dataset (banking transactions)
└── README.md
```

---

## 📋 Dataset Schema

The dataset is a CSV file with no column headers. Each row is a comma-separated banking transaction record.

| Field                | Index | Type   | Description                                        |
|----------------------|-------|--------|----------------------------------------------------|
| `transaction_id`     | 0     | int    | Unique transaction identifier                      |
| `customer_name`      | 1     | str    | Customer name                                      |
| `account_type`       | 2     | str    | Account type: `Savings` / `Current`                |
| `transaction_type`   | 3     | str    | Type: `Deposit` / `Withdrawal` / `Transfer`        |
| `amount`             | 4     | int    | Transaction amount in ₹ (INR)                      |
| `city`               | 5     | str    | City where the transaction occurred                |

**Sample record:**
```
1001,Ravi,Savings,Deposit,5000,Delhi
```

---

## 📊 Analysis Covered

| Exercise Range | Topics Covered                                                                        |
|----------------|---------------------------------------------------------------------------------------|
| Q1 – Q15       | `filter`, `map`, `count`, `reduceByKey`, `sortBy`, `take`, `takeOrdered`, `flatMap`   |
| Q16 – Q30      | Top-N cities, averages, currency conversion (₹→USD), multi-condition filters          |
| Q31 – Q50      | `flatMap`, `distinct`, `join`, suspicious detection, composite keys, 10% uplift       |
| Q51 – Q69      | Deposit-only customers, high-frequency cities, city stats, withdrawal ranking, tags   |

**Selected exercises include:**
- Net balance per customer (Deposit = +, Withdrawal / Transfer = −)
- Average transaction amount per account type and per city
- Currency conversion: ₹ → USD at a fixed exchange rate
- Difference between total Deposit and Withdrawal per city using `join`
- Customers who made both Deposit and Withdrawal transactions (set logic)
- Customers who made only Deposit transactions
- High-frequency cities with more than 5 transactions
- Top contributor (customer) per city by single transaction amount
- Tag transactions as `"high"` (> ₹8,000) or `"low"`
- Composite key aggregations: `(account_type, transaction_type)`, `(city, account_type)`
- Total and average transaction amount per city in a single chained transform
- Customers who performed both Transfer and Withdrawal transactions
- Transactions where amount ends with `000`

---

## 🚀 Getting Started

### Prerequisites

- Python 3.x
- PySpark (`pip install pyspark`)
- Google Colab (recommended) or a local Spark environment

### Installation

```bash
pip install pyspark
```

### Running in Google Colab

```python
!pip install pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Banking_RDD_Analytics") \
    .getOrCreate()

sc = spark.sparkContext
```

Upload `banking_rdd_dataset.csv` to `/content/sample_data/` in your Colab session, then run all cells sequentially.

---

## 💡 Key Concepts Demonstrated

**SparkSession & SparkContext initialisation**
```python
spark = SparkSession.builder.master("local").appName("Banking_RDD_Analytics").getOrCreate()
sc = spark.sparkContext
```

**Loading and parsing a CSV into an RDD**
```python
data_rdd = sc.textFile("/content/sample_data/banking_rdd_dataset.csv") \
              .map(lambda row: row.split(","))
```

**Net balance per customer (signed aggregation)**
```python
net_balance = data_rdd \
    .map(lambda cols: (
        cols[1],
        int(cols[4]) if cols[3] == 'Deposit'
        else -int(cols[4]) if cols[3] in ['Withdrawal', 'Transfer']
        else 0
    )) \
    .reduceByKey(lambda x, y: x + y)
```

**Deposit vs Withdrawal difference per city using `join`**
```python
deposit_withdrawal_diff = total_deposit.join(total_withdrawal) \
    .mapValues(lambda x: x[0] - x[1])
```

**Average deposit per city using `mapValues`**
```python
average_deposit_per_city = data_rdd \
    .filter(lambda cols: cols[3] == 'Deposit') \
    .map(lambda cols: (cols[5], (int(cols[4]), 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda x: round(x[0] / x[1], 2))
```

**Set-based customer classification**
```python
both_deposit_withdrawal = data_rdd \
    .map(lambda cols: (cols[1], {cols[3]})) \
    .reduceByKey(lambda a, b: a.union(b)) \
    .filter(lambda x: 'Deposit' in x[1] and 'Withdrawal' in x[1])
```

**City-level total and average in one pass**
```python
city_stats = data_rdd \
    .map(lambda cols: (cols[5], (int(cols[4]), 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda x: (x[0], x[1][0], round(x[1][0] / x[1][1], 2)))
```

---

## 🛠️ Technologies Used

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-RDD%20API-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Google Colab](https://img.shields.io/badge/Google%20Colab-F9AB00?style=flat-square&logo=googlecolab&logoColor=white)

---

<div align="center">

*Built by **Radhika Deshpande** · PySpark RDD exercises on a simulated banking transactions dataset*

</div>
