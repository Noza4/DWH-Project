# ☁️ მონაცემთა ცენტრალიზებული საცავი — ERP & CRM სისტემებისთვის

> Cloud-Based Data Warehouse for ERP and CRM Systems

ცენტრალიზებული, ღრუბლოვანი მონაცემთა ბაზა (data warehouse), რომელიც აერთიანებს **ERP** და **CRM** სისტემების მონაცემებს ერთიან, ანალიზისთვის ოპტიმიზებულ სტრუქტურაში. პროექტი აღწერს სრულ პროცესს — ნედლი მონაცემიდან ვიზუალიზაციამდე — **ETL** პროცესების, **AWS**-ისა და **BI** ხელსაწყოების მეშვეობით.

![status](https://img.shields.io/badge/status-in_development-yellow)
![python](https://img.shields.io/badge/python-3.11+-blue)
![cloud](https://img.shields.io/badge/cloud-AWS-orange)
![license](https://img.shields.io/badge/license-MIT-green)

---

## 📋 სარჩევი

- [მიმოხილვა](#-მიმოხილვა)
- [არქიტექტურა](#-არქიტექტურა)
- [ტექნოლოგიური სტეკი](#-ტექნოლოგიური-სტეკი)
- [მონაცემთა მოდელი](#-მონაცემთა-მოდელი)
- [პროექტის სტრუქტურა](#-პროექტის-სტრუქტურა)
- [ინსტალაცია](#-ინსტალაცია)
- [გამოყენება](#-გამოყენება)
- [API](#-api)
- [გუნდი](#-გუნდი)
- [ლიცენზია](#-ლიცენზია)

---

## 🎯 მიმოხილვა

თანამედროვე ბიზნესებში ERP და CRM სისტემები ხშირად მუშაობს იზოლირებულად — მონაცემები ინახება სხვადასხვა ფორმატსა და ბაზაში, რაც ართულებს ანალიზსა და გადაწყვეტილების მიღებას.

ეს პროექტი ქმნის **ერთიან ჭეშმარიტებისთვის წყაროს (single source of truth)**, რომელიც:

- 📥 აგროვებს მონაცემებს ERP და CRM წყაროებიდან;
- 🧹 ასუფთავებს, გარდაქმნის და ვალიდაციას უკეთებს მათ რომ ერთ სტანდარტამდე დაიყვანოს;
- 🏛️ ინახავს **star schema** სტრუქტურით ღრუბლოვან საწყობში;
- 📊 ხელმისაწვდომს ხდის BI დაშბორდებისა და REST API-ის მეშვეობით;
- ⏱️ ავტომატიზირებულია **Apache Airflow**-ით ყოველდღიური გაშვებისთვის.

---

## 🏗️ არქიტექტურა

```
┌──────────────┐   ┌──────────┐   ┌─────────────┐   ┌─────────────┐   ┌──────────┐
│ წყაროები     │   │ Ingestion│   │ ETL         │   │ Warehouse   │   │ BI / API │
│ ERP + CRM    │─▶│ AWS RDS   |──▶│ Python     │──▶│ AWS RDS    │──▶│ Power BI │
│              │   │ Data Lake│   │ + Airflow   │   │ Star Schema │   │          │
└──────────────┘   └──────────┘   └─────────────┘   └─────────────┘   └──────────┘
```

მონაცემთა ნაკადი დაყოფილია ექვს ლოგიკურ ფენად, თითოეული მკაფიოდ განსაზღვრული პასუხისმგებლობით. დეტალური დიაგრამები იხილეთ [ტექნიკურ დოკუმენტაციაში](docs/technical_documentation.docx).

---

## 🛠️ ტექნოლოგიური სტეკი

| კატეგორია | ტექნოლოგია |
|---|---|
| **ენა / ETL** | Python 3.11, Pandas |
| **ბაზა** | Postgres, psycopg2 |
| **საწყობი** | AWS RDS (postgres) |
| **ღრუბელი** | AWS RDS (postgres) |
| **ორკესტრაცია** | Apache Airflow |
| **ვიზუალიზაცია** | Power BI |

---

## 🗃️ მონაცემთა მოდელი

ბაზა აგებულია **star schema**-ზე — 5 dimension და 2 fact ცხრილით.

**Dimension ცხრილები:**
- `dim_customer` — მომხმარებლები (CRM)
- `dim_product` — პროდუქცია (ERP)
- `dim_employee` — თანამშრომლები / აგენტები
- `dim_date` — კალენდარული განზომილება
- `dim_region` — გეოგრაფიული განზომილება

**Fact ცხრილები:**
- `fact_sales` — გაყიდვების ტრანზაქციები (ERP)
- `fact_interactions` — მომხმარებლის ინტერაქციები (CRM)

სრული DDL: [`sql/schema.sql`](sql/schema.sql)

---

## 📁 პროექტის სტრუქტურა

```
erp-crm-warehouse/
├── data/                  # ნედლი / სიმულირებული მონაცემები (CSV)
├── pipeline/              # ETL ლოგიკა
│   ├── extract.py         #   მონაცემთა ამოღება (ERP, CRM)
│   ├── transform.py       #   გაწმენდა, dim/fact აგება
│   ├── validate.py        #   ვალიდაცია (nulls, integrity)
│   ├── load.py            #   DWH-ში ჩატვირთვა
│   ├── init_schema.py     #   ცხრილების შექმნა
│   └── run.py             #   pipeline-ის ორკესტრაცია
├── dags/
│   └── etl_dag.py         # Airflow DAG
├── sql/
│   └── schema.sql         # DDL სკრიპტები
├── docs/
│   └── technical_documentation.docx
├── .env.example           # გარემოს ცვლადების ნიმუში
├── requirements.txt
└── README.md
```

---

## ⚙️ ინსტალაცია

### წინაპირობები
- Python 3.11+
- AWS account (Free Tier) + AWS CLI 2.x
- Apache Airflow 2.8+
- Git

### ნაბიჯები

```bash
# 1. რეპოზიტორიის კლონირება
git clone https://github.com/<team>/erp-crm-warehouse.git
cd erp-crm-warehouse

# 2. ვირტუალური გარემო
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. დამოკიდებულებები
pip install -r requirements.txt

# 4. გარემოს ცვლადები
cp .env.example .env             # შემდეგ შეავსეთ თქვენი მონაცემებით
```

`.env` ფაილის შევსება:

```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=eu-central-1
S3_BUCKET=erp-crm-datalake

REDSHIFT_HOST=cluster.xxxxx.eu-central-1.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB=warehouse
REDSHIFT_USER=admin
REDSHIFT_PASSWORD=your_password
```

---

## 🚀 გამოყენება

```bash
# ცხრილების შექმნა საწყობში
python -m pipeline.init_schema

# ETL pipeline-ის გაშვება
python -m pipeline.run --source data/ --target redshift

# REST API-ის გაშვება
uvicorn api.main:app --reload --port 8000
#   → დოკუმენტაცია: http://localhost:8000/docs
```

**Airflow-ით ავტომატიზაცია:**

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
cp dags/etl_dag.py $AIRFLOW_HOME/dags/
airflow webserver --port 8080    # ცალკე ტერმინალში
airflow scheduler                # ცალკე ტერმინალში
#   → UI: http://localhost:8080  (DAG: erp_crm_etl)
```

---

## 🔌 API

Base URL: `http://localhost:8000/api/v1`

| Method | Endpoint | აღწერა |
|---|---|---|
| `GET` | `/health` | სერვისის სტატუსი |
| `GET` | `/kpi/revenue` | ჯამური შემოსავალი (ფილტრებით) |
| `GET` | `/kpi/roi` | ROI მაჩვენებელი |
| `GET` | `/sales/by-region` | გაყიდვები რეგიონების ჭრილში |
| `GET` | `/sales/by-product` | ტოპ პროდუქტები |
| `GET` | `/customers/top` | ტოპ მომხმარებლები |
| `POST` | `/etl/trigger` | ETL-ის ხელით გაშვება |
| `GET` | `/etl/status/{run_id}` | გაშვების სტატუსი |

**მაგალითი:**

```bash
curl "http://localhost:8000/api/v1/kpi/revenue?start=2025-01-01&end=2025-03-31&region=Tbilisi"
```

```json
{
  "period": { "start": "2025-01-01", "end": "2025-03-31" },
  "region": "Tbilisi",
  "currency": "GEL",
  "total_revenue": 184320.50,
  "total_cost": 121008.00,
  "orders": 842
}
```

სრული API დოკუმენტაცია: [ტექნიკური დოკუმენტაცია, თავი 4](docs/technical_documentation.docx).

---

## 👥 გუნდი

| სახელი | როლი |
|---|---|
| ნიკოლოზ ნოზაძე | Data Engineer — ETL, მონაცემთა მოდელი |
| ცოტნე კახნიაშვილი | Cloud & BI Engineer — AWS, ვიზუალიზაცია |

---

## 📄 ლიცენზია

ეს პროექტი ვრცელდება **MIT** ლიცენზიით. იხ. [`LICENSE`](LICENSE).

---

<p align="center">
  <i>საბაკალავრო პროექტი · კომპიუტერული მეცნიერება · 2026</i>
</p>
