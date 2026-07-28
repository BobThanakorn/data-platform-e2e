# Data Platform End-to-End: NYC Taxi Medallion

โปรเจกต์นี้สร้างแพลตฟอร์มข้อมูลแบบรันบนเครื่องตัวเอง (local-first)  
รับข้อมูลแท็กซี่ NYC Yellow Taxi จากแหล่งสาธารณะ แล้วประมวลผลจนได้ dashboard พร้อมใช้งาน  
โดยไม่ต้องพึ่งบริการ cloud ที่ต้องเสียเงิน

## สถาปัตยกรรมโดยรวม

```text
NYC TLC (ดาวน์โหลดผ่าน HTTPS)
    |
    v
Python ingestion — ตรวจ schema / จำนวนแถว / checksum
    |
    v
MinIO Bronze + ไฟล์ cache ในเครื่อง (Parquet เก็บแบบไม่แก้ทับ)
    |
    v
DuckDB + dbt
    |-- Silver: จัดชนิดข้อมูล, ตัดซ้ำ, ติดสถานะคุณภาพ, กักแถวมีปัญหา
    `-- Gold: สรุปรายวัน / ตามโซน / ตามชั่วโมง
    |
    +--> เก็บ Parquet ชั้น Silver/Gold ลง MinIO
    `--> PostgreSQL --> Apache Superset (dashboard)
```

Apache Airflow คุมตารางเวลา, การลองใหม่เมื่อพลาด, backfill และการ publish ผลลัพธ์

## ส่วนประกอบในโปรเจกต์

- **Apache Airflow** — จัดคิวและรัน pipeline
- **MinIO** — คลังข้อมูลแบบ S3 บนเครื่อง
- **DuckDB + dbt** — แปลงข้อมูลชั้น Bronze → Silver → Gold
- **PostgreSQL** — เก็บตาราง Gold และประวัติการรัน pipeline
- **Apache Superset** — ทำ dashboard / BI
- **dbt + Great Expectations** — ตรวจคุณภาพข้อมูล
- **โปรไฟล์เสริม (เปิดเมื่อต้องการ):** Prometheus/Grafana, Marquez/OpenLineage, Redpanda, Spark
- **pytest, Ruff, pre-commit, GitHub Actions** — ทดสอบและ CI
- ข้อมูลตัวอย่าง: NYC Yellow Taxi ปี 2024 (เริ่มเดือนมกราคม แล้ว backfill ทั้งปีได้)

## สิ่งที่ต้องมีก่อนเริ่ม

- Windows 11 พร้อม WSL2 และ Docker Engine / Podman Desktop / Rancher Desktop
- Docker Compose v2
- RAM อย่างน้อย 16 GB
- พื้นที่ว่างประมาณ 10 GB สำหรับ 1 เดือน หรือประมาณ 30 GB ถ้าทำทั้งปี + ส่วนเสริม
- พอร์ตว่าง: `15432`, `8080`, `8088`, `9000`, `9001`

> หมายเหตุ: Docker Desktop อาจมีเงื่อนไขลิขสิทธิ์  
> ถ้าใช้ Docker Engine บน WSL2, Podman หรือ Rancher จะเลี่ยงปัญหา subscription ของ Docker Desktop ได้

## เริ่มต้นใช้งานเร็วๆ

สร้างไฟล์ `.env` ก่อนครั้งแรก (ไฟล์นี้ไม่ถูกอัปขึ้น Git):

```powershell
py -3.11 scripts\generate_env.py
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

จากนั้นเปิดหน้าเว็บเหล่านี้:

- Airflow: <http://localhost:8080>
- MinIO Console: <http://localhost:9001>
- Superset: <http://localhost:8088>
- Dashboard NYC Taxi: <http://localhost:8088/superset/dashboard/nyc-taxi-analytics/>

ชื่อผู้ใช้และรหัสผ่านอยู่ในไฟล์ `.env` บนเครื่องคุณ

## รันข้อมูลเดือนมกราคม 2024 แบบครบวงจร

ในหน้า Airflow ให้เปิด DAG ชื่อ `nyc_taxi_medallion`  
แล้วเลือก **Trigger DAG w/ config** พร้อมค่านี้:

```json
{
  "year": 2024,
  "month": 1,
  "force_download": false
}
```

หรือสั่งจาก PowerShell:

```powershell
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_medallion `
  --conf '{\"year\": 2024, \"month\": 1, \"force_download\": false}'
```

รอบแรกจะดาวน์โหลดประมาณ 3 ล้านแถว  
รอบถัดไปถ้าไฟล์เดิม checksum เดิม ระบบจะใช้ไฟล์ในเครื่องและข้ามอัปโหลด MinIO ที่ไม่จำเป็น

## ขั้นตอนใน pipeline

1. `resolve_month` — เลือกเดือนที่ระบุ หรือย้อนหลัง 2 เดือน (ตามช่วงที่ TLC มักเผยแพร่ข้อมูล)
2. `source_is_available` — ถ้าแหล่งข้อมูลยังไม่ออก จะข้ามอย่างสุภาพ ไม่ถือว่า fail
3. `ingest` — ดาวน์โหลดไฟล์ trip (Parquet) และรายชื่อโซนแท็กซี่ (CSV)
4. ตรวจ schema, จำนวนแถว และ SHA-256 แล้วเขียนลง manifest
5. อัปโหลดไฟล์ Bronze ขึ้น MinIO ด้วยชื่อ object ที่คงที่
6. `dbt build` — รวม `fact_trips` แบบ incremental, สร้าง marts และรันทดสอบ
7. Great Expectations — ตรวจจำนวนแถว, อัตรา reject และ SLA ภายใน 24 ชั่วโมง
8. ส่งออก Silver/Gold เป็น Parquet อัดลง MinIO
9. อัปเดตตาราง Gold ใน PostgreSQL เฉพาะเดือนที่รัน (แทนที่แบบ transaction)
10. บันทึกผล freshness, การเตือนปริมาณผิดปกติ (เทียบ 7 รอบก่อน) และผลคุณภาพลง audit

## โครงสร้างคลังข้อมูล

```text
bronze/
  nyc_taxi/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet
  nyc_taxi/zones/taxi_zone_lookup.csv
  _manifests/year=2024/month=01/manifest.json
silver/
  fact_trips/year=2024/month=01/part-000.parquet
  silver_quarantine/year=2024/month=01/part-000.parquet
  dim_zone/year=2024/month=01/part-000.parquet
gold/
  mart_daily_demand/year=2024/month=01/part-000.parquet
  mart_zone_performance/year=2024/month=01/part-000.parquet
  mart_hourly_pattern/year=2024/month=01/part-000.parquet
  _manifests/year=2024/month=01/manifest.json
```

โครงสร้างเดียวกันถูก cache ไว้ที่โฟลเดอร์ `lake/` บนเครื่อง (ถูก ignore จาก Git)

## นโยบายคุณภาพข้อมูล

แต่ละทริปใน Silver จะได้สถานะ `quality_status` หนึ่งค่า เช่น:

- `accepted` — ใช้ได้
- `missing_timestamp` — ขาดเวลา
- `invalid_duration` — ระยะเวลาไม่สมเหตุสมผล
- `duration_out_of_range` — ระยะเวลาอยู่นอกช่วงที่รับได้
- `distance_out_of_range` — ระยะทางอยู่นอกช่วงที่รับได้
- `amount_out_of_range` — ยอดเงินอยู่นอกช่วงที่รับได้
- `missing_zone` — หาโซนไม่เจอ

แถวที่ใช้ไม่ได้จะถูกส่งไป `silver_quarantine` ไม่ถูกลบเงียบๆ  
`dbt build` จะล้มเหลวถ้า:

- ID ที่ต้องไม่ซ้ำกลับซ้ำ
- คีย์มิติที่จำเป็นหาย
- ความสัมพันธ์กับโซนไม่ถูกต้อง
- สัดส่วนแถวที่ reject เกิน 5%

Great Expectations ยังตรวจเพิ่มว่า Bronze/Silver ไม่ว่าง, อัตรา reject และความสดของ pipeline  
ถ้าปริมาณ Bronze เปลี่ยนเกิน 30% เมื่อเทียบกับสูงสุด 7 รอบที่สำเร็จก่อนหน้า จะบันทึกเป็นคำเตือนใน audit (ไม่บังคับ fail)

## Dashboard ใน Superset

บริการ `superset-bootstrap` จะสร้างให้อัตโนมัติ (รันซ้ำได้ ไม่สร้างของซ้ำ):

- การเชื่อมต่อ PostgreSQL ชื่อ `NYC Taxi Analytics`
- dataset ของ Gold marts ทั้ง 3 ตัว และตาราง audit
- การ์ด KPI 4 ใบ, กราฟแนวโน้มรายวัน, โซนยอดนิยม, heatmap วัน×ชั่วโมง และอัตราคุณภาพ
- dashboard ที่เผยแพร่แล้วชื่อ `NYC Taxi Analytics`

ถ้า reset Superset แล้ว รัน bootstrap อีกครั้งได้ดังนี้:

```powershell
docker compose run --rm superset-bootstrap
```

มี SQL สำเร็จรูปไว้ดูเพิ่มใน `analytics/dashboard_queries.sql`

## รันโดยไม่ใช้ Airflow

เปิดแค่โครงสร้างพื้นฐาน แล้วรันสคริปต์ท้องถิ่น:

```powershell
docker compose up -d postgres minio
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe scripts\run_local.py --year 2024 --month 1
```

สคริปต์จะอ่าน `.env` แล้วทำ ingest → `dbt build` → ส่งออก Parquet → publish เข้า PostgreSQL เหมือน pipeline หลัก

## ทดสอบและตรวจโค้ด

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check .
docker compose config --quiet
docker compose exec airflow-scheduler airflow dags list-import-errors
```

## Backfill หลายเดือน

รันทีละเดือนตามลำดับ (เพื่อไม่ให้หลายโปรเซสเขียนไฟล์ DuckDB พร้อมกัน):

```powershell
.\scripts\task.ps1 backfill -Start 2024-01 -End 2024-12
```

Bronze เก็บแบบเพิ่มอย่างเดียว, `fact_trips` ใช้คีย์ไม่ซ้ำ, และ PostgreSQL แทนที่เฉพาะเดือนที่รัน  
ดังนั้นการลองใหม่จะปลอดภัย (idempotent)

## โปรไฟล์เสริม (ไม่บังคับ)

ไม่จำเป็นต่อ pipeline หลัก  
ถ้าระบบมี RAM ราว 16 GB แนะนำเปิดทีละชุด:

```powershell
# Prometheus http://localhost:9090 และ Grafana http://localhost:3001
.\scripts\task.ps1 observability

# Marquez API http://localhost:5000 และ UI http://localhost:3002
.\scripts\task.ps1 lineage

# จำลอง streaming 10,000 ทริปผ่าน Redpanda
.\scripts\task.ps1 streaming

# ทดลองสรุปข้อมูลด้วย Spark
.\scripts\task.ps1 spark
```

ตั้งค่า `ALERT_WEBHOOK_URL` ถ้าต้องการแจ้งเตือนเมื่อ pipeline พลาด  
คำสั่ง observability / lineage จะช่วยเปิด StatsD หรือ OpenLineage ของ Airflow เมื่อ recreate scheduler

## เครื่องมือช่วยและเอกสาร

- `Makefile` — คำสั่งเทียบเท่าสำหรับ Unix/WSL
- `scripts/task.ps1` — จุดเริ่มหลักบน Windows
- `scripts/acceptance.ps1` — ตรวจโค้ด, บริการ และจำนวนแถวในคลัง
- `scripts/recovery_drill.ps1` — จำลอง MinIO ล่ม แล้วตรวจว่ารันซ้ำได้ถูกต้อง
- `.github/workflows/ci.yml` — รันเทส, lint, dbt parse และตรวจ Compose บน GitHub
- `.pre-commit-config.yaml` — ตรวจก่อน commit บนเครื่อง
- โฟลเดอร์ `docs/` — สถาปัตยกรรม, ADR, พจนานุกรมข้อมูล, runbook, ส่วนขยาย และสคริปต์ demo

## การดูแลระบบประจำวัน

ดูสถานะและ log:

```powershell
docker compose ps
docker compose logs --since=10m airflow-scheduler
docker compose logs --since=10m minio
```

หยุดบริการโดยยังเก็บข้อมูลไว้:

```powershell
docker compose down
```

ล้างข้อมูลใน container ทั้งหมด (ใช้เมื่อตั้งใจรีเซ็ตจริงๆ):

```powershell
docker compose down --volumes
Remove-Item -Recurse -Force lake, logs
```

คำสั่งรีเซ็ตจะลบประวัติ audit บนเครื่องและการตั้งค่า Superset  
ไฟล์ Bronze ดาวน์โหลดใหม่ได้ แต่ของที่ตั้งค่าไว้ในเครื่องจะหาย

## ขอบเขตเรื่องค่าใช้จ่าย

คอมโพเนนต์หลักทั้งหมดรันบนเครื่องตัวเอง ไม่มีค่าบริการ cloud ที่บังคับใช้  
แต่ยังมีต้นทุนจริง เช่น เครื่องคอมพิวเตอร์, พื้นที่เก็บข้อมูล, อินเทอร์เน็ต และไฟ  
การใช้ GitHub Actions ของ repo สาธารณะขึ้นกับโควต้าของ GitHub  
ถ้าอยากเลี่ยงค่า cloud ให้รันเทสบนเครื่องเป็นค่าเริ่มต้น
