# FIWARE Air Quality Monitoring – Local Demo

Demo smart city app with **FIWARE**: environmental monitoring using the **AirQualityObserved** data model, **Orion Context Broker**, **QuantumLeap**, **CrateDB**, and **Grafana**.

Based on the [official FIWARE training (Spanish)](https://fiware-training.readthedocs.io/es-mx/latest/casodeestudio/descripcion/).

---

## What This Demo Does

- **Orion Context Broker**: Stores current context (air quality entities).
- **QuantumLeap**: Subscribes to Orion; on every entity update it stores a time-series record in CrateDB.
- **CrateDB**: Time-series storage for history.
- **Grafana**: Visualizes the stored series (e.g. CO, temperature over time).

You create/update air quality entities via REST (or the provided Python scripts); Orion notifies QuantumLeap; you see history in CrateDB and Grafana.

---

## Prerequisites

- **Docker** and **Docker Compose**. Use `docker compose` in the commands below as appropriate for your system.
- **Python 3.9+** and `pip` (for the optional Python client scripts).

**Linux (CrateDB):** This stack runs CrateDB with `discovery.type=single-node`, which skips the `vm.max_map_count` bootstrap check, so **you do not need to set any host sysctl** for the demo. If you ever run CrateDB without single-node (e.g. multi-node), set `sudo sysctl -w vm.max_map_count=262144` and make it permanent in `/etc/sysctl.conf`.

---

## 1. Start the Stack

From the project root:

```bash
docker compose -f docker-compose-demo.yml up -d
```

Wait until all services are healthy (Orion may take ~30s). Check:

```bash
docker compose -f docker-compose-demo.yml ps
```

**Endpoints:**

| Service            | URL                          |
|--------------------|------------------------------|
| Orion Context Broker | http://localhost:1026/version |
| QuantumLeap        | http://localhost:8668/v2/version |
| CrateDB Admin UI   | http://localhost:4200        |
| Grafana            | http://localhost:3000       |
| **MongoDB (Mongo Express)** | http://localhost:8082 (user: `admin`, password: `admin`) |

---

## 2. Create Entity and Subscription (Python)

Create a virtualenv and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Create the air quality entity and the QuantumLeap subscription (uses tenant `Fiware-Service: airquality` so Orion and QuantumLeap stay in sync):

```bash
python scripts/setup_air_quality.py
```

If you already had an entity/subscription from before, re-run this so the new tenant is used; then run the update script again.

---

## 3. Simulate Sensor Updates

Each update is stored as a new time-series point by QuantumLeap:

```bash
python scripts/update_sensor_values.py
python scripts/update_sensor_values.py --co 25 --temperature 22.5
```

---

## 4. Query Current State and History

- **Current entity (Orion):**
  ```bash
  python scripts/query_entity.py
  ```
- **History (QuantumLeap):**
  ```bash
  python scripts/query_entity.py --history --last-n 20
  ```

Or with curl:

- Orion: `curl -s http://localhost:1026/v2/entities/AirQualityUnit01`
- QuantumLeap: `curl -s "http://localhost:8668/v2/entities/AirQualityUnit01?type=AirQualityObserved"`

---

## 5. MongoDB (current state / “last photo”)

- Open **http://localhost:8082** (Mongo Express is on port **8082** to avoid conflicts; use the root URL only, no `/v1` or other path).
- Log in with user `admin`, password `admin`.
- Browse the database Orion uses (e.g. the collection where entities and subscriptions are stored) to see the **current** entity state.

## 6. CrateDB (historical data)

- Open http://localhost:4200 (CrateDB Admin UI).
- This demo uses tenant **airquality** (`Fiware-Service`), so QuantumLeap creates schema **mtairquality** (not `doc`). In **Tables**, open schema **mtairquality** and look for **etairqualityobserved**.
- In **Console**, run:
  ```sql
  SHOW SCHEMAS;
  ```
  You should see `mtairquality`. Then:
  ```sql
  SELECT * FROM mtairquality.etairqualityobserved ORDER BY time_index DESC LIMIT 20;
  ```
  If you see no rows, run `python scripts/debug_quantumleap.py` and check `docker compose -f docker-compose-demo.yml logs quantumleap --tail 30` to confirm Orion is notifying QuantumLeap and that writes succeed.

---

## 7. Grafana

1. Open http://localhost:3000  
   - User: `admin`  
   - Password: `admin` (change if prompted).

2. **Add CrateDB as a datasource (no Crate plugin needed)**  
   CrateDB supports the PostgreSQL wire protocol, so use the **built-in PostgreSQL** datasource:
   - Configuration (cogwheel) → Data sources → **Add data source**.
   - Select **PostgreSQL**.
   - **Host:** `crate:5432` (from Grafana container; use the service name `crate`).
   - **Database:** `doc` (or leave default).
   - **User** and **Password:** leave empty (CrateDB in this stack has auth disabled), or try user `crate` with no password.
   - **TLS/SSL Mode:** disable.
   - Click **Save & Test**; you should see “Database Connection OK”.

3. **Dashboard**
   - Create a new dashboard → Add panel → choose a **Time series** (or Graph) panel.
   - In the query editor, select your PostgreSQL (CrateDB) datasource and switch to **Code** / SQL mode.
   - Example query (table name depends on tenant: `doc` or `mtairquality` for Fiware-Service `airquality`):
     ```sql
     SELECT time_index AS time, CO FROM doc.etairqualityobserved WHERE entity_id = 'AirQualityUnit01' ORDER BY time ASC
     ```
     If you use tenant `airquality`, try `mtairquality.etairqualityobserved` instead of `doc.etairqualityobserved`.
   - Ensure the time column is named `time` (or set the time column in the panel options).

---

## 8. Stop the Stack

```bash
docker compose -f docker-compose-demo.yml down
```

To remove volumes (clear MongoDB and CrateDB data):

```bash
docker compose -f docker-compose-demo.yml down -v
```

---

## Environment Variables (Optional)

- `ORION_BASE_URL` – Orion (default: `http://localhost:1026`)
- `QUANTUMLEAP_BASE_URL` – QuantumLeap (default: `http://localhost:8668`)
- `QUANTUMLEAP_INTERNAL_URL` – URL Orion uses to notify QuantumLeap (default: `http://quantumleap:8668`; only change if Orion runs in another network)

---

## Troubleshooting

- **`version` is obsolete**: Compose no longer requires the `version` key; it has been removed from `docker-compose-demo.yml`.
- **QuantumLeap image**: This project uses `orchestracities/quantumleap` (the [current image](https://hub.docker.com/r/orchestracities/quantumleap)) instead of the deprecated `smartsdk/quantumleap`. CrateDB is set to 4.6.7 for compatibility; Redis is included because QuantumLeap expects it.
- **QuantumLeap 500 “No more Servers available” / “Failed to establish a new connection” to Crate**: QuantumLeap talks to CrateDB at `crate:4200`. If CrateDB isn’t ready yet, you get this error. The stack now waits for CrateDB to be healthy before starting QuantumLeap. If you already had the stack running, **restart it** so the new order applies: `docker compose -f docker-compose-demo.yml down && docker compose -f docker-compose-demo.yml up -d`, then wait 1–2 minutes before calling the history API.
- **CrateDB “bootstrap checks failed” / vm.max_map_count**: This compose runs CrateDB as a single node so that check is skipped. If you see it anyway, set `sudo sysctl -w vm.max_map_count=262144` on the host that runs Docker (or in the VM/WSL2 that runs the containers).
- **Orion 1026 not responding**: Wait 30–60 s after `up -d`; Orion depends on MongoDB.
- **QuantumLeap not receiving notifications**: The subscription must use a URL reachable from the Orion container (e.g. `http://quantumleap:8668/v2/notify`). The Python setup uses this by default.
- **No data in CrateDB / QuantumLeap history 404**: The stack uses tenant `Fiware-Service: airquality`. Re-run `scripts/setup_air_quality.py` then `scripts/update_sensor_values.py` so entity, subscription, and updates use that tenant. Run `python scripts/debug_quantumleap.py` to check subscriptions and lastSuccess; check `docker compose -f docker-compose-demo.yml logs quantumleap`.
- **Grafana cannot reach CrateDB**: Use `http://crate:4200` when Grafana runs in the same Compose network; on some setups you may need `http://host.docker.internal:4200` and enable “Allow host access” for the Grafana container.

---

## Docs

- [FIWARE basics and concepts](docs/FIWARE_BASICS.md) – short intro to FIWARE and this stack.
- [Official FIWARE training (case study)](https://fiware-training.readthedocs.io/es-mx/latest/casodeestudio/descripcion/)
