# FIWARE Basics – Short Introduction

This document gives a minimal overview of FIWARE and the components used in this project. Useful if you are new to FIWARE.

---

## What is FIWARE?

**FIWARE** is an open-source platform for building smart solutions (smart cities, IoT, industry). It provides:

- **Standard APIs** for context information (NGSI) and data models.
- **Building blocks** (Context Broker, IoT agents, data publication, etc.) that you combine instead of building everything from scratch.

Context is represented as **entities** with **attributes** (e.g. a “sensor” entity with “temperature”, “location”). The **Orion Context Broker** stores and serves this context and can **notify** other systems when data changes.

---

## NGSI (Next Generation Service Interface)

- **NGSI** is the API model used by Orion (and other FIWARE components).
- **NGSI-LD** is the newer, linked-data variant; **NGSI v2** is what we use here (Orion v2 API).
- You create **entities** (e.g. type `AirQualityObserved`, id `AirQualityUnit01`) with **attributes** (e.g. `CO`, `temperature`, `location`).
- You **update** attributes with PATCH; you can **subscribe** so that on each change Orion sends an HTTP notification to a URL you define.

---

## Components in This Demo

| Component | Role |
|-----------|------|
| **Orion Context Broker** | Stores current context (entities/attributes). Exposes NGSI v2 REST API (create, read, update, delete entities; create subscriptions). When context changes, Orion **sends HTTP notifications** to subscribed URLs (e.g. QuantumLeap)—it does not push data from MongoDB. |
| **MongoDB** | Orion’s **internal** persistence: Orion stores here:<br>- **Entities** (current state: `AirQualityUnit01` with its attributes like CO, temperature, etc.)<br>- **Subscriptions** (e.g. the subscription that tells Orion to notify QuantumLeap when AirQualityObserved changes)<br>- **Registrations** (if you had context providers)<br><br>Nothing in this demo reads from or writes to MongoDB directly; you interact with Orion via its REST API. QuantumLeap gets data only from **Orion’s HTTP notification payloads** (when Orion sends POST to QuantumLeap), not by reading MongoDB. |
| **QuantumLeap** | Listens to **Orion** via subscriptions. When you update an entity, Orion sends an HTTP POST (notification) to QuantumLeap with the changed attributes. QuantumLeap then writes one **time-series** row per notification into CrateDB. |
| **CrateDB** | Time-series database used by QuantumLeap. Stores the history of entity changes (e.g. table `etairqualityobserved`). You can query it with SQL or via QuantumLeap API. |
| **Grafana** | Connects to CrateDB (or QuantumLeap) and draws dashboards (graphs, maps, etc.) over the time-series data. |

---

## Data Flow (This Project)

1. **You** create an entity in Orion (e.g. `AirQualityUnit01` of type `AirQualityObserved`) and a **subscription** whose `notification.http.url` points to QuantumLeap (`http://quantumleap:8668/v2/notify`).
2. **You** (or a script/sensor) **call Orion’s API** to update that entity (e.g. `PATCH /v2/entities/AirQualityUnit01/attrs` with new CO, temperature).
3. **Orion** receives the API call, stores the new state in **MongoDB**, checks subscriptions, and **immediately pushes** an HTTP POST notification to QuantumLeap with the updated attributes.
4. **QuantumLeap** receives the notification and inserts a time-series row into **CrateDB** (with a time index).
5. **Grafana** queries CrateDB (or you use QuantumLeap API) to show time-series charts.

**Key point:** When you call the API to update an entity, Orion **pushes** that data to QuantumLeap via HTTP notification. This is event-driven: each API update triggers a push, so the time-series database gets updated in real-time.

So: **Orion + MongoDB = current state** (only the latest “photo” of each entity), **QuantumLeap + CrateDB = history** (every change over time). **Grafana** visualizes that history.

---

## FIWARE Data Models

FIWARE defines **data models** (entity types and attributes) for many domains: environment, parking, street lights, etc. This demo uses **AirQualityObserved** (air quality). You can browse:

- [FIWARE Data Models](https://fiware-datamodels.readthedocs.io/)
- [Smart Data Models](https://smartdatamodels.org/) (evolved from FIWARE)

Using standard models helps interoperability and reuse of tools (e.g. QuantumLeap knows how to map `AirQualityObserved` into CrateDB).

---

## Useful Links

- [FIWARE Training (Spanish) – Case study used here](https://fiware-training.readthedocs.io/es-mx/latest/casodeestudio/descripcion/)
- [Orion Context Broker – API](https://fiware-orion.readthedocs.io/)
- [QuantumLeap](https://quantumleap.readthedocs.io/)
- [CrateDB](https://crate.io/docs/)
- [Grafana](https://grafana.com/docs/)
