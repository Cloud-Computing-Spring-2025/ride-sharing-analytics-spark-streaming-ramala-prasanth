# Real-Time Ride-Sharing Data Analytics Using Spark Structured Streaming

## 📘 Overview

This project simulates real-time ride-sharing data and performs real-time ingestion, parsing, and aggregation using Apache Spark Structured Streaming. It demonstrates three key tasks in the streaming data pipeline:

1. **Ingest and store streaming data in CSV format**
2. **Process each micro-batch and store data**
3. **Perform windowed aggregation on streaming data**

---

## 📊 Dataset Description

The dataset is generated in real-time using Python and the `Faker` library. Each ride event includes:

| Field        | Type     | Description                              |
|--------------|----------|------------------------------------------|
| trip_id      | String   | Unique identifier for each trip          |
| driver_id    | Integer  | ID of the driver                         |
| distance_km  | Float    | Distance of the ride in kilometers       |
| fare_amount  | Float    | Fare charged for the trip                |
| timestamp    | String   | Event time in `YYYY-MM-DD HH:MM:SS` format |

---

## ⚙️ Components

### 1. **Data Generator (Python Socket Server)**
- **File**: `data_generator.py`
- Uses a socket to stream fake ride events in JSON format over `localhost:9999`.

### 2. **Task 1 - Ingest and Save to CSV**
- **File**: `task1_streaming_ingestion.py`
- Reads the raw JSON data from the socket, parses it, and saves it as CSV files.

### 3. **Task 2 - Micro-Batch Processing with `foreachBatch`**
- **File**: `task2_micro_batch.py`
- Parses the data and saves each batch into a separate CSV file using `foreachBatch`.

### 4. **Task 3 - Time-Based Aggregation**
- **File**: `task3_windowed_aggregation.py`
- Performs 5-minute window aggregations on `fare_amount` and stores the output as CSV.

---

## 🔌 About Sockets and Streaming

- **Socket**: A socket is an endpoint for sending/receiving data across a network. In this project, the Python script acts as a socket server, pushing data to connected clients (Spark).
- **Streaming**: Structured Streaming in Spark processes data in near real-time. It handles data as it arrives, making it ideal for use cases like fraud detection, live analytics, and monitoring.

---

## ▶️ How to Run the Project

### 🔹 Step 1: Start the Socket Server

Open a terminal and run the data generator:

```bash
python data_generator.py
```
🔹 Step 2: Run One of the Spark Tasks
Open a new terminal and run one of the Spark jobs:

Task 1: Ingest and Save CSV
```
spark-submit task1.py
```
Task 2: Micro-Batch Processing
```
spark-submit task2.py
```
Task 3: Windowed Aggregation
```
spark-submit task3_windowed_aggregation.py
```
🔹 Output
Task 1 saves files to: output/streaming_data/

Task 2 saves batches to: output/aggregated/

Task 3 saves windowed data to: output/window/

Each folder will contain .csv files generated during the streaming process.

📦 Requirements
Python 3.x

Apache Spark 3.x

PySpark

Faker

Java 8+

📁 Folder Structure
javascript
Copy
Edit
.
├── data_generator.py
├── task1.py
├── task2.py
├── task3.py
├── output/
│   ├── streaming_data/
│   ├── aggregated/
│   └── window/
