# ClickHouse Integration Tool

A full-stack data integration utility to ingest, join, and export data between CSV files and ClickHouse.  
Built as part of the **Zeotap Software Engineer Intern Assignment**.

## ✨ Features

- 📤 **CSV → ClickHouse**: Upload CSV files, map columns, and insert data into ClickHouse tables
- 🔗 **Multi-Table JOINs**: Select multiple tables and define JOIN keys/conditions from the UI
- 📥 **ClickHouse → CSV**: Export table data or JOIN results into downloadable CSV files
- ⚙️ **Dynamic Configuration**: Enter ClickHouse credentials, define column types, and preview mappings
- 📊 **Live Feedback**: View ingestion success/failure messages, backend logs, and export status

## ⚙️ Tech Stack

| Layer     | Tech Used                                      |
|-----------|------------------------------------------------|
| Backend   | Java 17, Spring Boot 3, ClickHouse JDBC, Apache Commons CSV, JUnit |
| Frontend  | React 18, Tailwind CSS, Axios                  |
| Database  | ClickHouse                                     |
| Build     | Maven (Backend), npm (Frontend)                |

## 🚀 Setup Instructions

### 🧩 Prerequisites

- Java 17+
- Maven
- Node.js + npm
- ClickHouse (running locally or accessible remotely)

### 🖥️ Backend Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/Shailendra-Niranjan/clickhouse-ingestion-tool.git
   cd clickhouse-ingestion-tool/backend
   ```

2. **Build and run the backend**
   ```bash
   mvn clean install
   mvn spring-boot:run
   ```

📍 Backend runs at: http://localhost:8080

### 🌐 Frontend Setup

1. **Navigate to the frontend directory**
   ```bash
   cd ../frontend
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Start the development server**
   ```bash
   npm start
   ```

📍 Frontend runs at: http://localhost:3000

## 📸 UI Highlights

- 🔼 Upload CSV and preview columns
- 🛠️ Map or create ClickHouse tables from UI
- 🔗 Configure JOINs between any number of tables
- 📤 Export JOINed/queried data into CSV
- ✅ Get real-time success/error messages

## 📁 Project Structure

```
clickhouse-ingestion-tool/
├── backend/      → Spring Boot backend
└── frontend/     → React + Tailwind CSS frontend
```
