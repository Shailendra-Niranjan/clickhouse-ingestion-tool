#🔗 ClickHouse Integration Tool
A full-stack application for integrating CSV flat file operations with ClickHouse, designed and built as part of the Zeotap Software Engineer Intern Assignment.

This tool enables users to:

Ingest CSV data into ClickHouse tables

Perform multi-table JOINs with user-defined conditions

Export ClickHouse data back to CSV

Configure ClickHouse connection details via UI

⚙️ Backend built with Spring Boot, Frontend powered by React and Tailwind CSS.

✨ Features Overview
✅ Core Functionalities
📤 CSV → ClickHouse: Upload CSV files and map them to ClickHouse tables

🔗 Multi-Table JOINs: Select multiple ClickHouse tables and define JOIN keys via UI

📥 ClickHouse → CSV: Export table data or JOIN results into downloadable CSV files

⚙️ Dynamic Config: Select table names, define column types, and insert batch records

📊 Preview & Logs: View ingestion status, export progress, and backend logs

🏗️ Tech Stack

Layer	Tech
Backend	Java 17, Spring Boot 3, ClickHouse JDBC, Apache Commons CSV, JUnit
Frontend	React 18, Tailwind CSS, Axios
Database	ClickHouse
Build Tools	Maven (Backend), npm (Frontend)
🚀 Setup Instructions
🧩 Prerequisites
Java 17+

Maven

Node.js + npm

ClickHouse running locally or on a remote server

🖥️ Backend Setup
Clone the repository

bash
Copy
Edit
git clone https://github.com/your-username/clickhouse-integration-tool.git
cd clickhouse-integration-tool/backend
Configure ClickHouse connection



bash
Copy
Edit
mvn clean install
mvn spring-boot:run
Backend will be available at: http://localhost:8080

🌐 Frontend Setup
Navigate to frontend

bash
Copy
Edit
cd ../frontend
Install dependencies

bash
Copy
Edit
npm install
Start the React app

bash
Copy
Edit
npm start
Access the frontend at: http://localhost:3000

📸 UI Highlights
Upload CSV with column preview

Select or create target table

Configure JOIN between multiple tables

Preview or download exported CSV file

Real-time success/error messages

