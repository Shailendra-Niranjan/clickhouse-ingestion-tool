import React, { useState } from "react";
import PreviewModal from "./PreviewModal ";


function App() {
  const [source, setSource] = useState("");
  const [csvFile, setCsvFile] = useState(null);
  const [delimiter, setDelimiter] = useState(",");
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [tableName, setTableName] = useState("");
  const [clickhouseConfig, setClickhouseConfig] = useState({
    host: "szhvetw9hs.asia-southeast1.gcp.clickhouse.cloud",
    port: "8443", // Try 8123 or 8443 depending on your ClickHouse Cloud
    database: "default",
    username: "default",
    token: "IsTIEwy.jjI4a",
  });

    // For ClickHouse → Flat File flow
    const [loadedColumns, setLoadedColumns] = useState([]);
    const [selectedTableForExport, setSelectedTableForExport] = useState("");
    const [exportColumns, setExportColumns] = useState([]);
    const [csvExport, setCsvExport] = useState("");
  
  const [tableExists, setTableExists] = useState(true);

  const [tables, setTables] = useState([]);
  const [status, setStatus] = useState("");



  // for preview modal
  const [data, setData] = useState([]);
  const [rowCount, setRowCount] = useState(0);
  const [isModalOpen, setIsModalOpen] = useState(false);

  const handlePreview = async () => {

    if (!selectedTableForExport || exportColumns.length === 0) {
      alert("Please select a table and at least one column.");
      return;
    }
    const params = new URLSearchParams();
    params.append("tableName", selectedTableForExport);
    params.append("config", JSON.stringify(clickhouseConfig));
    params.append("columnsJson", JSON.stringify(exportColumns));



    const res = await fetch(`http://localhost:8080/api/clickhouse/preview-table?${params.toString()}`, {
      method: "POST"
    });

    const json = await res.json();
    if (json.status === "success") {
      setData(json.data);
      setRowCount(json.rowCount);
      setIsModalOpen(true); // Show popup
    }
  };


  const handleDownloadCSVFromClickhouse = async () => {
    if (!selectedTableForExport || exportColumns.length === 0) {
      alert("Please select a table and at least one column.");
      return;
    }
  
    const formData = new FormData();
    formData.append("tableName", selectedTableForExport);
    formData.append("columns", JSON.stringify(exportColumns));
    formData.append("config", JSON.stringify(clickhouseConfig));
  
    try {
      const response = await fetch("http://localhost:8080/api/clickhouse/fetch-table-data", {
        method: "POST",
        body: formData,
      });
  
      if (!response.ok) {
        const err = await response.json();
        alert("❌ Failed to fetch data: " + err.error);
        return;
      }
  
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "Download_file"+`${Math.ceil(Math.random()*100)}.csv`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
  
      setStatus("✅ CSV downloaded successfully.");
    } catch (err) {
      console.error("Download error:", err);
      alert("❌ Error downloading CSV: " + err.message);
    }
  };
  

  const handleConnectClickhouse = async () => {
    try {
      const response = await fetch("http://localhost:8080/api/clickhouse/connect", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(clickhouseConfig),
      });

      const data = await response.json();

      if (response.ok) {
        setTables(data.tables || []);
        console.log("Connected to ClickHouse:", data);
        setStatus("✅ Connected to ClickHouse!");
      } else {
        setTables([]);
        setStatus("❌ Connection Failed: " + data.error);
        console.error("Connection Failed:", data.error);
      }
    } catch (err) {
      setStatus("❌ Error: " + err.message);
      console.error("Error:", err.message);
    }
  };


  const handleLoadColumnsFromClickhouse = async () => {
    if (!selectedTableForExport) {
      alert("Please select a table first.");
      return;
    }
  
    const formData = new FormData();
    formData.append("tableName", selectedTableForExport);
    formData.append("config", JSON.stringify(clickhouseConfig));
  
    try {
      const response = await fetch("http://localhost:8080/api/clickhouse/get-table-columns", {
        method: "POST",
        body: formData
      });
  
      if (!response.ok) {
        const err = await response.json();
        alert("❌ Failed to fetch columns: " + err.error);
        return;
      }
  
      const data = await response.json();
      setLoadedColumns(data.columns || []);
      setExportColumns([]); 
      setStatus("✅ Columns loaded successfully.");
    } catch (err) {
      console.error("Error loading columns:", err);
      alert("❌ Error loading columns: " + err.message);
    }
  };
  

  const checkTableExists = async (name) => {
    if (!name) return;
  
    const formData = new FormData();
    formData.append("tableName", name); // use the passed `name`, not `tableName` state
    formData.append("config", JSON.stringify(clickhouseConfig));
  
    try {
      const response = await fetch("http://localhost:8080/api/csv/check-table", {
        method: "POST",
        body: formData, // Don't add headers manually
      });
  
      const data = await response.json();
      setTableExists(data.exists); // true or false from backend
    } catch (error) {
      console.error("Failed to check table:", error);
      setTableExists(false);
    }
  };
  
  



  // Make API call to backend to get column names from the CSV
  const handleGetColumns = async () => {
    if (!csvFile) {
      console.log("Please upload a CSV file.");
      return;
    }

    const formData = new FormData();
    formData.append("file", csvFile);
    formData.append("delimiter", delimiter);

    try {
      const response = await fetch("http://localhost:8080/api/csv/columns", {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        console.log("Failed to fetch column names.");
      }

      const data = await response.json();
      setColumns(data);
      // setError("");
    } catch (err) {
      console.error(" error"+err.message);
    }
  };

  const handleCSVUpload = async () => {
    if (!csvFile) {
      alert("Please select a CSV file.");
      return;
    }

    const formData = new FormData();
    formData.append("file", csvFile);
    formData.append("delimiter", delimiter);

    try {
      const res = await fetch("http://localhost:8080/api/preview-csv", {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      setColumns(data.columns || []);
    } catch (err) {
      alert("Failed to preview CSV: " + err.message);
    }
  };

  const handleIngest = async () => {
    if (!csvFile || !tableName || selectedColumns.length === 0) {
      alert("Please upload CSV, enter table name, and select at least one column.");
      return;
    }
  
    const formData = new FormData();
    formData.append("file", csvFile);
    formData.append("delimiter", delimiter || ",");
    formData.append("tableName", tableName);
    formData.append("columns", JSON.stringify(selectedColumns));
    formData.append("config", JSON.stringify(clickhouseConfig));
  
    try {
      setStatus("🚀 Ingesting data into ClickHouse...");
      const response = await fetch("http://localhost:8080/api/csv/ingest", {
        method: "POST",
        body: formData,
      });
  
      const result = await response.json();
  
      if (response.ok) {
        setStatus(`✅ Success: ${result.inserted} rows inserted, ${result.skipped} skipped.`);
        setTableExists(true);
      } else {
        setStatus(`❌ Ingestion failed: ${result.error}`);
      }
    } catch (err) {
      setStatus(`❌ Error during ingestion: ${err.message}`);
      console.error(err);
    }
  };
  

  return (
    <div className="min-h-screen bg-gray-100 p-6">
      <div className="max-w-2xl mx-auto bg-white p-6 rounded-xl shadow-md">
        <h2 className="text-2xl font-bold mb-4">Zeotap Ingestion Tool</h2>

        <label className="block mb-2 font-medium">Select Data Source:</label>
        <select
          className="w-full border p-2 mb-4"
          value={source}
          onChange={(e) => setSource(e.target.value)}
        >
          <option value="">-- Select --</option>
          <option value="clickhouse">ClickHouse → Flat File</option>
          <option value="flatfile">Flat File → ClickHouse</option>
        </select>

        {source === "flatfile" && (
          <div>
            <h3 className="text-xl font-semibold mb-2">Upload CSV</h3>
            <input
              className="w-full border p-2 mb-2"
              type="file"
              accept=".csv"
              onChange={(e) => setCsvFile(e.target.files[0])}
            />
            <input
              className="w-full border p-2 mb-4"
              placeholder="Delimiter (default is comma ,)"
              value={delimiter}
              onChange={(e) => setDelimiter(e.target.value)}
            />
            <button
              className="w-full bg-green-600 text-white py-2 rounded-md mb-4"
              onClick={handleGetColumns}
            >
              Load Columns
            </button>

            {columns.length > 0 && (
              <div className="mb-4">
                <h4 className="font-medium mb-2">Select Columns:</h4>
                {columns.map((col) => (
                  <div key={col}>
                    <label className="inline-flex items-center space-x-2">
                      <input
                        type="checkbox"
                        value={col}
                        checked={selectedColumns.includes(col)}
                        onChange={(e) => {
                          const checked = e.target.checked;
                          setSelectedColumns((prev) =>
                            checked
                              ? [...prev, col]
                              : prev.filter((c) => c !== col)
                          );
                        }}
                      />
                      <span>{col}</span>
                    </label>
                  </div>
                ))}
              </div>
            )}
              <input
                className="w-full border p-2 mb-2"
                placeholder="Table Name to Create in ClickHouse"
                value={tableName}
                onChange={(e) => {
                  setTableName(e.target.value)
                  setTableExists(true)
                }}
              />

              <button
                className="w-full bg-yellow-500 text-white py-2 rounded-md mb-4"
                onClick={() => checkTableExists(tableName)}
              >
                Check Table Name
              </button>


            <h3 className="text-xl font-semibold mb-2">ClickHouse Config</h3>
            {["host", "port", "database", "username", "token"].map((field) => (
              <input
                key={field}
                className="w-full border p-2 mb-2"
                placeholder={field}
                value={clickhouseConfig[field]}
                onChange={(e) =>
                  setClickhouseConfig({
                    ...clickhouseConfig,
                    [field]: e.target.value,
                  })
                }
              />
            ))}

                <button
                  className={`w-full py-2 rounded-md mt-4 text-white ${
                    tableExists ? "bg-gray-400 cursor-not-allowed" : "bg-blue-600"
                  }`}
                  onClick={handleIngest}
                  disabled={tableExists}
                >
                  Start Ingestion
                </button>

                {tableExists && (
                  <p className="text-red-500 text-sm mt-2">
                    ⚠️ Verify the Table.
                  </p>
                )}


            {status && (
              <div className="mt-4 p-2 bg-gray-100 rounded border text-sm">
                {status}
              </div>
            )}
          </div>
        )}
         {source === "clickhouse" && (
          <div>
            <h3 className="text-xl font-semibold mb-2">ClickHouse Config</h3>
            {["host", "port", "database", "username", "token"].map((field) => (
              <input
                key={field}
                className="w-full border p-2 mb-2"
                placeholder={field}
                value={clickhouseConfig[field]}
                onChange={(e) =>
                  setClickhouseConfig({
                    ...clickhouseConfig,
                    [field]: e.target.value,
                  })
                }
              />
            ))}
            <button
              className="w-full bg-blue-600 text-white py-2 rounded-md mb-4"
              onClick={handleConnectClickhouse}
            >
              Connect to ClickHouse
            </button>
            {/* <button
              className="w-full bg-yellow-500 text-white py-2 rounded-md mb-4"
              // onClick={handleLoadTables}
            >
              Load Tables
            </button> */}

            {tables.length > 0 && (
              <div>
                <h4 className="font-medium mb-2">Select Table:</h4>
                <select
                  value={selectedTableForExport}
                  onChange={(e) => setSelectedTableForExport(e.target.value)}
                >
                  <option value="">-- Select Table --</option>
                  {tables.map((table) => (
                    <option key={table} value={table}>
                      {table}
                    </option>
                  ))}
                </select>
                <button
                  className="w-full bg-green-600 text-white py-2 rounded-md mb-4"
                  onClick={handleLoadColumnsFromClickhouse}
                >
                  Load Columns
                </button>
              </div>
            )}

            {loadedColumns.length > 0 && (
              <div>
                <h4 className="font-medium mb-2">Select Columns for Export:</h4>
                {loadedColumns.map((col) => (
                  <div key={col}>
                    <label className="inline-flex items-center space-x-2">
                      <input
                        type="checkbox"
                        value={col}
                        checked={exportColumns.includes(col)}
                        onChange={(e) => {
                          const checked = e.target.checked;
                          setExportColumns((prev) =>
                            checked ? [...prev, col] : prev.filter((c) => c !== col)
                          );
                        }}
                      />
                      <span>{col}</span>
                    </label>
                  </div>
                ))}
              </div>
            )}
                <button
                  className="w-full bg-indigo-600 text-white px-4 mb-4 py-2 rounded"
                  onClick={handlePreview}
                >
                  Preview
                </button>

                <PreviewModal
                  isOpen={isModalOpen}
                  onClose={() => setIsModalOpen(false)}
                  data={data}
                  rowCount={rowCount}
                />
            <button
              className="w-full bg-blue-600 text-white py-2 rounded-md mb-4"
              onClick={handleDownloadCSVFromClickhouse}
            >
              Export to CSV
            </button>

            {status && (
              <div className="mt-4 p-2 bg-gray-100 rounded border text-sm">
                {status}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
