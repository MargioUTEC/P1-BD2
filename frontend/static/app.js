const apiUrl = "http://127.0.0.1:8000/api/query";

const sqlInput = document.getElementById("sqlInput");
const runBtn = document.getElementById("runBtn");
const clearBtn = document.getElementById("clearBtn");
const logOutput = document.getElementById("logOutput");
const resultsTable = document.getElementById("resultsTable");
const tableHead = resultsTable.querySelector("thead");
const tableBody = resultsTable.querySelector("tbody");
const searchBox = document.getElementById("searchBox");
const rowCount = document.getElementById("rowCount");
const emptyState = document.getElementById("emptyState");

runBtn.addEventListener("click", async () => {
  const sql = sqlInput.value.trim().replace(/;+$/, "");
  if (!sql) return;

  logOutput.textContent = "Ejecutando consulta...";
  tableHead.innerHTML = "";
  tableBody.innerHTML = "";
  emptyState.style.display = "none";

  try {
    const res = await fetch(apiUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql }),
    });

    const data = await res.json();

    logOutput.textContent = data.output || "";

    if (Array.isArray(data.results) && data.results.length > 0) {
      renderTable(data.results);
    } else {
      emptyState.style.display = "block";
      rowCount.textContent = "";
    }
  } catch (e) {
    logOutput.textContent = "Error al ejecutar la consulta:\n" + e;
  }
});

clearBtn.addEventListener("click", () => {
  sqlInput.value = "";
  logOutput.textContent = "";
  tableHead.innerHTML = "";
  tableBody.innerHTML = "";
  rowCount.textContent = "";
  emptyState.style.display = "block";
});

searchBox.addEventListener("input", () => {
  const query = searchBox.value.toLowerCase();
  let visible = 0;
  for (const row of tableBody.querySelectorAll("tr")) {
    const text = row.textContent.toLowerCase();
    const match = text.includes(query);
    row.style.display = match ? "" : "none";
    if (match) visible++;
  }
  rowCount.textContent = `${visible} filas`;
});

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    document.querySelectorAll(".tab-content").forEach(c => c.classList.remove("active"));
    tab.classList.add("active");
    document.getElementById(tab.dataset.tab + "View").classList.add("active");
  });
});

function renderTable(rows) {
  if (!rows || rows.length === 0) return;

  const keys = Object.keys(rows[0]);
  tableHead.innerHTML = "";
  tableBody.innerHTML = "";

  const headerRow = document.createElement("tr");
  for (const key of keys) {
    const th = document.createElement("th");
    th.textContent = key;
    headerRow.appendChild(th);
  }
  tableHead.appendChild(headerRow);

  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const key of keys) {
      const td = document.createElement("td");
      td.textContent = row[key];
      tr.appendChild(td);
    }
    tableBody.appendChild(tr);
  }

  rowCount.textContent = `${rows.length} filas`;
}
