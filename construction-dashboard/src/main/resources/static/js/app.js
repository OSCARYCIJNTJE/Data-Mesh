async function loadTables() {

    const response =
        await fetch('/api/duckdb/tables');

    const tables =
        await response.json();

    let html = "";

    tables.forEach(table => {
        html += `<li>${table}</li>`;
    });

    document.getElementById("tables").innerHTML = html;
}

async function loadSummary() {

    const response =
        await fetch('/api/duckdb/site-health-summary');

    const data =
        await response.json();

    if (data.length === 0) {
        return;
    }

    let html = "<table><tr>";

    Object.keys(data[0]).forEach(col => {
        html += `<th>${col}</th>`;
    });

    html += "</tr>";

    data.forEach(row => {

        html += "<tr>";

        Object.values(row).forEach(value => {
            html += `<td>${value}</td>`;
        });

        html += "</tr>";
    });

    html += "</table>";

    document.getElementById("summary").innerHTML = html;
}

async function runPipeline() {

    document.getElementById("logs").textContent =
        "Running pipeline...";

    const response =
        await fetch('/api/pipeline/run', {
            method: 'POST'
        });

    const text =
        await response.text();

    document.getElementById("logs").textContent =
        text;
}