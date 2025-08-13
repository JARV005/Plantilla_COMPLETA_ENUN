const fs = require('fs');
const csv = require('csv-parser');
const mysql = require('mysql2');

async function uploadAllCSV(folderPath) {
  const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'tu_password',
    database: 'tu_base_de_datos'
  });

  try {
    // Conexión MySQL
    await new Promise((resolve, reject) => {
      connection.connect(err => {
        if (err) return reject(err);
        console.log('Conexión a MySQL exitosa');
        resolve();
      });
    });

    // Filtrar solo archivos CSV
    const files = fs.readdirSync(folderPath).filter(file => file.endsWith('.csv'));

    if (files.length === 0) {
      console.log('No se encontraron archivos CSV en la carpeta.');
      return;
    }

    for (const file of files) {
      const filePath = `${folderPath}/${file}`;
      const tableName = file.replace('.csv', '').replace(/\s+/g, '_'); // sin path

      const rows = [];

      // Leer CSV
      await new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
          .pipe(csv())
          .on('data', (data) => rows.push(data))
          .on('end', resolve)
          .on('error', reject);
      });

      if (rows.length === 0) {
        console.log(`El archivo "${file}" está vacío. Saltando...`);
        continue;
      }

      // Columnas dinámicas
      const columns = Object.keys(rows[0]);
      const placeholders = columns.map(() => '?').join(', ');

      // Query dinámica
      const query = `
        INSERT INTO ${tableName} (${columns.join(', ')})
        VALUES (${placeholders})
        ON DUPLICATE KEY UPDATE
        ${columns.map(col => `${col} = VALUES(${col})`).join(', ')}
      `;

      // Insertar cada fila
      for (const row of rows) {
        const values = columns.map(col => row[col]);
        await new Promise((resolve, reject) => {
          connection.execute(query, values, (err) => {
            if (err) return reject(err);
            resolve();
          });
        });
      }

      console.log(`Datos de "${file}" cargados en la tabla "${tableName}".`);
    }

  } catch (err) {
    console.error('Error procesando CSVs:', err);
  } finally {
    connection.end();
    console.log('Conexión cerrada.');
  }
}

// Ejemplo de uso
uploadAllCSV('./csv_folder');





//


-- Total pagado por cada cliente SELECT c.id AS customer_id, c.name AS customer_name, SUM(t.amount) AS total_paid FROM customers c JOIN transactions t ON c.id = t.customer_id GROUP BY c.id, c.name;

app.get('/total-pagado', (req, res) => { const { customer_id } = req.query;

const query = SELECT  c.id AS customer_id, c.name AS customer_name, SUM(t.amount) AS total_paid FROM customers c JOIN transactions t ON c.id = t.customer_id WHERE (? IS NULL OR c.id = ?) GROUP BY c.id, c.name;

connection.execute(query, [customer_id || null, customer_id || null], (err, results) => { if (err) return res.status(500).json({ error: err.message }); res.json(results); }); });

-- Facturas pendientes con información de cliente y transacción asociada

SELECT i.id AS invoice_id, i.invoice_number, i.total_amount, i.amount_paid, (i.total_amount - i.amount_paid) AS pending_amount, c.id AS customer_id, c.name AS customer_name, t.id AS transaction_id, t.transaction_date, t.amount AS transaction_amount FROM invoices i JOIN customers c ON i.customer_id = c.id LEFT JOIN transactions t ON i.id = t.invoice_id WHERE i.amount_paid < i.total_amount;

app.get('/facturas-pendientes', (req, res) => { const { customer_id } = req.query;

const query = SELECT  i.id AS invoice_id, i.invoice_number, i.total_amount, i.amount_paid, (i.total_amount - i.amount_paid) AS pending_amount, c.id AS customer_id, c.name AS customer_name, t.id AS transaction_id, t.transaction_date, t.amount AS transaction_amount FROM invoices i JOIN customers c ON i.customer_id = c.id LEFT JOIN transactions t ON i.id = t.invoice_id WHERE i.amount_paid < i.total_amount AND (? IS NULL OR c.id = ?);

connection.execute(query, [customer_id || null, customer_id || null], (err, results) => { if (err) return res.status(500).json({ error: err.message }); res.json(results); }); });

-- Listado de transacciones por plataforma

SELECT t.id AS transaction_id, t.platform, t.amount, t.transaction_date, c.id AS customer_id, c.name AS customer_name, i.id AS invoice_id, i.invoice_number FROM transactions t JOIN customers c ON t.customer_id = c.id JOIN invoices i ON t.invoice_id = i.id WHERE t.platform = 'Nequi'; -- Cambia 'Nequi' por la plataforma deseada

app.get('/transacciones-plataforma', (req, res) => { const { platform } = req.query;

const query = SELECT  t.id AS transaction_id, t.platform, t.amount, t.transaction_date, c.id AS customer_id, c.name AS customer_name, i.id AS invoice_id, i.invoice_number FROM transactions t JOIN customers c ON t.customer_id = c.id JOIN invoices i ON t.invoice_id = i.id WHERE (? IS NULL OR t.platform = ?);

connection.execute(query, [platform || null, platform || null], (err, results) => { if (err) return res.status(500).json({ error: err.message }); res.json(results); }); });

