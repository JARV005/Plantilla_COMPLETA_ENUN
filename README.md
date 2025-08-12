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
