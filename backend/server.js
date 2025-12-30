const express = require('express');
const cors = require('cors');
require('dotenv').config();
const connectionManager = require('./connectionManager');
const preferencesManager = require('./preferencesManager');
const searchService = require('./searchService');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3001;

// File upload setup
const uploadDir = path.join(__dirname, 'uploads');
fs.mkdirSync(uploadDir, { recursive: true });

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => {
    const timestamp = Date.now();
    const ext = path.extname(file.originalname) || '.xlsx';
    const base = path.basename(file.originalname, ext).replace(/[^a-zA-Z0-9-_]/g, '_');
    cb(null, `${base}_${timestamp}${ext}`);
  }
});

const excelMimeTypes = new Set([
  'application/vnd.ms-excel',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
]);

const upload = multer({
  storage,
  fileFilter: (_req, file, cb) => {
    if (excelMimeTypes.has(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error('Only Excel files are allowed (.xls, .xlsx)'));
    }
  },
  limits: {
    fileSize: 25 * 1024 * 1024 // 25 MB
  }
});

// Basic identifier quoting to keep ad-hoc stats queries safe
function quoteIdent(identifier, type) {
  if (typeof identifier !== 'string' || identifier.length === 0) {
    throw new Error('Invalid identifier');
  }
  // Disallow null bytes to avoid injection
  if (/\0/.test(identifier)) {
    throw new Error('Invalid identifier');
  }
  if (type === 'postgresql') {
    return '"' + identifier.replace(/"/g, '""') + '"';
  }
  return '`' + identifier.replace(/`/g, '``') + '`';
}

function freshnessLabelFromDate(value) {
  if (!value) return { label: 'unknown', days: null };
  const dt = new Date(value);
  if (isNaN(dt.getTime())) return { label: 'unknown', days: null };
  const diffDays = (Date.now() - dt.getTime()) / (1000 * 60 * 60 * 24);
  if (diffDays <= 1) return { label: 'fresh', days: Number(diffDays.toFixed(1)) };
  if (diffDays <= 7) return { label: 'recent', days: Number(diffDays.toFixed(1)) };
  if (diffDays <= 30) return { label: 'stale', days: Number(diffDays.toFixed(1)) };
  return { label: 'outdated', days: Number(diffDays.toFixed(1)) };
}

// Middleware
app.use(cors());
app.use(express.json());
app.use('/uploads', express.static(uploadDir));

// Initialize preferences
async function initializePreferences() {
  await preferencesManager.loadPreferences();
  console.log('✓ Preferences loaded');
}

// Initialize search service
async function initializeSearch() {
  await searchService.initialize();
  console.log('✓ Search service initialized');
}

// Note: Database connections are now manually added via the UI
// The automatic .env initialization has been disabled to allow manual selection
// If you want to enable auto-loading from .env, uncomment the code below:

/*
// Initialize database connections from environment variables
async function initializeConnections() {
  try {
    // Initialize PostgreSQL connection if configured
    if (process.env.POSTGRES_DB_HOST && process.env.POSTGRES_DB_USER && process.env.POSTGRES_DB_PASSWORD) {
      const postgresConfig = {
        type: 'postgresql',
        name: 'PostgreSQL (from .env)',
        host: process.env.POSTGRES_DB_HOST,
        port: process.env.POSTGRES_DB_PORT || 5432,
        user: process.env.POSTGRES_DB_USER,
        password: process.env.POSTGRES_DB_PASSWORD,
        database: process.env.POSTGRES_DB_NAME || process.env.POSTGRES_DB_USER
      };
      
      const pgConn = await connectionManager.addConnection(postgresConfig);
      console.log('✓ PostgreSQL connection initialized:', pgConn.name);
    }

    // Initialize MySQL connection if configured
    if (process.env.MYSQL_DB_HOST && process.env.MYSQL_DB_USER && process.env.MYSQL_DB_PASSWORD) {
      const mysqlConfig = {
        type: 'mysql',
        name: 'MySQL (from .env)',
        host: process.env.MYSQL_DB_HOST,
        port: process.env.MYSQL_DB_PORT || 3306,
        user: process.env.MYSQL_DB_USER,
        password: process.env.MYSQL_DB_PASSWORD,
        database: process.env.MYSQL_DB_NAME || process.env.MYSQL_DB_USER
      };
      
      const mysqlConn = await connectionManager.addConnection(mysqlConfig);
      console.log('✓ MySQL connection initialized:', mysqlConn.name);
    }

    if (!process.env.POSTGRES_DB_HOST && !process.env.MYSQL_DB_HOST) {
      console.log('ℹ No database connections configured in .env file');
      console.log('  You can add connections via the UI or configure them in .env');
    }
  } catch (error) {
    console.error('⚠ Error initializing connections from .env:', error.message);
    console.log('  You can still add connections manually via the UI');
  }
}
*/

// API Routes

// Connection Management
app.post('/api/connections', async (req, res) => {
  try {
    const { type, name, host, port, user, password, database } = req.body;
    
    if (!type || !host || !user || !password || !database) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const connection = await connectionManager.addConnection({
      type,
      name,
      host,
      port,
      user,
      password,
      database
    });

    // Index the new database in background
    searchService.indexDatabase(connection.id).catch(err => {
      console.error('Error indexing new database:', err.message);
    });

    res.json(connection);
  } catch (err) {
    console.error('Error adding connection:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/connections', async (req, res) => {
  try {
    const allConnections = connectionManager.getAllConnections();
    const visibleDatabases = await preferencesManager.getVisibleDatabases();
    
    // If no preferences set (empty array), show all databases
    if (visibleDatabases.length === 0) {
      return res.json(allConnections);
    }
    
    // Filter to only show visible databases
    const filteredConnections = allConnections.filter(conn => 
      visibleDatabases.includes(conn.id)
    );
    
    res.json(filteredConnections);
  } catch (err) {
    console.error('Error fetching connections:', err);
    res.status(500).json({ error: 'Failed to fetch connections' });
  }
});

app.delete('/api/connections/:id', async (req, res) => {
  try {
    await searchService.clearIndex(req.params.id);
    await connectionManager.removeConnection(req.params.id);
    res.json({ message: 'Connection removed successfully' });
  } catch (err) {
    console.error('Error removing connection:', err);
    res.status(500).json({ error: err.message });
  }
});

// Get all schemas in current database
app.get('/api/connections/:id/schemas', async (req, res) => {
  const { id } = req.params;
  
  try {
    const conn = connectionManager.getConnection(id);
    let query, params;
    
    if (conn.type === 'postgresql') {
      query = `
        SELECT schema_name as name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schema_name
      `;
    } else if (conn.type === 'mysql') {
      query = `
        SELECT SCHEMA_NAME as name
        FROM information_schema.SCHEMATA
        WHERE SCHEMA_NAME = ? AND SCHEMA_NAME NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
        ORDER BY SCHEMA_NAME
      `;
      params = [conn.config.database];
    }
    
    const result = await connectionManager.executeQuery(id, query, params);
    res.json(result);
  } catch (err) {
    console.error('Error fetching schemas:', err);
    res.status(500).json({ error: err.message });
  }
});

// Get all tables in a schema
app.get('/api/connections/:id/schemas/:schema/tables', async (req, res) => {
  const { id, schema } = req.params;
  
  try {
    const conn = connectionManager.getConnection(id);
    let query, params;
    
    if (conn.type === 'postgresql') {
      query = `
        SELECT 
          table_name as name,
          table_type as type
        FROM information_schema.tables
        WHERE table_schema = $1
        ORDER BY table_name
      `;
      params = [schema];
    } else if (conn.type === 'mysql') {
      query = `
        SELECT 
          TABLE_NAME as name,
          TABLE_TYPE as type
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = ?
        ORDER BY TABLE_NAME
      `;
      params = [schema];
    }
    
    const result = await connectionManager.executeQuery(id, query, params);
    res.json(result);
  } catch (err) {
    console.error('Error fetching tables:', err);
    res.status(500).json({ error: err.message });
  }
});

// Get columns for a table
app.get('/api/connections/:id/schemas/:schema/tables/:table/columns', async (req, res) => {
  const { id, schema, table } = req.params;
  
  try {
    const conn = connectionManager.getConnection(id);
    let query, params;
    
    if (conn.type === 'postgresql') {
      query = `
        SELECT 
          column_name as name,
          data_type as type,
          is_nullable as nullable,
          column_default as default_value
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
      `;
      params = [schema, table];
    } else if (conn.type === 'mysql') {
      query = `
        SELECT 
          COLUMN_NAME as name,
          DATA_TYPE as type,
          IS_NULLABLE as nullable,
          COLUMN_DEFAULT as default_value
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
      `;
      params = [schema, table];
    }
    
    const result = await connectionManager.executeQuery(id, query, params);
    res.json(result);
  } catch (err) {
    console.error('Error fetching columns:', err);
    res.status(500).json({ error: err.message });
  }
});

// Get data from a table with pagination
app.get('/api/connections/:id/schemas/:schema/tables/:table/data', async (req, res) => {
  const { id, schema, table } = req.params;
  const limit = parseInt(req.query.limit) || 100;
  const offset = parseInt(req.query.offset) || 0;
  
  try {
    const conn = connectionManager.getConnection(id);
    let countQuery, dataQuery, countParams, dataParams;
    
    if (conn.type === 'postgresql') {
      countQuery = `SELECT COUNT(*) as total FROM "${schema}"."${table}"`;
      dataQuery = `SELECT * FROM "${schema}"."${table}" LIMIT $1 OFFSET $2`;
      countParams = [];
      dataParams = [limit, offset];
    } else if (conn.type === 'mysql') {
      countQuery = `SELECT COUNT(*) as total FROM \`${schema}\`.\`${table}\``;
      dataQuery = `SELECT * FROM \`${schema}\`.\`${table}\` LIMIT ? OFFSET ?`;
      countParams = [];
      dataParams = [limit, offset];
    }
    
    const countResult = await connectionManager.executeQuery(id, countQuery, countParams);
    const dataResult = await connectionManager.executeQuery(id, dataQuery, dataParams);
    
    res.json({
      data: dataResult,
      total: parseInt(countResult[0].total),
      limit,
      offset
    });
  } catch (err) {
    console.error('Error fetching table data:', err);
    res.status(500).json({ error: err.message });
  }
});

// Table-level stats (row count, freshness)
app.get('/api/connections/:id/schemas/:schema/tables/:table/stats', async (req, res) => {
  const { id, schema, table } = req.params;

  try {
    const conn = connectionManager.getConnection(id);
    const safeSchema = quoteIdent(schema, conn.type);
    const safeTable = quoteIdent(table, conn.type);

    let rowCountQuery;
    let columnsQuery;
    let columnsParams;

    if (conn.type === 'postgresql') {
      rowCountQuery = `SELECT COUNT(*) as row_count FROM ${safeSchema}.${safeTable}`;
      columnsQuery = `
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
      `;
      columnsParams = [schema, table];
    } else if (conn.type === 'mysql') {
      rowCountQuery = `SELECT COUNT(*) as row_count FROM ${safeSchema}.${safeTable}`;
      columnsQuery = `
        SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      `;
      columnsParams = [schema, table];
    } else {
      return res.status(400).json({ error: 'Unsupported database type' });
    }

    const [rowCountResult, columnsResult] = await Promise.all([
      connectionManager.executeQuery(id, rowCountQuery),
      connectionManager.executeQuery(id, columnsQuery, columnsParams)
    ]);

    const timestampCandidates = ['updated_at', 'modified_at', 'last_updated', 'last_modified', 'timestamp', 'ts'];
    const allowedTypes = conn.type === 'postgresql'
      ? ['timestamp without time zone', 'timestamp with time zone', 'date']
      : ['timestamp', 'datetime', 'date'];

    let freshnessColumn = null;
    for (const col of columnsResult) {
      const colName = (col.column_name || '').toLowerCase();
      const colType = (col.data_type || '').toLowerCase();
      if (timestampCandidates.includes(colName) && allowedTypes.some(t => colType.includes(t))) {
        freshnessColumn = col.column_name;
        break;
      }
    }

    // Fallback to first timestamp-like column if named candidates not found
    if (!freshnessColumn) {
      const fallback = columnsResult.find(col => {
        const colType = (col.data_type || '').toLowerCase();
        return allowedTypes.some(t => colType.includes(t));
      });
      if (fallback) {
        freshnessColumn = fallback.column_name;
      }
    }

    let freshness = null;
    let freshnessLabel = { label: 'unknown', days: null };
    if (freshnessColumn) {
      const safeColumn = quoteIdent(freshnessColumn, conn.type);
      const freshnessQuery = conn.type === 'postgresql'
        ? `SELECT MAX(${safeColumn}) as freshness FROM ${safeSchema}.${safeTable}`
        : `SELECT MAX(${safeColumn}) as freshness FROM ${safeSchema}.${safeTable}`;
      const freshnessResult = await connectionManager.executeQuery(id, freshnessQuery);
      freshness = freshnessResult[0]?.freshness || null;
      freshnessLabel = freshnessLabelFromDate(freshness);
    }

    const rowCountValue = rowCountResult[0]?.row_count ?? rowCountResult[0]?.count ?? rowCountResult[0]?.total;
    const rowCount = rowCountValue !== undefined ? Number(rowCountValue) : null;

    res.json({
      row_count: Number.isFinite(rowCount) ? rowCount : null,
      freshness,
      freshness_column: freshnessColumn,
      freshness_label: freshnessLabel.label,
      freshness_days: freshnessLabel.days
    });
  } catch (err) {
    console.error('Error fetching table stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// Column-level stats (distinct count, min/max range)
app.get('/api/connections/:id/schemas/:schema/tables/:table/columns/:column/stats', async (req, res) => {
  const { id, schema, table, column } = req.params;

  try {
    const conn = connectionManager.getConnection(id);
    const safeSchema = quoteIdent(schema, conn.type);
    const safeTable = quoteIdent(table, conn.type);
    const safeColumn = quoteIdent(column, conn.type);

    const distinctLimit = 50;

    // Get column type to build safer expressions
    let columnType = null;
    if (conn.type === 'postgresql') {
      const typeResult = await connectionManager.executeQuery(
        id,
        `SELECT data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`,
        [schema, table, column]
      );
      columnType = (typeResult[0]?.data_type || '').toLowerCase();
    } else if (conn.type === 'mysql') {
      const typeResult = await connectionManager.executeQuery(
        id,
        `SELECT DATA_TYPE as data_type FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
        [schema, table, column]
      );
      columnType = (typeResult[0]?.data_type || '').toLowerCase();
    }

    const numericTypes = ['int', 'integer', 'smallint', 'bigint', 'decimal', 'numeric', 'real', 'double', 'double precision', 'float', 'float4', 'float8'];
    const dateTypes = ['date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone', 'datetime', 'time'];

    let columnExpr = safeColumn;
    if (numericTypes.some(t => columnType && columnType.includes(t))) {
      columnExpr = conn.type === 'postgresql' ? `${safeColumn}::numeric` : safeColumn;
    } else if (dateTypes.some(t => columnType && columnType.includes(t))) {
      columnExpr = conn.type === 'postgresql' ? `${safeColumn}::timestamptz` : safeColumn;
    } else {
      columnExpr = conn.type === 'postgresql' ? `${safeColumn}::text` : `CAST(${safeColumn} AS CHAR)`;
    }

    let statsQuery;
    let distinctValuesQuery;
    if (conn.type === 'postgresql') {
      statsQuery = `
        SELECT 
          COUNT(DISTINCT ${columnExpr}) as distinct_count,
          MIN(${columnExpr}) as min_value,
          MAX(${columnExpr}) as max_value
        FROM ${safeSchema}.${safeTable}
      `;
      distinctValuesQuery = `
        SELECT DISTINCT ${columnExpr} as value
        FROM ${safeSchema}.${safeTable}
        ORDER BY ${columnExpr}
        LIMIT ${distinctLimit}
      `;
    } else if (conn.type === 'mysql') {
      statsQuery = `
        SELECT 
          COUNT(DISTINCT ${columnExpr}) as distinct_count,
          MIN(${columnExpr}) as min_value,
          MAX(${columnExpr}) as max_value
        FROM ${safeSchema}.${safeTable}
      `;
      distinctValuesQuery = `
        SELECT DISTINCT ${columnExpr} as value
        FROM ${safeSchema}.${safeTable}
        ORDER BY ${columnExpr}
        LIMIT ${distinctLimit}
      `;
    } else {
      return res.status(400).json({ error: 'Unsupported database type' });
    }

    const [statsResult, distinctResult] = await Promise.all([
      connectionManager.executeQuery(id, statsQuery),
      connectionManager.executeQuery(id, distinctValuesQuery)
    ]);
    const stats = statsResult[0] || {};

    const distinctValue = stats.distinct_count ?? stats.distinct ?? stats.count;
    const distinctValues = (distinctResult || []).map(r => r.value).filter(v => v !== null && v !== undefined);

    res.json({
      distinct_count: distinctValue !== undefined ? Number(distinctValue) : null,
      min_value: stats.min_value ?? null,
      max_value: stats.max_value ?? null,
      distinct_values: distinctValues
    });
  } catch (err) {
    console.error('Error fetching column stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// Preferences Management
app.get('/api/preferences', async (req, res) => {
  try {
    const preferences = await preferencesManager.loadPreferences();
    res.json(preferences);
  } catch (err) {
    console.error('Error fetching preferences:', err);
    res.status(500).json({ error: 'Failed to fetch preferences' });
  }
});

app.put('/api/preferences/visible-databases', async (req, res) => {
  try {
    const { databaseIds } = req.body;
    
    if (!Array.isArray(databaseIds)) {
      return res.status(400).json({ error: 'databaseIds must be an array' });
    }
    
    const visibleDatabases = await preferencesManager.setVisibleDatabases(databaseIds);
    res.json({ visibleDatabases });
  } catch (err) {
    console.error('Error updating preferences:', err);
    res.status(500).json({ error: 'Failed to update preferences' });
  }
});

app.get('/api/connections/all', (req, res) => {
  try {
    // This endpoint returns ALL connections, ignoring preferences
    const connections = connectionManager.getAllConnections();
    res.json(connections);
  } catch (err) {
    console.error('Error fetching all connections:', err);
    res.status(500).json({ error: 'Failed to fetch connections' });
  }
});

// Search endpoints
app.get('/api/search', async (req, res) => {
  try {
    const { q, type, database_type, connection_id, schema_name, limit, offset } = req.query;
    
    const results = await searchService.search(q, {
      type,
      database_type,
      connection_id,
      schema_name,
      limit: limit ? parseInt(limit) : 100,
      offset: offset ? parseInt(offset) : 0
    });
    
    res.json(results);
  } catch (err) {
    console.error('Error searching:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/search/reindex', async (req, res) => {
  try {
    const { connectionId } = req.body;
    
    if (connectionId) {
      const count = await searchService.indexDatabase(connectionId);
      res.json({ message: 'Database reindexed', count });
    } else {
      const count = await searchService.reindexAll();
      res.json({ message: 'All databases reindexed', count });
    }
  } catch (err) {
    console.error('Error reindexing:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/search/stats', (req, res) => {
  try {
    const stats = searchService.getStats();
    res.json(stats);
  } catch (err) {
    console.error('Error getting stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// Get available schemas for filters
app.get('/api/search/schemas', async (req, res) => {
  try {
    const stats = searchService.getStats();
    let allDocs;
    
    if (stats.useElasticsearch) {
      const result = await searchService.search('', { limit: 10000 });
      allDocs = result.results;
    } else {
      allDocs = searchService.inMemoryIndex;
    }
    
    const schemas = [...new Set(allDocs.map(doc => doc.schema_name))].sort();
    res.json(schemas);
  } catch (err) {
    console.error('Error getting schemas:', err);
    res.status(500).json({ error: err.message });
  }
});

// Excel upload endpoint
app.post('/api/uploads/excel', upload.single('file'), (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const downloadUrl = `/uploads/${req.file.filename}`;
    res.json({
      message: 'File uploaded successfully',
      fileName: req.file.filename,
      originalName: req.file.originalname,
      size: req.file.size,
      downloadUrl
    });
  } catch (err) {
    console.error('Error handling upload:', err);
    res.status(500).json({ error: 'Failed to upload file' });
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', message: 'Data Catalog API is running' });
});

// Start server and initialize preferences
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await initializePreferences();
  await initializeSearch();
  console.log('ℹ Database connections must be added manually via the UI');
});
