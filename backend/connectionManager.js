const { Pool } = require('pg');
const mysql = require('mysql2/promise');
const { v4: uuidv4 } = require('uuid');

class ConnectionManager {
  constructor() {
    this.connections = new Map();
  }

  async addConnection(config) {
    const connectionId = uuidv4();
    const { type, name, host, port, user, password, database } = config;

    try {
      let connection;
      
      if (type === 'postgresql') {
        connection = new Pool({
          host,
          port: port || 5432,
          user,
          password,
          database,
          max: 10,
          idleTimeoutMillis: 30000,
          connectionTimeoutMillis: 2000,
        });

        // Test connection
        await connection.query('SELECT NOW()');
      } else if (type === 'mysql') {
        connection = await mysql.createPool({
          host,
          port: port || 3306,
          user,
          password,
          database,
          waitForConnections: true,
          connectionLimit: 10,
          queueLimit: 0
        });

        // Test connection
        await connection.query('SELECT 1');
      } else {
        throw new Error('Unsupported database type');
      }

      this.connections.set(connectionId, {
        id: connectionId,
        type,
        name: name || `${type}:${database}`,
        config: { host, port, user, database },
        connection,
        createdAt: new Date()
      });

      return {
        id: connectionId,
        type,
        name: name || `${type}:${database}`,
        config: { host, port, user, database }
      };
    } catch (error) {
      throw new Error(`Failed to connect to database: ${error.message}`);
    }
  }

  async removeConnection(connectionId) {
    const conn = this.connections.get(connectionId);
    if (!conn) {
      throw new Error('Connection not found');
    }

    try {
      if (conn.type === 'postgresql') {
        await conn.connection.end();
      } else if (conn.type === 'mysql') {
        await conn.connection.end();
      }
      this.connections.delete(connectionId);
      return true;
    } catch (error) {
      throw new Error(`Failed to close connection: ${error.message}`);
    }
  }

  getConnection(connectionId) {
    const conn = this.connections.get(connectionId);
    if (!conn) {
      throw new Error('Connection not found');
    }
    return conn;
  }

  getAllConnections() {
    return Array.from(this.connections.values()).map(conn => ({
      id: conn.id,
      type: conn.type,
      name: conn.name,
      config: conn.config,
      createdAt: conn.createdAt
    }));
  }

  async executeQuery(connectionId, query, params = []) {
    const conn = this.getConnection(connectionId);
    
    try {
      if (conn.type === 'postgresql') {
        const result = await conn.connection.query(query, params);
        return result.rows;
      } else if (conn.type === 'mysql') {
        const [rows] = await conn.connection.query(query, params);
        return rows;
      }
    } catch (error) {
      throw new Error(`Query failed: ${error.message}`);
    }
  }
}

module.exports = new ConnectionManager();
