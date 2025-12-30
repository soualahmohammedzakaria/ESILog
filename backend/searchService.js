const { Client } = require('@elastic/elasticsearch');
const connectionManager = require('./connectionManager');

class SearchService {
  constructor() {
    this.client = null;
    this.useElasticsearch = false;
    this.inMemoryIndex = [];
    this.indexName = 'data_catalog';
  }

  async initialize() {
    try {
      // Try to connect to Elasticsearch (if running locally)
      this.client = new Client({ 
        node: process.env.ELASTICSEARCH_URL || 'http://localhost:9200',
        requestTimeout: 5000
      });
      
      await this.client.ping();
      this.useElasticsearch = true;
      console.log('✓ Connected to Elasticsearch');
      await this.createIndexIfNotExists();
    } catch (error) {
      console.log('ℹ Elasticsearch not available, using in-memory search');
      this.useElasticsearch = false;
    }
  }

  async createIndexIfNotExists() {
    if (!this.useElasticsearch) return;

    try {
      const exists = await this.client.indices.exists({ index: this.indexName });
      
      if (!exists) {
        await this.client.indices.create({
          index: this.indexName,
          body: {
            mappings: {
              properties: {
                type: { type: 'keyword' }, // 'table' or 'column'
                name: { type: 'text', analyzer: 'standard' },
                table_name: { type: 'text', analyzer: 'standard' },
                schema_name: { type: 'keyword' },
                database_name: { type: 'keyword' },
                database_type: { type: 'keyword' },
                connection_id: { type: 'keyword' },
                connection_name: { type: 'text' },
                data_type: { type: 'keyword' },
                description: { type: 'text' },
                is_nullable: { type: 'boolean' },
                table_type: { type: 'keyword' },
                full_path: { type: 'keyword' },
                owner: { type: 'keyword' }
              }
            }
          }
        });
        console.log('✓ Elasticsearch index created');
      }
    } catch (error) {
      console.error('Error creating index:', error.message);
    }
  }

  async indexDatabase(connectionId) {
    const conn = connectionManager.getConnection(connectionId);
    const documents = [];

    try {
      // Get all schemas
      let schemasQuery, schemasParams;
      if (conn.type === 'postgresql') {
        schemasQuery = `
          SELECT schema_name as name
          FROM information_schema.schemata
          WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
          ORDER BY schema_name
        `;
      } else if (conn.type === 'mysql') {
        schemasQuery = `
          SELECT SCHEMA_NAME as name
          FROM information_schema.SCHEMATA
          WHERE SCHEMA_NAME = ? AND SCHEMA_NAME NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
          ORDER BY SCHEMA_NAME
        `;
        schemasParams = [conn.config.database];
      }

      const schemas = await connectionManager.executeQuery(connectionId, schemasQuery, schemasParams);

      // For each schema, get tables and columns
      for (const schema of schemas) {
        // Get tables
        let tablesQuery, tablesParams;
        if (conn.type === 'postgresql') {
          tablesQuery = `
            SELECT table_name as name, table_type as type
            FROM information_schema.tables
            WHERE table_schema = $1
            ORDER BY table_name
          `;
          tablesParams = [schema.name];
        } else if (conn.type === 'mysql') {
          tablesQuery = `
            SELECT TABLE_NAME as name, TABLE_TYPE as type
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME
          `;
          tablesParams = [schema.name];
        }

        const tables = await connectionManager.executeQuery(connectionId, tablesQuery, tablesParams);

        for (const table of tables) {
          // Index table
          const tableDoc = {
            type: 'table',
            name: table.name,
            table_name: table.name,
            schema_name: schema.name,
            database_name: conn.config.database,
            database_type: conn.type,
            connection_id: connectionId,
            connection_name: conn.name,
            table_type: table.type,
            full_path: `${conn.config.database}.${schema.name}.${table.name}`,
            owner: conn.config.user || null
          };
          documents.push(tableDoc);

          // Get columns
          let columnsQuery, columnsParams;
          if (conn.type === 'postgresql') {
            columnsQuery = `
              SELECT 
                column_name as name,
                data_type as type,
                is_nullable as nullable
              FROM information_schema.columns
              WHERE table_schema = $1 AND table_name = $2
              ORDER BY ordinal_position
            `;
            columnsParams = [schema.name, table.name];
          } else if (conn.type === 'mysql') {
            columnsQuery = `
              SELECT 
                COLUMN_NAME as name,
                DATA_TYPE as type,
                IS_NULLABLE as nullable
              FROM information_schema.COLUMNS
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION
            `;
            columnsParams = [schema.name, table.name];
          }

          const columns = await connectionManager.executeQuery(connectionId, columnsQuery, columnsParams);

          // Index columns
          for (const column of columns) {
            const columnDoc = {
              type: 'column',
              name: column.name,
              table_name: table.name,
              schema_name: schema.name,
              database_name: conn.config.database,
              database_type: conn.type,
              connection_id: connectionId,
              connection_name: conn.name,
              data_type: column.type,
              is_nullable: column.nullable === 'YES',
              full_path: `${conn.config.database}.${schema.name}.${table.name}.${column.name}`,
              owner: conn.config.user || null
            };
            documents.push(columnDoc);
          }
        }
      }

      // Store documents
      if (this.useElasticsearch) {
        await this.bulkIndexToElasticsearch(documents);
      } else {
        this.addToInMemoryIndex(documents);
      }

      console.log(`✓ Indexed ${documents.length} items from ${conn.name}`);
      return documents.length;
    } catch (error) {
      console.error(`Error indexing database ${conn.name}:`, error.message);
      throw error;
    }
  }

  async bulkIndexToElasticsearch(documents) {
    if (!this.useElasticsearch || documents.length === 0) return;

    const body = documents.flatMap(doc => [
      { index: { _index: this.indexName, _id: doc.full_path } },
      doc
    ]);

    await this.client.bulk({ refresh: true, body });
  }

  addToInMemoryIndex(documents) {
    // Remove existing documents from this connection
    if (documents.length > 0) {
      const connectionId = documents[0].connection_id;
      this.inMemoryIndex = this.inMemoryIndex.filter(doc => doc.connection_id !== connectionId);
    }
    
    // Add new documents
    this.inMemoryIndex.push(...documents);
  }

  parseQuery(queryString) {
    // Parse query with AND, OR, NOT operators
    // Returns an object with required, optional, and excluded terms
    if (!queryString || !queryString.trim()) {
      return { required: [], optional: [], excluded: [] };
    }

    const tokens = queryString.match(/\(([^)]+)\)|"([^"]+)"|[^\s()]+/g) || [];
    const result = { required: [], optional: [], excluded: [] };
    let currentMode = 'required'; // 'required', 'optional', 'excluded'

    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i].trim();

      if (token.toUpperCase() === 'AND') {
        currentMode = 'required';
      } else if (token.toUpperCase() === 'OR') {
        currentMode = 'optional';
      } else if (token.toUpperCase() === 'NOT') {
        currentMode = 'excluded';
      } else if (token === '(') {
        // Skip parentheses for now
        continue;
      } else if (token.startsWith('(') && token.endsWith(')')) {
        // Handle grouped terms like (table OR view)
        const groupContent = token.slice(1, -1);
        const groupTerms = groupContent.match(/[^\s()]+/g) || [];
        
        let groupMode = 'optional';
        for (let j = 0; j < groupTerms.length; j++) {
          const groupToken = groupTerms[j].toUpperCase();
          if (groupToken === 'OR') {
            groupMode = 'optional';
          } else if (groupToken === 'AND') {
            groupMode = 'required';
          } else if (groupToken === 'NOT') {
            groupMode = 'excluded';
          } else {
            result[groupMode].push(groupToken.toLowerCase());
          }
        }
      } else {
        // It's a search term
        result[currentMode].push(token.toLowerCase());
      }
    }

    return result;
  }

  async search(query, filters = {}) {
    if (this.useElasticsearch) {
      return await this.searchElasticsearch(query, filters);
    } else {
      return this.searchInMemory(query, filters);
    }
  }

  async searchElasticsearch(query, filters) {
    const must = [];
    const mustNot = [];
    
    if (query && query.trim()) {
      const queryParts = this.parseQuery(query);

      // Add required terms (AND)
      for (const term of queryParts.required) {
        must.push({
          multi_match: {
            query: term,
            fields: ['name^3', 'table_name^2', 'schema_name', 'database_name', 'full_path'],
            fuzziness: 'AUTO'
          }
        });
      }

      // Add optional terms (OR) - at least one must match if no required terms
      if (queryParts.optional.length > 0) {
        if (queryParts.required.length === 0) {
          // If only optional terms, use should clause
          must.push({
            bool: {
              should: queryParts.optional.map(term => ({
                multi_match: {
                  query: term,
                  fields: ['name^3', 'table_name^2', 'schema_name', 'database_name', 'full_path'],
                  fuzziness: 'AUTO'
                }
              })),
              minimum_should_match: 1
            }
          });
        } else {
          // If there are required terms, optional terms boost score
          must.push({
            bool: {
              should: queryParts.optional.map(term => ({
                multi_match: {
                  query: term,
                  fields: ['name^3', 'table_name^2', 'schema_name', 'database_name', 'full_path'],
                  fuzziness: 'AUTO',
                  boost: 2
                }
              }))
            }
          });
        }
      }

      // Add excluded terms (NOT)
      for (const term of queryParts.excluded) {
        mustNot.push({
          multi_match: {
            query: term,
            fields: ['name', 'table_name', 'schema_name', 'database_name', 'full_path'],
            fuzziness: 'AUTO'
          }
        });
      }

      // If no query parts matched any pattern, fall back to simple search
      if (queryParts.required.length === 0 && queryParts.optional.length === 0 && queryParts.excluded.length === 0) {
        must.push({
          multi_match: {
            query: query,
            fields: ['name^3', 'table_name^2', 'schema_name', 'database_name', 'full_path'],
            fuzziness: 'AUTO'
          }
        });
      }
    }

    const filter = [];
    
    if (filters.type) {
      filter.push({ term: { type: filters.type } });
    }
    
    if (filters.database_type) {
      filter.push({ term: { database_type: filters.database_type } });
    }
    
    if (filters.connection_id) {
      filter.push({ term: { connection_id: filters.connection_id } });
    }

    if (filters.schema_name) {
      filter.push({ term: { schema_name: filters.schema_name } });
    }

    const searchBody = {
      query: {
        bool: {
          must: must.length > 0 ? must : [{ match_all: {} }],
          must_not: mustNot.length > 0 ? mustNot : [],
          filter: filter
        }
      },
      size: filters.limit || 100,
      from: filters.offset || 0
    };

    const result = await this.client.search({
      index: this.indexName,
      body: searchBody
    });

    return {
      total: result.hits.total.value,
      results: result.hits.hits.map(hit => ({ ...hit._source, score: hit._score }))
    };
  }

  searchInMemory(query, filters) {
    let results = [...this.inMemoryIndex];

    // Apply filters
    if (filters.type) {
      results = results.filter(doc => doc.type === filters.type);
    }

    if (filters.database_type) {
      results = results.filter(doc => doc.database_type === filters.database_type);
    }

    if (filters.connection_id) {
      results = results.filter(doc => doc.connection_id === filters.connection_id);
    }

    if (filters.schema_name) {
      results = results.filter(doc => doc.schema_name === filters.schema_name);
    }

    // Apply search query with AND/OR/NOT support
    const computeScore = (doc, term) => {
      const name = doc.name.toLowerCase();
      const tableName = (doc.table_name || '').toLowerCase();
      const fullPath = (doc.full_path || '').toLowerCase();
      let score = 0;
      if (name === term) score += 10;
      if (tableName === term) score += 8;
      if (name.includes(term)) score += 6;
      if (tableName.includes(term)) score += 4;
      if (fullPath.includes(term)) score += 3;
      return score;
    };

    const matchesTerm = (doc, term) => computeScore(doc, term) > 0;

    if (query && query.trim()) {
      const queryParts = this.parseQuery(query);

      results = results.filter(doc => {
        // All required terms must match
        if (queryParts.required.length > 0) {
          const allRequired = queryParts.required.every(term => matchesTerm(doc, term));
          if (!allRequired) return false;
        }

        // At least one optional term must match (if no required terms)
        if (queryParts.optional.length > 0 && queryParts.required.length === 0) {
          const anyOptional = queryParts.optional.some(term => matchesTerm(doc, term));
          if (!anyOptional) return false;
        }

        // None of the excluded terms must match
        if (queryParts.excluded.length > 0) {
          const noExcluded = queryParts.excluded.every(term => !matchesTerm(doc, term));
          if (!noExcluded) return false;
        }

        return true;
      });

      // Calculate scores for matching documents
      results = results.map(doc => {
        let score = 0;
        const allTerms = [...queryParts.required, ...queryParts.optional];
        for (const term of allTerms) {
          score += computeScore(doc, term);
        }
        return { ...doc, score };
      });

      results.sort((a, b) => b.score - a.score || a.name.localeCompare(b.name));
    } else {
      results = results.map(doc => ({ ...doc, score: 0 }));
    }

    const limit = filters.limit || 100;
    const offset = filters.offset || 0;
    
    return {
      total: results.length,
      results: results.slice(offset, offset + limit)
    };
  }

  async clearIndex(connectionId = null) {
    if (this.useElasticsearch) {
      if (connectionId) {
        await this.client.deleteByQuery({
          index: this.indexName,
          body: {
            query: {
              term: { connection_id: connectionId }
            }
          }
        });
      } else {
        await this.client.indices.delete({ index: this.indexName });
        await this.createIndexIfNotExists();
      }
    } else {
      if (connectionId) {
        this.inMemoryIndex = this.inMemoryIndex.filter(doc => doc.connection_id !== connectionId);
      } else {
        this.inMemoryIndex = [];
      }
    }
  }

  async reindexAll() {
    await this.clearIndex();
    const connections = connectionManager.getAllConnections();
    
    let totalIndexed = 0;
    for (const conn of connections) {
      try {
        const count = await this.indexDatabase(conn.id);
        totalIndexed += count;
      } catch (error) {
        console.error(`Failed to index ${conn.name}:`, error.message);
      }
    }
    
    return totalIndexed;
  }

  getStats() {
    return {
      useElasticsearch: this.useElasticsearch,
      inMemoryCount: this.inMemoryIndex.length
    };
  }
}

module.exports = new SearchService();
