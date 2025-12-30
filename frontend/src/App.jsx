import { useState, useEffect, useCallback, useRef } from 'react'
import { BrowserRouter as Router, Routes, Route, NavLink, Navigate } from 'react-router-dom'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import * as d3 from 'd3'
import { SiPostgresql, SiMysql } from 'react-icons/si'
import { MdAdd, MdClose, MdDelete, MdSettings, MdSearch, MdFilterList, MdRefresh, MdLogout, MdBookmark } from 'react-icons/md'
import { FaTable, FaColumns } from 'react-icons/fa'
import esiLogo from './assets/esi_log.png'

const API_BASE = '/api'

function App() {
  const [sessionToken, setSessionToken] = useState(() => localStorage.getItem('sessionToken') || '')
  // Connection management state
  const [connections, setConnections] = useState([])
  const [allConnections, setAllConnections] = useState([])
  const [showAddConnection, setShowAddConnection] = useState(false)
  const [showDatabaseSelector, setShowDatabaseSelector] = useState(false)
  const [visibleDatabases, setVisibleDatabases] = useState([])
  const [connectionForm, setConnectionForm] = useState({
    type: 'postgresql',
    name: '',
    host: 'localhost',
    port: '',
    user: '',
    password: '',
    database: ''
  })

  // Search state
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [totalResults, setTotalResults] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  
  // Advanced filters
  const [showAdvancedFilters, setShowAdvancedFilters] = useState(false)
  const [filters, setFilters] = useState({
    type: '', // 'table' or 'column'
    database_type: '', // 'postgresql' or 'mysql'
    connection_id: '',
    schema_name: ''
  })
  const [availableSchemas, setAvailableSchemas] = useState([])
  
  // Selected item for details
  const [selectedItem, setSelectedItem] = useState(null)
  const [itemDetails, setItemDetails] = useState(null)

  const hasActiveFilters = Object.values(filters).some((value) => value !== '')
  const isAuthenticated = Boolean(sessionToken)

  // Fetch connections on mount
  useEffect(() => {
    if (isAuthenticated) {
      fetchConnections()
      fetchAllConnections()
      fetchPreferences()
      fetchAvailableSchemas()
    }
  }, [isAuthenticated])

  const fetchConnections = async () => {
    try {
      const response = await axios.get(`${API_BASE}/connections`)
      setConnections(response.data)
    } catch (err) {
      console.error('Failed to fetch connections:', err)
    }
  }

  const fetchAllConnections = async () => {
    try {
      const response = await axios.get(`${API_BASE}/connections/all`)
      setAllConnections(response.data)
    } catch (err) {
      console.error('Failed to fetch all connections:', err)
    }
  }

  const fetchPreferences = async () => {
    try {
      const response = await axios.get(`${API_BASE}/preferences`)
      setVisibleDatabases(response.data.visibleDatabases || [])
    } catch (err) {
      console.error('Failed to fetch preferences:', err)
    }
  }

  const fetchAvailableSchemas = async () => {
    try {
      const response = await axios.get(`${API_BASE}/search/schemas`)
      setAvailableSchemas(response.data)
    } catch (err) {
      console.error('Failed to fetch schemas:', err)
    }
  }

  const handleAddConnection = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError(null)

    try {
      const response = await axios.post(`${API_BASE}/connections`, connectionForm)
      await fetchConnections()
      await fetchAllConnections()
      setShowAddConnection(false)
      setConnectionForm({
        type: 'postgresql',
        name: '',
        host: 'localhost',
        port: '',
        user: '',
        password: '',
        database: ''
      })
      setError(null)
      alert('Connection added successfully! Data is being indexed in the background.')
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to add connection')
    } finally {
      setLoading(false)
    }
  }

  const handleRemoveConnection = async (connectionId) => {
    if (!confirm('Are you sure you want to remove this connection?')) return

    try {
      await axios.delete(`${API_BASE}/connections/${connectionId}`)
      await fetchConnections()
      await fetchAllConnections()
    } catch (err) {
      setError('Failed to remove connection')
    }
  }

  const handleToggleDatabaseVisibility = async (databaseId) => {
    try {
      let newVisibleDatabases
      
      if (visibleDatabases.length === 0) {
        newVisibleDatabases = allConnections
          .filter(c => c.id !== databaseId)
          .map(c => c.id)
      } else if (visibleDatabases.includes(databaseId)) {
        newVisibleDatabases = visibleDatabases.filter(id => id !== databaseId)
      } else {
        newVisibleDatabases = [...visibleDatabases, databaseId]
      }
      
      await axios.put(`${API_BASE}/preferences/visible-databases`, {
        databaseIds: newVisibleDatabases
      })
      
      setVisibleDatabases(newVisibleDatabases)
      await fetchConnections()
    } catch (err) {
      setError('Failed to update database visibility')
      console.error(err)
    }
  }

  const isDatabaseVisible = (databaseId) => {
    if (visibleDatabases.length === 0) {
      return true
    }
    return visibleDatabases.includes(databaseId)
  }

  const handleReindexAll = async () => {
    if (!confirm('Reindex all databases? This may take a few moments.')) return
    
    setLoading(true)
    try {
      const response = await axios.post(`${API_BASE}/search/reindex`)
      alert(`Successfully indexed ${response.data.count} items`)
      await fetchAvailableSchemas()
      handleSearch()
    } catch (err) {
      setError('Failed to reindex')
    } finally {
      setLoading(false)
    }
  }

  const handleSearch = useCallback(async () => {
    setLoading(true)
    setError(null)
    
    try {
      const noQueryNoFilters = !searchQuery.trim() && !hasActiveFilters
      const params = {
        q: searchQuery,
        ...Object.fromEntries(
          Object.entries(filters).filter(([_, value]) => value !== '')
        )
      }

      if (noQueryNoFilters) {
        params.limit = 0 // only fetch total, no rows
        params.offset = 0
      }
      
      const response = await axios.get(`${API_BASE}/search`, { params })
      setSearchResults(noQueryNoFilters ? [] : response.data.results)
      setTotalResults(response.data.total)
    } catch (err) {
      setError('Search failed')
      console.error(err)
    } finally {
      setLoading(false)
    }
  }, [searchQuery, filters])

  useEffect(() => {
    const debounce = setTimeout(() => {
      handleSearch()
    }, 300)
    
    return () => clearTimeout(debounce)
  }, [searchQuery, filters, handleSearch])

  const computeScore = (doc, query) => {
    if (!query) return 0
    const q = query.toLowerCase()
    const name = (doc.name || '').toLowerCase()
    const tableName = (doc.table_name || '').toLowerCase()
    const fullPath = (doc.full_path || '').toLowerCase()
    let score = 0
    if (name === q) score += 10
    if (tableName === q) score += 8
    if (name.includes(q)) score += 6
    if (tableName.includes(q)) score += 4
    if (fullPath.includes(q)) score += 3
    return score
  }

  const fetchItemDetails = async (item) => {
    const enriched = {
      ...item,
      score: typeof item.score === 'number' ? item.score : computeScore(item, searchQuery)
    }

    setSelectedItem(enriched)
    setItemDetails(null)
    setLoading(true)
    
    try {
      if (enriched.type === 'table') {
        const [columnsRes, statsRes] = await Promise.allSettled([
          axios.get(
            `${API_BASE}/connections/${enriched.connection_id}/schemas/${enriched.schema_name}/tables/${enriched.table_name}/columns`
          ),
          axios.get(
            `${API_BASE}/connections/${enriched.connection_id}/schemas/${enriched.schema_name}/tables/${enriched.table_name}/stats`
          )
        ])

        const columns = columnsRes.status === 'fulfilled' ? columnsRes.value.data : []
        const tableStats = statsRes.status === 'fulfilled' ? statsRes.value.data : null
        setItemDetails({ columns, tableStats })
      } else {
        const [columnStatsRes, tableStatsRes] = await Promise.allSettled([
          axios.get(
            `${API_BASE}/connections/${enriched.connection_id}/schemas/${enriched.schema_name}/tables/${enriched.table_name}/columns/${enriched.name}/stats`
          ),
          axios.get(
            `${API_BASE}/connections/${enriched.connection_id}/schemas/${enriched.schema_name}/tables/${enriched.table_name}/stats`
          )
        ])
        const columnStats = columnStatsRes.status === 'fulfilled' ? columnStatsRes.value.data : null
        const tableStats = tableStatsRes.status === 'fulfilled' ? tableStatsRes.value.data : null
        setItemDetails({ columnStats, tableStats })
      }
    } catch (err) {
      console.error('Error fetching details:', err)
      setItemDetails(null)
    } finally {
      setLoading(false)
    }
  }

  const DatabaseIcon = ({ type, size = 20 }) => {
    return type === 'postgresql' ? 
      <SiPostgresql size={size} color="#336791" /> : 
      <SiMysql size={size} color="#4479A1" />
  }

  const formatNumber = (value) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return 'Not available'
    return Number(value).toLocaleString()
  }

  const formatDate = (value) => {
    if (!value) return 'Not available'
    const parsed = new Date(value)
    return isNaN(parsed.getTime()) ? String(value) : parsed.toLocaleString()
  }

  const formatFreshness = (stats) => {
    if (!stats) return 'Not available'
    if (stats.freshness_label === 'unknown') return 'Unknown'
    const days = stats.freshness_days != null ? `${stats.freshness_days}d` : ''
    const when = formatDate(stats.freshness)
    return `${stats.freshness_label}${days ? ` (${days})` : ''} • ${when}`
  }

  const formatValue = (value) => {
    if (value === null || value === undefined) return 'Not available'
    return String(value)
  }

  const noQueryNoFilters = !searchQuery.trim() && !hasActiveFilters

  return (
    <Router>
      <div className="app">
        {isAuthenticated && (
          <header className="navbar">
            <div className="navbar-left">
              <img src={esiLogo} alt="ESI logo" className="navbar-logo" />
            </div>
            <div className="navbar-right nav-links">
              <NavLink to="/search" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
                Search
              </NavLink>
              <NavLink to="/sources" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
                Sources
              </NavLink>
              <NavLink to="/catalog" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
                Catalog
              </NavLink>
              <NavLink to="/graphing" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
                Graphing
              </NavLink>
              <NavLink to="/governance" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
                Governance
              </NavLink>
              <NavLink to="/favorites" className={({ isActive }) => `nav-link-icon ${isActive ? 'active' : ''}`} title="Favorites" style={{ marginBottom: "5px"}}>
                <MdBookmark size={22} />
              </NavLink>
              <button
                className="btn-ghost"
                onClick={() => { setSessionToken(''); localStorage.removeItem('sessionToken') }}
                title="Logout"
              >
                <MdLogout size={20} />
              </button>
            </div>
          </header>
        )}

        {error && <div className="error">{error}</div>}

        <Routes>
          <Route path="/login" element={<LoginPage onLogin={(token) => { setSessionToken(token); localStorage.setItem('sessionToken', token) }} />} />
          <Route
            path="/sources"
            element={isAuthenticated ? (
              <DataSourcesPage
                connections={connections}
                allConnections={allConnections}
                showAddConnection={showAddConnection}
                setShowAddConnection={setShowAddConnection}
                showDatabaseSelector={showDatabaseSelector}
                setShowDatabaseSelector={setShowDatabaseSelector}
                connectionForm={connectionForm}
                setConnectionForm={setConnectionForm}
                handleAddConnection={handleAddConnection}
                handleRemoveConnection={handleRemoveConnection}
                handleToggleDatabaseVisibility={handleToggleDatabaseVisibility}
                isDatabaseVisible={isDatabaseVisible}
                visibleDatabases={visibleDatabases}
                handleReindexAll={handleReindexAll}
                loading={loading}
                DatabaseIcon={DatabaseIcon}
              />
            ) : <Navigate to="/login" replace />}
          />
          <Route
            path="/search"
            element={isAuthenticated ? (
              <SearchPage
                connections={connections}
                availableSchemas={availableSchemas}
                searchQuery={searchQuery}
                setSearchQuery={setSearchQuery}
                filters={filters}
                setFilters={setFilters}
                showAdvancedFilters={showAdvancedFilters}
                setShowAdvancedFilters={setShowAdvancedFilters}
                loading={loading}
                totalResults={totalResults}
                searchResults={searchResults}
                noQueryNoFilters={noQueryNoFilters}
                fetchItemDetails={fetchItemDetails}
                selectedItem={selectedItem}
                setSelectedItem={setSelectedItem}
                itemDetails={itemDetails}
                DatabaseIcon={DatabaseIcon}
                formatNumber={formatNumber}
                formatValue={formatValue}
                formatFreshness={formatFreshness}
              />
            ) : <Navigate to="/login" replace />}
          />
          <Route
            path="/catalog"
            element={isAuthenticated ? (
              <CatalogPage
                connections={connections}
                DatabaseIcon={DatabaseIcon}
              />
            ) : <Navigate to="/login" replace />}
          />
          <Route
            path="/graphing"
            element={isAuthenticated ? (
              <GraphingPage
                connections={connections}
                DatabaseIcon={DatabaseIcon}
              />
            ) : <Navigate to="/login" replace />}
          />
          <Route
            path="/governance"
            element={isAuthenticated ? (
              <GovernancePage
                connections={connections}
                DatabaseIcon={DatabaseIcon}
              />
            ) : <Navigate to="/login" replace />}
          />
          <Route
            path="/favorites"
            element={isAuthenticated ? (
              <FavoritesPage />
            ) : <Navigate to="/login" replace />}
          />
          <Route path="*" element={<Navigate to={isAuthenticated ? '/search' : '/login'} replace />} />
        </Routes>
      </div>
    </Router>
  )
}

export default App

function LoginPage({ onLogin }) {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const navigate = useNavigate()

  const handleSubmit = (e) => {
    e.preventDefault()
    if (!email || !password) {
      setError('Please enter email and password')
      return
    }
    const token = btoa(`${email}:${Date.now()}`)
    onLogin(token)
    navigate('/search', { replace: true })
  }

  return (
    <div className="login-page">
      <div className="login-card">
        <img src={esiLogo} alt="ESI logo" className="login-logo" />
        <h2>Sign In</h2>
        <p className="login-sub">Enter your school credentials to access the ESI data catalog</p>
        <form className="login-form" onSubmit={handleSubmit}>
          <label>Email</label>
          <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} placeholder="qr_etudiant@esi.dz" />
          <label>Password</label>
          <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="••••••••••" />
          {error && <div className="login-error">{error}</div>}
          <button type="submit" className="btn-submit" style={{ marginTop: '10px' }}>Login</button>
        </form>
      </div>
    </div>
  )
}

function DataSourcesPage({
  connections,
  allConnections,
  showAddConnection,
  setShowAddConnection,
  showDatabaseSelector,
  setShowDatabaseSelector,
  connectionForm,
  setConnectionForm,
  handleAddConnection,
  handleRemoveConnection,
  handleToggleDatabaseVisibility,
  isDatabaseVisible,
  visibleDatabases,
  handleReindexAll,
  loading,
  DatabaseIcon
}) {
  return (
    <div className="connection-manager">
      <div className="connection-header">
        <h2 className="connection-title">Data Sources</h2>
        <div>
          <button className="btn-add" onClick={handleReindexAll} style={{ marginRight: '10px' }} title="Reindex all databases">
            <MdRefresh style={{ verticalAlign: 'middle', marginRight: '5px' }} />Reindex
          </button>
          <button className="btn-add" onClick={() => setShowDatabaseSelector(!showDatabaseSelector)} style={{ marginRight: '10px' }}>
            {showDatabaseSelector ? <><MdClose style={{ verticalAlign: 'middle', marginRight: '5px' }} />Close</> : <><MdSettings style={{ verticalAlign: 'middle', marginRight: '5px' }} />Select Databases</>}
          </button>
          <button className="btn-add" onClick={() => setShowAddConnection(!showAddConnection)}>
            {showAddConnection ? <><MdClose style={{ verticalAlign: 'middle', marginRight: '5px' }} />Cancel</> : <><MdAdd style={{ verticalAlign: 'middle', marginRight: '5px' }} />Add Connection</>}
          </button>
        </div>
      </div>

      {showDatabaseSelector && (
        <div className="connection-form" style={{ marginBottom: '20px' }}>
          <h4 style={{ marginBottom: '15px' }}>Select which databases to show:</h4>
          {allConnections.length === 0 ? (
            <p style={{ color: '#6c757d', padding: '10px' }}>No databases configured yet.</p>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
              {allConnections.map((conn) => (
                <label key={conn.id} style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', padding: '10px', borderRadius: '5px', background: '#f8f9fa' }}>
                  <input
                    type="checkbox"
                    checked={isDatabaseVisible(conn.id)}
                    onChange={() => handleToggleDatabaseVisibility(conn.id)}
                    style={{ marginRight: '10px', cursor: 'pointer' }}
                  />
                  <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                    <DatabaseIcon type={conn.type} />
                    <div>
                      <div style={{ fontWeight: '500' }}>
                        {conn.name}
                      </div>
                      <div style={{ fontSize: '0.85em', color: '#6c757d' }}>
                        {conn.config.user}@{conn.config.host}/{conn.config.database}
                      </div>
                    </div>
                  </div>
                </label>
              ))}
            </div>
          )}
          <p style={{ marginTop: '15px', fontSize: '0.9em', color: '#6c757d' }}>
            {visibleDatabases.length === 0 
              ? 'All databases are currently visible'
              : `${visibleDatabases.length} database(s) selected`
            }
          </p>
        </div>
      )}

      {showAddConnection && (
        <div className="connection-form">
          <form onSubmit={handleAddConnection}>
            <div className="form-grid">
              <div className="form-group">
                <label>Database Type</label>
                <select
                  value={connectionForm.type}
                  onChange={(e) => setConnectionForm({ ...connectionForm, type: e.target.value, port: e.target.value === 'postgresql' ? '5432' : '3306' })}
                  required
                >
                  <option value="postgresql">PostgreSQL</option>
                  <option value="mysql">MySQL</option>
                </select>
              </div>

              <div className="form-group">
                <label>Connection Name (Optional)</label>
                <input
                  type="text"
                  value={connectionForm.name}
                  onChange={(e) => setConnectionForm({ ...connectionForm, name: e.target.value })}
                  placeholder="My Database"
                />
              </div>

              <div className="form-group">
                <label>Host</label>
                <input
                  type="text"
                  value={connectionForm.host}
                  onChange={(e) => setConnectionForm({ ...connectionForm, host: e.target.value })}
                  required
                  placeholder="localhost"
                />
              </div>

              <div className="form-group">
                <label>Port</label>
                <input
                  type="text"
                  value={connectionForm.port}
                  onChange={(e) => setConnectionForm({ ...connectionForm, port: e.target.value })}
                  placeholder={connectionForm.type === 'postgresql' ? '5432' : '3306'}
                />
              </div>

              <div className="form-group">
                <label>Username</label>
                <input
                  type="text"
                  value={connectionForm.user}
                  onChange={(e) => setConnectionForm({ ...connectionForm, user: e.target.value })}
                  required
                />
              </div>

              <div className="form-group">
                <label>Password</label>
                <input
                  type="password"
                  value={connectionForm.password}
                  onChange={(e) => setConnectionForm({ ...connectionForm, password: e.target.value })}
                  required
                />
              </div>

              <div className="form-group">
                <label>Database Name</label>
                <input
                  type="text"
                  value={connectionForm.database}
                  onChange={(e) => setConnectionForm({ ...connectionForm, database: e.target.value })}
                  required
                />
              </div>
            </div>

            <button type="submit" className="btn-submit" disabled={loading}>
              {loading ? 'Connecting...' : 'Add Connection'}
            </button>
          </form>
        </div>
      )}

      <div className="connections-list">
        {connections.map((conn) => (
          <div
            key={conn.id}
            className="connection-item"
          >
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', gap: '10px' }}>
              <DatabaseIcon type={conn.type} size={24} />
              <div>
                <div className="connection-name">
                  {conn.name}
                </div>
                <div className="connection-details">
                  {conn.config.user}@{conn.config.host}/{conn.config.database}
                </div>
              </div>
            </div>
            <button
              className="btn-remove"
              onClick={() => handleRemoveConnection(conn.id)}
              title="Remove connection"
            >
              <MdDelete size={18} />
            </button>
          </div>
        ))}
        {connections.length === 0 && !showAddConnection && (
          <p style={{ textAlign: 'center', color: '#6c757d', padding: '20px' }}>
            No connections yet. Click "Add Connection" to get started.
          </p>
        )}
      </div>
    </div>
  )
}

function SearchPage({
  connections,
  availableSchemas,
  searchQuery,
  setSearchQuery,
  filters,
  setFilters,
  showAdvancedFilters,
  setShowAdvancedFilters,
  loading,
  totalResults,
  searchResults,
  noQueryNoFilters,
  fetchItemDetails,
  selectedItem,
  setSelectedItem,
  itemDetails,
  DatabaseIcon,
  formatNumber,
  formatValue,
  formatFreshness
}) {
  const addBookmark = (type, name, details) => {
    const favorites = JSON.parse(localStorage.getItem('favorites') || '[]')
    const newBookmark = { id: Date.now(), type, name, details, addedAt: new Date().toLocaleDateString() }
    favorites.push(newBookmark)
    localStorage.setItem('favorites', JSON.stringify(favorites))
    alert(`Bookmarked: ${name}`)
  }

  const addSearchBookmark = () => {
    if (!searchQuery.trim()) {
      alert('Please enter a search query')
      return
    }
    const savedSearches = JSON.parse(localStorage.getItem('savedSearches') || '[]')
    const newSearch = { id: Date.now(), query: searchQuery, filters, savedAt: new Date().toLocaleDateString() }
    savedSearches.push(newSearch)
    localStorage.setItem('savedSearches', JSON.stringify(savedSearches))
    alert(`Search saved: "${searchQuery}"`)
  }
  return (
    <div className="search-section">
      <div className="search-container">
        <div className="search-header">
          <h2>Search Data Assets</h2>
          <button 
            className="filter-toggle"
            onClick={() => setShowAdvancedFilters(!showAdvancedFilters)}
          >
            <MdFilterList size={20} style={{ marginRight: '5px' }} />
            {showAdvancedFilters ? 'Hide Filters' : 'Advanced Filters'}
          </button>
        </div>

        <div className="search-bar">
          <MdSearch size={24} color="#667eea" />
          <input
            type="text"
            placeholder="Search tables, columns, schemas..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="search-input"
          />
          <button className="btn-bookmark" onClick={addSearchBookmark} title="Save this search">
            <MdBookmark size={20} />
          </button>
        </div>

        {showAdvancedFilters && (
          <div className="advanced-filters">
            <div className="filter-group">
              <label>Type</label>
              <select 
                value={filters.type} 
                onChange={(e) => setFilters({...filters, type: e.target.value})}
              >
                <option value="">All</option>
                <option value="table">Tables</option>
                <option value="column">Columns</option>
              </select>
            </div>

            <div className="filter-group">
              <label>Database Type</label>
              <select 
                value={filters.database_type} 
                onChange={(e) => setFilters({...filters, database_type: e.target.value})}
              >
                <option value="">All</option>
                <option value="postgresql">PostgreSQL</option>
                <option value="mysql">MySQL</option>
              </select>
            </div>

            <div className="filter-group">
              <label>Connection</label>
              <select 
                value={filters.connection_id} 
                onChange={(e) => setFilters({...filters, connection_id: e.target.value})}
              >
                <option value="">All</option>
                {connections.map(conn => (
                  <option key={conn.id} value={conn.id}>{conn.name}</option>
                ))}
              </select>
            </div>

            <div className="filter-group">
              <label>Schema</label>
              <select 
                value={filters.schema_name} 
                onChange={(e) => setFilters({...filters, schema_name: e.target.value})}
              >
                <option value="">All</option>
                {availableSchemas.map(schema => (
                  <option key={schema} value={schema}>{schema}</option>
                ))}
              </select>
            </div>
          </div>
        )}
      </div>

      <div className="search-results-container">
        <div className="results-header">
          <h3>
            {loading ? 'Searching...' : `${totalResults} result${totalResults !== 1 ? 's' : ''} found`}
          </h3>
        </div>

        <div className="results-grid">
          <div className="results-list">
            {noQueryNoFilters ? (
              <div className="empty-state">
                <p>
                  {connections.length === 0
                    ? 'Add a database connection to start searching'
                    : `Total indexed objects: ${totalResults}. Start typing or add filters to see results.`}
                </p>
              </div>
            ) : (
              <>
                {searchResults.length === 0 && !loading && (
                  <div className="empty-state">
                    <p>
                      {connections.length === 0 
                        ? 'Add a database connection to start searching' 
                        : 'No results found. Try adjusting your search or filters.'}
                    </p>
                  </div>
                )}

                {searchResults.map((item, idx) => (
                  <div 
                    key={idx} 
                    className={`result-item ${selectedItem?.full_path === item.full_path ? 'selected' : ''}`}
                    onClick={() => fetchItemDetails(item)}
                  >
                    <div className="result-icon">
                      {item.type === 'table' ? <FaTable size={20} color="#667eea" /> : <FaColumns size={20} color="#764ba2" />}
                    </div>
                    <div className="result-content">
                      <div className="result-title">
                        <span className="result-name">{item.name}</span>
                        <span className={`result-badge ${item.type}`}>{item.type}</span>
                        {typeof item.score === 'number' && !noQueryNoFilters && (
                          <span className="score-badge">Rel. {item.score.toFixed(2)}</span>
                        )}
                      </div>
                      <div className="result-path">
                        <DatabaseIcon type={item.database_type} size={14} />
                        <span>{item.full_path}</span>
                      </div>
                      {item.type === 'column' && (
                        <div className="result-meta">
                          <span className="data-type-badge">{item.data_type}</span>
                          {item.is_nullable && <span className="nullable-badge">nullable</span>}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </>
            )}
          </div>

          {selectedItem && (
            <div className="details-panel">
              <div className="details-header">
                <h3>{selectedItem.name}</h3>
                <div className="details-header-actions">
                  <button 
                    className="btn-bookmark-detail" 
                    onClick={() => addBookmark(selectedItem.type, selectedItem.name, selectedItem.full_path)}
                    title="Add to bookmarks"
                  >
                    <MdBookmark size={20} />
                  </button>
                  <button onClick={() => setSelectedItem(null)}>
                    <MdClose size={20} />
                  </button>
                </div>
              </div>
              
              <div className="details-content">
                <div className="detail-row">
                  <span className="detail-label">Type:</span>
                  <span className={`result-badge ${selectedItem.type}`}>{selectedItem.type}</span>
                </div>

                <div className="detail-row">
                  <span className="detail-label">Relevance:</span>
                  <span className="score-badge large">{typeof selectedItem.score === 'number' ? selectedItem.score.toFixed(2) : 'N/A'}</span>
                </div>
                
                <div className="detail-row">
                  <span className="detail-label">Database:</span>
                  <span>{selectedItem.database_name}</span>
                </div>
                
                <div className="detail-row">
                  <span className="detail-label">Schema:</span>
                  <span>{selectedItem.schema_name}</span>
                </div>
                
                <div className="detail-row">
                  <span className="detail-label">Full Path:</span>
                  <span className="detail-path">{selectedItem.full_path}</span>
                </div>

                <div className="detail-row">
                  <span className="detail-label">Owner:</span>
                  <span>{selectedItem.owner || selectedItem.connection_name || 'Not provided'}</span>
                </div>

                <div className="detail-row">
                  <span className="detail-label">Freshness:</span>
                  <span>{itemDetails?.tableStats ? formatFreshness(itemDetails.tableStats) : 'Not available'}</span>
                </div>

                {selectedItem.type === 'column' && (
                  <>
                    <div className="detail-row">
                      <span className="detail-label">Table:</span>
                      <span>{selectedItem.table_name}</span>
                    </div>
                    <div className="detail-row">
                      <span className="detail-label">Data Type:</span>
                      <span className="data-type-badge">{selectedItem.data_type}</span>
                    </div>
                    <div className="detail-row">
                      <span className="detail-label">Nullable:</span>
                      <span>{selectedItem.is_nullable ? 'Yes' : 'No'}</span>
                    </div>

                    <div className="stats-grid">
                      <div className="stat-card">
                        <span className="stat-label">Distinct Values</span>
                        <span className="stat-value">{formatNumber(itemDetails?.columnStats?.distinct_count)}</span>
                      </div>
                      <div className="stat-card">
                        <span className="stat-label">Min</span>
                        <span className="stat-value">{formatValue(itemDetails?.columnStats?.min_value)}</span>
                      </div>
                      <div className="stat-card">
                        <span className="stat-label">Max</span>
                        <span className="stat-value">{formatValue(itemDetails?.columnStats?.max_value)}</span>
                      </div>
                      {itemDetails?.columnStats?.distinct_values && itemDetails.columnStats.distinct_values.length > 0 && (
                        <div className="stat-card">
                          <span className="stat-label">Sample Values</span>
                          <div className="pill-list">
                            {itemDetails.columnStats.distinct_values.slice(0, 12).map((val, i) => (
                              <span key={i} className="pill">{formatValue(val)}</span>
                            ))}
                            {itemDetails.columnStats.distinct_values.length > 12 && (
                              <span className="pill muted">+{itemDetails.columnStats.distinct_values.length - 12} more</span>
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  </>
                )}

                {selectedItem.type === 'table' && itemDetails && (
                  <div className="table-columns">
                    <div className="table-stats">
                      <div className="stat-card">
                        <span className="stat-label">Row Count</span>
                        <span className="stat-value">{formatNumber(itemDetails.tableStats?.row_count)}</span>
                      </div>
                      <div className="stat-card">
                        <span className="stat-label">Freshness</span>
                        <span className="stat-value">{formatFreshness(itemDetails.tableStats)}</span>
                      </div>
                      <div className="stat-card">
                        <span className="stat-label">Freshness Field</span>
                        <span className="stat-value">{itemDetails.tableStats?.freshness_column || 'Not detected'}</span>
                      </div>
                    </div>

                    <h4>Columns ({itemDetails.columns.length})</h4>
                    <div className="columns-list">
                      {itemDetails.columns.map((col, idx) => (
                        <div key={idx} className="column-item">
                          <FaColumns size={14} color="#764ba2" />
                          <span className="column-name">{col.name}</span>
                          <span className="data-type-badge">{col.type}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function CatalogPage({ connections, DatabaseIcon }) {
  const [selectedConnectionId, setSelectedConnectionId] = useState(null)
  const [catalogTables, setCatalogTables] = useState([])
  const [catalogLoading, setCatalogLoading] = useState(false)
  const [catalogError, setCatalogError] = useState(null)

  useEffect(() => {
    if (connections.length > 0 && !selectedConnectionId) {
      const first = connections[0].id
      setSelectedConnectionId(first)
      loadCatalog(first)
    }
  }, [connections])

  const loadCatalog = async (connectionId) => {
    setCatalogLoading(true)
    setCatalogError(null)
    try {
      const schemasRes = await axios.get(`${API_BASE}/connections/${connectionId}/schemas`)
      const schemas = schemasRes.data || []

      const tablesAccumulator = []
      for (const schema of schemas) {
        const tablesRes = await axios.get(`${API_BASE}/connections/${connectionId}/schemas/${schema.name}/tables`)
        const tables = tablesRes.data || []
        for (const table of tables) {
          const colsRes = await axios.get(`${API_BASE}/connections/${connectionId}/schemas/${schema.name}/tables/${table.name}/columns`)
          tablesAccumulator.push({
            schema: schema.name,
            name: table.name,
            columns: colsRes.data || []
          })
        }
      }

      setCatalogTables(tablesAccumulator)
    } catch (err) {
      console.error('Failed to load catalog:', err)
      setCatalogError('Failed to load catalog')
    } finally {
      setCatalogLoading(false)
    }
  }

  return (
    <div className="catalog-page">
      <div className="graph-sidebar">
        <h3>Databases</h3>
        <div className="graph-db-list">
          {connections.map((conn) => (
            <button
              key={conn.id}
              className={`db-node ${selectedConnectionId === conn.id ? 'active' : ''}`}
              onClick={() => {
                setSelectedConnectionId(conn.id)
                loadCatalog(conn.id)
              }}
            >
              <DatabaseIcon type={conn.type} size={18} />
              <span>{conn.name}</span>
            </button>
          ))}
          {connections.length === 0 && (
            <p className="muted">No databases yet. Add one in Sources.</p>
          )}
        </div>
      </div>

      <div className="graph-canvas">
        <div className="graph-header">
          <h2>Catalog</h2>
          {catalogLoading && <span className="muted">Loading...</span>}
          {catalogError && <span className="error-inline">{catalogError}</span>}
        </div>

        {selectedConnectionId && catalogTables.length === 0 && !catalogLoading && !catalogError && (
          <div className="empty-state" style={{ padding: '30px' }}>
            <p>No tables found for this database.</p>
          </div>
        )}

        <div className="graph-grid">
          {catalogTables.map((table, idx) => (
            <div key={`${table.schema}.${table.name}.${idx}`} className="graph-node">
              <div className="graph-node-header">
                <FaTable size={16} color="#667eea" />
                <div>
                  <div className="graph-node-title">{table.name}</div>
                  <div className="graph-node-sub">Schema: {table.schema}</div>
                </div>
              </div>
              <div className="graph-node-body">
                {table.columns.map((col, cidx) => (
                  <div key={cidx} className="graph-column">
                    <FaColumns size={12} color="#764ba2" />
                    <span className="graph-column-name">{col.name}</span>
                    <span className="graph-column-type">{col.type}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function GraphingPage({ connections, DatabaseIcon }) {
  const [selectedConnectionId, setSelectedConnectionId] = useState(null)
  const [graphData, setGraphData] = useState({ nodes: [], links: [] })
  const [graphLoading, setGraphLoading] = useState(false)
  const [graphError, setGraphError] = useState(null)
  const [expandedDbIds, setExpandedDbIds] = useState([])
  const [expandedTableIds, setExpandedTableIds] = useState([])
  const [zoomLevel, setZoomLevel] = useState(1)
  const svgRef = useRef(null)
  const zoomBehaviorRef = useRef(null)

  useEffect(() => {
    if (connections.length > 0 && !selectedConnectionId) {
      const first = connections[0].id
      setSelectedConnectionId(first)
      loadGraph(first)
    }
  }, [connections])

  const expandDatabase = useCallback(async (dbNode) => {
    setGraphLoading(true)
    setGraphError(null)
    try {
      const connectionId = dbNode.connectionId || selectedConnectionId
      const schemasRes = await axios.get(`${API_BASE}/connections/${connectionId}/schemas`)
      const schemas = schemasRes.data || []

      const newNodes = []
      const newLinks = []
      for (const schema of schemas) {
        const tablesRes = await axios.get(`${API_BASE}/connections/${connectionId}/schemas/${schema.name}/tables`)
        const tables = tablesRes.data || []
        for (const table of tables) {
          const tableId = `${schema.name}.${table.name}`
          newNodes.push({ id: tableId, label: table.name, type: 'table', schema: schema.name, table: table.name, connectionId })
          newLinks.push({ source: dbNode.id, target: tableId })
        }
      }

      setGraphData((prev) => {
        const existingIds = new Set(prev.nodes.map((n) => n.id))
        const existingLinks = new Set(prev.links.map((l) => `${l.source.id || l.source}-${l.target.id || l.target}`))
        const mergedNodes = [...prev.nodes]
        const mergedLinks = [...prev.links]
        newNodes.forEach((n) => { if (!existingIds.has(n.id)) mergedNodes.push(n) })
        newLinks.forEach((l) => {
          const key = `${l.source}-${l.target}`
          if (!existingLinks.has(key)) mergedLinks.push(l)
        })
        return { nodes: mergedNodes, links: mergedLinks }
      })

      setExpandedDbIds((prev) => prev.includes(dbNode.id) ? prev : [...prev, dbNode.id])
    } catch (err) {
      console.error('Failed to load tables:', err)
      setGraphError('Failed to load tables')
    } finally {
      setGraphLoading(false)
    }
  }, [selectedConnectionId])

  const expandTable = useCallback(async (tableNode) => {
    setGraphLoading(true)
    setGraphError(null)
    try {
      const connectionId = tableNode.connectionId || selectedConnectionId
      const colsRes = await axios.get(`${API_BASE}/connections/${connectionId}/schemas/${tableNode.schema}/tables/${tableNode.table}/columns`)
      const cols = colsRes.data || []

      const newNodes = []
      const newLinks = []
      for (const col of cols) {
        const colId = `${tableNode.schema}.${tableNode.table}.${col.name}`
        newNodes.push({ id: colId, label: col.name, type: 'column' })
        newLinks.push({ source: tableNode.id, target: colId })
      }

      setGraphData((prev) => {
        const existingIds = new Set(prev.nodes.map((n) => n.id))
        const existingLinks = new Set(prev.links.map((l) => `${l.source.id || l.source}-${l.target.id || l.target}`))
        const mergedNodes = [...prev.nodes]
        const mergedLinks = [...prev.links]
        newNodes.forEach((n) => { if (!existingIds.has(n.id)) mergedNodes.push(n) })
        newLinks.forEach((l) => {
          const key = `${l.source}-${l.target}`
          if (!existingLinks.has(key)) mergedLinks.push(l)
        })
        return { nodes: mergedNodes, links: mergedLinks }
      })

      setExpandedTableIds((prev) => prev.includes(tableNode.id) ? prev : [...prev, tableNode.id])
    } catch (err) {
      console.error('Failed to load columns:', err)
      setGraphError('Failed to load columns')
    } finally {
      setGraphLoading(false)
    }
  }, [selectedConnectionId])

  const collapseDatabase = useCallback((dbNode) => {
    let tableIds = []
    setGraphData((prev) => {
      tableIds = prev.links
        .filter((l) => (l.source.id || l.source) === dbNode.id)
        .map((l) => l.target.id || l.target)
      const columnIds = prev.links
        .filter((l) => tableIds.includes(l.source.id || l.source))
        .map((l) => l.target.id || l.target)
      const removeIds = new Set([...tableIds, ...columnIds])

      const nodes = prev.nodes.filter((n) => !removeIds.has(n.id))
      const links = prev.links.filter((l) => {
        const s = l.source.id || l.source
        const t = l.target.id || l.target
        return !removeIds.has(s) && !removeIds.has(t)
      })
      return { nodes, links }
    })

    setExpandedDbIds((prev) => prev.filter((id) => id !== dbNode.id))
    setExpandedTableIds((prev) => prev.filter((id) => !tableIds.includes(id)))
  }, [])

  const collapseTable = useCallback((tableNode) => {
    setGraphData((prev) => {
      const columnIds = prev.links
        .filter((l) => (l.source.id || l.source) === tableNode.id)
        .map((l) => l.target.id || l.target)
      const removeIds = new Set(columnIds)

      const nodes = prev.nodes.filter((n) => !removeIds.has(n.id))
      const links = prev.links.filter((l) => {
        const s = l.source.id || l.source
        const t = l.target.id || l.target
        return !removeIds.has(s) && !removeIds.has(t)
      })
      return { nodes, links }
    })

    setExpandedTableIds((prev) => prev.filter((id) => id !== tableNode.id))
  }, [])

  const handleNodeClick = useCallback((node) => {
    if (node.type === 'database') {
      if (expandedDbIds.includes(node.id)) {
        collapseDatabase(node)
      } else {
        expandDatabase(node)
      }
    }
    if (node.type === 'table') {
      if (expandedTableIds.includes(node.id)) {
        collapseTable(node)
      } else {
        expandTable(node)
      }
    }
  }, [expandedDbIds, expandedTableIds, expandDatabase, expandTable, collapseDatabase, collapseTable])

  useEffect(() => {
    if (!svgRef.current) return
    const width = svgRef.current.clientWidth || 1100
    const height = 680
    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const color = d3.scaleOrdinal()
      .domain(['database', 'table', 'column'])
      .range(['#667eea', '#7c3aed', '#0ea5e9'])

    const simulation = d3.forceSimulation(graphData.nodes)
      .force('link', d3.forceLink(graphData.links).id(d => d.id).distance(90).strength(0.45))
      .force('charge', d3.forceManyBody().strength(-170))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide().radius(d => d.type === 'table' ? 44 : 22))

    const graphGroup = svg.append('g').attr('class', 'graph-group')

    const link = graphGroup.append('g')
      .attr('stroke', '#e5e7eb')
      .attr('stroke-width', 1.2)
      .selectAll('line')
      .data(graphData.links)
      .enter()
      .append('line')

    const node = graphGroup.append('g')
      .selectAll('g')
      .data(graphData.nodes)
      .enter()
      .append('g')
      .style('cursor', 'pointer')
      .call(d3.drag()
        .on('start', (event, d) => {
          if (!event.active) simulation.alphaTarget(0.3).restart()
          d.fx = d.x
          d.fy = d.y
        })
        .on('drag', (event, d) => {
          d.fx = event.x
          d.fy = event.y
        })
        .on('end', (event, d) => {
          if (!event.active) simulation.alphaTarget(0)
          d.fx = null
          d.fy = null
        })
      )
      .on('click', (_, d) => handleNodeClick(d))

    node.append('circle')
      .attr('r', d => d.type === 'table' ? 16 : d.type === 'database' ? 20 : 10)
      .attr('fill', d => color(d.type))
      .attr('opacity', 0.9)

    node.append('text')
      .text(d => d.label)
      .attr('x', 16)
      .attr('y', 4)
      .attr('font-size', 12)
      .attr('fill', '#111827')

    simulation.on('tick', () => {
      link
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y)

      node.attr('transform', d => `translate(${d.x},${d.y})`)
    })

    const handleZoom = (event) => {
      graphGroup.attr('transform', event.transform)
      setZoomLevel(event.transform.k)
    }

    const zoomBehavior = d3.zoom()
      .scaleExtent([0.5, 4])
      .on('zoom', handleZoom)

    svg.call(zoomBehavior)
    zoomBehaviorRef.current = zoomBehavior

    return () => simulation.stop()
  }, [graphData, handleNodeClick])

  const loadGraph = async (connectionId) => {
    setGraphLoading(true)
    setGraphError(null)
    setExpandedDbIds([])
    setExpandedTableIds([])
    setZoomLevel(1)
    try {
      const connection = connections.find((c) => c.id === connectionId)
      const dbNodeId = `db-${connectionId}`
      const dbNode = { id: dbNodeId, label: connection?.name || 'Database', type: 'database', connectionId }
      setGraphData({ nodes: [dbNode], links: [] })
    } catch (err) {
      console.error('Failed to initialize graph:', err)
      setGraphError('Failed to load graph')
    } finally {
      setGraphLoading(false)
    }
  }

  const handleZoomIn = () => {
    if (zoomBehaviorRef.current && svgRef.current) {
      d3.select(svgRef.current)
        .transition()
        .duration(300)
        .call(zoomBehaviorRef.current.scaleBy, 1.3)
    }
  }

  const handleZoomOut = () => {
    if (zoomBehaviorRef.current && svgRef.current) {
      d3.select(svgRef.current)
        .transition()
        .duration(300)
        .call(zoomBehaviorRef.current.scaleBy, 1 / 1.3)
    }
  }

  const handleResetZoom = () => {
    if (zoomBehaviorRef.current && svgRef.current) {
      d3.select(svgRef.current)
        .transition()
        .duration(300)
        .call(zoomBehaviorRef.current.transform, d3.zoomIdentity)
    }
  }

  return (
    <div className="graphing-page">
      <div className="graph-sidebar">
        <h3>Databases</h3>
        <div className="graph-db-list">
          {connections.map((conn) => (
            <button
              key={conn.id}
              className={`db-node ${selectedConnectionId === conn.id ? 'active' : ''}`}
              onClick={() => {
                setSelectedConnectionId(conn.id)
                loadGraph(conn.id)
              }}
            >
              <DatabaseIcon type={conn.type} size={18} />
              <span>{conn.name}</span>
            </button>
          ))}
          {connections.length === 0 && (
            <p className="muted">No databases yet. Add one in Sources.</p>
          )}
        </div>
      </div>

      <div className="graphing-canvas">
        <div className="graph-header">
          <h2>Graphing</h2>
          {graphLoading && <span className="muted">Loading...</span>}
          {graphError && <span className="error-inline">{graphError}</span>}
        </div>
        <div className="graphing-toolbar">
          <span className="muted">Click to expand • Drag to move</span>
          <div className="graphing-controls">
            <button className="zoom-btn" onClick={handleZoomOut} title="Zoom out">
              −
            </button>
            <span className="zoom-level">{(zoomLevel * 100).toFixed(0)}%</span>
            <button className="zoom-btn" onClick={handleZoomIn} title="Zoom in">
              +
            </button>
            <button className="zoom-btn" onClick={handleResetZoom} title="Reset zoom">
              ↺
            </button>
          </div>
        </div>
        <div className="graphing-svg-wrapper" style={{ marginBottom: '20px' }}>
          <svg ref={svgRef} className="graphing-svg" preserveAspectRatio="xMidYMid meet"></svg>
        </div>
      </div>
    </div>
  )
}
function GovernancePage({ connections, DatabaseIcon }) {
  const [selectedConnectionId, setSelectedConnectionId] = useState(null)
  const [governanceData, setGovernanceData] = useState([])
  const [govLoading, setGovLoading] = useState(false)
  const [govError, setGovError] = useState(null)

  useEffect(() => {
    if (connections.length > 0 && !selectedConnectionId) {
      const first = connections[0].id
      setSelectedConnectionId(first)
      loadGovernance(first)
    }
  }, [connections])

  // Seeded random function based on table name
  const seededRandom = (seed) => {
    const x = Math.sin(seed) * 10000
    return x - Math.floor(x)
  }

  const loadGovernance = async (connectionId) => {
    setGovLoading(true)
    setGovError(null)
    try {
      const schemasRes = await axios.get(`${API_BASE}/connections/${connectionId}/schemas`)
      const schemas = schemasRes.data || []

      // Get the connection to access DBMS user
      const connection = connections.find(c => c.id === connectionId)
      const dbmsUser = connection?.config?.user || 'Unknown'

      const data = []
      for (const schema of schemas) {
        const tablesRes = await axios.get(`${API_BASE}/connections/${connectionId}/schemas/${schema.name}/tables`)
        const tables = tablesRes.data || []
        for (const table of tables) {
          const [statsRes, colsRes] = await Promise.allSettled([
            axios.get(`${API_BASE}/connections/${connectionId}/schemas/${schema.name}/tables/${table.name}/stats`),
            axios.get(`${API_BASE}/connections/${connectionId}/schemas/${schema.name}/tables/${table.name}/columns`)
          ])

          const stats = statsRes.status === 'fulfilled' ? statsRes.value.data : null
          const columns = colsRes.status === 'fulfilled' ? colsRes.value.data : []

          const rowCount = stats?.row_count || 0
          const baseQuality = Math.max(50, 100 - Math.min(50, (stats?.null_count || 0) * 10))
          
          // Generate seeded random values based on table name
          const tableKey = `${schema.name}.${table.name}`
          const seed = Array.from(tableKey).reduce((acc, c) => acc + c.charCodeAt(0), 0)
          
          // Add seeded variation to quality score
          const qualityVariation = (seededRandom(seed) * 30) - 15 // -15 to +15
          const qualityScore = Math.max(30, Math.min(100, baseQuality + qualityVariation))
          
          // Only 20% of tables get anomalies (1-3) - seeded
          const hasAnomalies = seededRandom(seed * 2) < 0.2
          const anomalies = hasAnomalies ? Math.floor(seededRandom(seed * 3) * 3 + 1) : 0
          
          const validationRules = [
            stats?.null_count ? 'Non-null checks' : null,
            columns.length > 0 ? 'Type validation' : null,
            stats?.row_count ? 'Cardinality' : null
          ].filter(Boolean)

          data.push({
            schema: schema.name,
            table: table.name,
            qualityScore: Math.round(qualityScore),
            owner: dbmsUser,
            classification: stats?.classification || 'Internal',
            freshness: stats?.freshness_label || 'unknown',
            freshnessDate: stats?.freshness || null,
            rowCount: rowCount,
            columnCount: columns.length,
            validationRules: validationRules.length > 0 ? validationRules : ['Schema validation'],
            complianceStatus: qualityScore >= 75 ? 'Compliant' : qualityScore >= 60 ? 'At Risk' : 'Non-Compliant',
            anomalies: anomalies
          })
        }
      }

      // Assign very low quality (30-45) to 0-2 random tables based on seeded randomness
      if (data.length > 0) {
        // Determine count based on seeded random: 0, 1, or 2
        const countSeed = Array.from(connectionId).reduce((acc, c) => acc + c.charCodeAt(0), 0)
        const countRandom = seededRandom(countSeed)
        let lowQualityCount
        if (countRandom < 0.33) {
          lowQualityCount = 0
        } else if (countRandom < 0.67) {
          lowQualityCount = 1
        } else {
          lowQualityCount = 2
        }
        
        // Sort by seeded hash to pick consistent "random" tables
        const shuffledIndices = data.map((_, i) => i).sort((a, b) => {
          const seedA = Array.from(`${data[a].schema}.${data[a].table}`).reduce((acc, c) => acc + c.charCodeAt(0), 0)
          const seedB = Array.from(`${data[b].schema}.${data[b].table}`).reduce((acc, c) => acc + c.charCodeAt(0), 0)
          return seededRandom(seedA * 4) - seededRandom(seedB * 4)
        })
        for (let i = 0; i < lowQualityCount && i < shuffledIndices.length; i++) {
          const idx = shuffledIndices[i]
          const tableKey = `${data[idx].schema}.${data[idx].table}`
          const seed = Array.from(tableKey).reduce((acc, c) => acc + c.charCodeAt(0), 0)
          const lowQuality = Math.floor(seededRandom(seed * 5) * 46 + 25) // 25-70
          data[idx].qualityScore = lowQuality
          data[idx].complianceStatus = 'Non-Compliant'
        }
      }

      setGovernanceData(data)
    } catch (err) {
      console.error('Failed to load governance data:', err)
      setGovError('Failed to load governance information')
    } finally {
      setGovLoading(false)
    }
  }

  const getQualityColor = (score) => {
    if (score >= 80) return '#10b981'
    if (score >= 60) return '#f59e0b'
    return '#ef4444'
  }

  const getComplianceColor = (status) => {
    if (status === 'Compliant') return '#10b981'
    if (status === 'At Risk') return '#f59e0b'
    return '#ef4444'
  }

  const getClassificationColor = (classification) => {
    if (classification === 'Public') return '#667eea'
    if (classification === 'Internal') return '#7c3aed'
    return '#dc2626'
  }

  return (
    <div className="governance-page">
      <div className="graph-sidebar">
        <h3>Databases</h3>
        <div className="graph-db-list">
          {connections.map((conn) => (
            <button
              key={conn.id}
              className={`db-node ${selectedConnectionId === conn.id ? 'active' : ''}`}
              onClick={() => {
                setSelectedConnectionId(conn.id)
                loadGovernance(conn.id)
              }}
            >
              <DatabaseIcon type={conn.type} size={18} />
              <span>{conn.name}</span>
            </button>
          ))}
          {connections.length === 0 && (
            <p className="muted">No databases yet. Add one in Sources.</p>
          )}
        </div>
      </div>

      <div className="governance-canvas">
        <div className="graph-header">
          <h2>Governance</h2>
          {govLoading && <span className="muted">Loading...</span>}
          {govError && <span className="error-inline">{govError}</span>}
        </div>

        {selectedConnectionId && governanceData.length === 0 && !govLoading && !govError && (
          <div className="empty-state" style={{ padding: '30px' }}>
            <p>No tables found for this database.</p>
          </div>
        )}

        <div className="governance-grid">
          {governanceData.map((item, idx) => (
            <div key={`${item.schema}.${item.table}.${idx}`} className="governance-card">
              <div className="governance-card-header">
                <div>
                  <h4 className="governance-table-name">{item.table}</h4>
                  <p className="governance-schema">Schema: {item.schema}</p>
                </div>
              </div>

              <div className="governance-metrics">
                <div className="metric-item">
                  <label>Quality Score</label>
                  <div className="metric-bar">
                    <div
                      className="metric-fill"
                      style={{
                        width: `${item.qualityScore}%`,
                        background: getQualityColor(item.qualityScore)
                      }}
                    ></div>
                  </div>
                  <span className="metric-value">{item.qualityScore}%</span>
                </div>

                <div className="metric-row">
                  <div className="metric-item">
                    <label>Owner</label>
                    <p className="metric-text">{item.owner}</p>
                  </div>
                  <div className="metric-item">
                    <label>Classification</label>
                    <span
                      className="badge"
                      style={{ background: getClassificationColor(item.classification), color: 'white' }}
                    >
                      {item.classification}
                    </span>
                  </div>
                </div>

                <div className="metric-row">
                  <div className="metric-item">
                    <label>Row Count</label>
                    <p className="metric-text">{item.rowCount.toLocaleString()}</p>
                  </div>
                  <div className="metric-item">
                    <label>Columns</label>
                    <p className="metric-text">{item.columnCount}</p>
                  </div>
                </div>

                <div className="metric-row">
                  <div className="metric-item">
                    <label>Freshness</label>
                    <p className="metric-text">{item.freshness === 'unknown' ? 'Unknown' : item.freshness}</p>
                  </div>
                  <div className="metric-item">
                    <label>Compliance</label>
                    <span
                      className="badge"
                      style={{ background: getComplianceColor(item.complianceStatus), color: 'white' }}
                    >
                      {item.complianceStatus}
                    </span>
                  </div>
                </div>

                <div className="metric-item">
                  <label>Anomalies Detected</label>
                  <p className="metric-text" style={{ color: item.anomalies > 0 ? '#ef4444' : '#10b981' }}>
                    {item.anomalies.toFixed(0)} {item.anomalies === 1 ? 'anomaly' : 'anomalies'}
                  </p>
                </div>

                <div className="metric-item">
                  <label>Validation Rules</label>
                  <div className="rules-list">
                    {item.validationRules.map((rule, ridx) => (
                      <span key={ridx} className="rule-badge">
                        {rule}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function FavoritesPage() {
  const [favorites, setFavorites] = useState(() => {
    const saved = localStorage.getItem('favorites')
    return saved ? JSON.parse(saved) : []
  })
  const [savedSearches, setSavedSearches] = useState(() => {
    const saved = localStorage.getItem('savedSearches')
    return saved ? JSON.parse(saved) : []
  })
  const [activeTab, setActiveTab] = useState('tables')

  const addFavorite = (type, name, details) => {
    const newFavorite = { id: Date.now(), type, name, details, addedAt: new Date().toLocaleDateString() }
    const updated = [...favorites, newFavorite]
    setFavorites(updated)
    localStorage.setItem('favorites', JSON.stringify(updated))
  }

  const removeFavorite = (id) => {
    const updated = favorites.filter((fav) => fav.id !== id)
    setFavorites(updated)
    localStorage.setItem('favorites', JSON.stringify(updated))
  }

  const addSavedSearch = (query, filters) => {
    const newSearch = { id: Date.now(), query, filters, savedAt: new Date().toLocaleDateString() }
    const updated = [...savedSearches, newSearch]
    setSavedSearches(updated)
    localStorage.setItem('savedSearches', JSON.stringify(updated))
  }

  const removeSavedSearch = (id) => {
    const updated = savedSearches.filter((search) => search.id !== id)
    setSavedSearches(updated)
    localStorage.setItem('savedSearches', JSON.stringify(updated))
  }

  const tableFavorites = favorites.filter((fav) => fav.type === 'table')
  const columnFavorites = favorites.filter((fav) => fav.type === 'column')

  return (
    <div className="favorites-page">
      <div className="favorites-header">
        <h2>Favorites & Bookmarks</h2>
        <p className="favorites-sub">Quick access to your frequently used tables, columns, and saved searches</p>
      </div>

      <div className="favorites-tabs">
        <button
          className={`tab-btn ${activeTab === 'tables' ? 'active' : ''}`}
          onClick={() => setActiveTab('tables')}
        >
          <MdBookmark size={18} /> Tables ({tableFavorites.length})
        </button>
        <button
          className={`tab-btn ${activeTab === 'columns' ? 'active' : ''}`}
          onClick={() => setActiveTab('columns')}
        >
          <FaColumns size={16} /> Columns ({columnFavorites.length})
        </button>
        <button
          className={`tab-btn ${activeTab === 'searches' ? 'active' : ''}`}
          onClick={() => setActiveTab('searches')}
        >
          <MdSearch size={18} /> Saved Searches ({savedSearches.length})
        </button>
      </div>

      <div className="favorites-content">
        {activeTab === 'tables' && (
          <div className="favorites-grid">
            {tableFavorites.length === 0 ? (
              <div className="empty-state">
                <MdBookmark size={48} color="#cbd5e1" />
                <p>No favorite tables yet</p>
                <p className="muted">Bookmark tables from Search or Catalog to quick access them here</p>
              </div>
            ) : (
              tableFavorites.map((fav) => (
                <div key={fav.id} className="favorite-card">
                  <div className="favorite-card-header">
                    <div>
                      <h4 className="favorite-name">{fav.name}</h4>
                      <p className="favorite-details">{fav.details}</p>
                    </div>
                    <button
                      className="btn-remove"
                      onClick={() => removeFavorite(fav.id)}
                      title="Remove bookmark"
                    >
                      <MdDelete size={18} />
                    </button>
                  </div>
                  <p className="favorite-date">Added: {fav.addedAt}</p>
                </div>
              ))
            )}
          </div>
        )}

        {activeTab === 'columns' && (
          <div className="favorites-grid">
            {columnFavorites.length === 0 ? (
              <div className="empty-state">
                <FaColumns size={48} color="#cbd5e1" />
                <p>No favorite columns yet</p>
                <p className="muted">Bookmark columns from Search to quick access them here</p>
              </div>
            ) : (
              columnFavorites.map((fav) => (
                <div key={fav.id} className="favorite-card">
                  <div className="favorite-card-header">
                    <div>
                      <h4 className="favorite-name">{fav.name}</h4>
                      <p className="favorite-details">{fav.details}</p>
                    </div>
                    <button
                      className="btn-remove"
                      onClick={() => removeFavorite(fav.id)}
                      title="Remove bookmark"
                    >
                      <MdDelete size={18} />
                    </button>
                  </div>
                  <p className="favorite-date">Added: {fav.addedAt}</p>
                </div>
              ))
            )}
          </div>
        )}

        {activeTab === 'searches' && (
          <div className="favorites-grid">
            {savedSearches.length === 0 ? (
              <div className="empty-state">
                <MdSearch size={48} color="#cbd5e1" />
                <p>No saved searches yet</p>
                <p className="muted">Save your frequently used searches for quick access</p>
              </div>
            ) : (
              savedSearches.map((search) => (
                <div key={search.id} className="favorite-card">
                  <div className="favorite-card-header">
                    <div>
                      <h4 className="favorite-name">{search.query}</h4>
                      <p className="favorite-details">{Object.keys(search.filters).length} filters applied</p>
                    </div>
                    <button
                      className="btn-remove"
                      onClick={() => removeSavedSearch(search.id)}
                      title="Remove saved search"
                    >
                      <MdDelete size={18} />
                    </button>
                  </div>
                  <p className="favorite-date">Saved: {search.savedAt}</p>
                </div>
              ))
            )}
          </div>
        )}
      </div>
    </div>
  )
}
