const fs = require('fs').promises;
const path = require('path');

const PREFS_PATH = path.join(__dirname, 'database-preferences.json');

class PreferencesManager {
  constructor() {
    this.preferences = null;
  }

  async loadPreferences() {
    try {
      const data = await fs.readFile(PREFS_PATH, 'utf8');
      this.preferences = JSON.parse(data);
    } catch (_err) {
      this.preferences = { visibleDatabases: [] };
      await this.savePreferences(this.preferences);
    }
    return this.preferences;
  }

  async savePreferences(preferences) {
    this.preferences = preferences;
    await fs.writeFile(PREFS_PATH, JSON.stringify(preferences, null, 2), 'utf8');
    return this.preferences;
  }

  async getVisibleDatabases() {
    if (!this.preferences) {
      await this.loadPreferences();
    }
    return this.preferences.visibleDatabases || [];
  }

  async setVisibleDatabases(databaseIds) {
    await this.loadPreferences();
    this.preferences.visibleDatabases = databaseIds;
    await this.savePreferences(this.preferences);
    return this.preferences.visibleDatabases;
  }
}

module.exports = new PreferencesManager();
