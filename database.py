import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class WeatherDatabase:
    def __init__(self, db_path="weather.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS weather_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    temperature_2m REAL,
                    relative_humidity_2m REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Database init error: {e}")
            raise
    
    def store_weather_data(self, records):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            count = 0
            for record in records:
                cursor.execute('''
                    INSERT INTO weather_data 
                    (timestamp, latitude, longitude, temperature_2m, relative_humidity_2m)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    record['timestamp'],
                    record['latitude'],
                    record['longitude'],
                    record['temperature_2m'],
                    record['relative_humidity_2m']
                ))
                count += 1
            
            conn.commit()
            conn.close()
            logger.info(f"Stored {count} records in database")
            return count
            
        except Exception as e:
            logger.error(f"Store error: {e}")
            raise
    
    def get_recent_data(self, hours=48):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM weather_data 
                WHERE datetime(timestamp) >= datetime('now', '-{} hours')
                ORDER BY timestamp DESC
                LIMIT 1000
            '''.format(hours))
            
            rows = cursor.fetchall()
            conn.close()
            
            columns = ['id', 'timestamp', 'latitude', 'longitude', 
                      'temperature_2m', 'relative_humidity_2m', 'created_at']
            
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Retrieve error: {e}")
            raise
    
    def get_data_summary(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM weather_data')
            total = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT MIN(timestamp), MAX(timestamp) 
                FROM weather_data
            ''')
            date_range = cursor.fetchone()
            
            cursor.execute('''
                SELECT DISTINCT latitude, longitude 
                FROM weather_data LIMIT 10
            ''')
            locations = cursor.fetchall()
            
            conn.close()
            
            return {
                "total_records": total,
                "unique_locations": len(locations),
                "date_range": {
                    "oldest": date_range[0],
                    "latest": date_range[1]
                },
                "locations": [{"lat": loc[0], "lon": loc[1]} for loc in locations],
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Summary error: {e}")
            raise

if __name__ == '__main__':
    db = WeatherDatabase()
    print("Database initialized successfully!")
