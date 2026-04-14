#!/usr/bin/env python3
"""
FIP Radio Direct API Logger
Polls https://api.radiofrance.fr/livemeta/pull/7 directly.
Detects song changes using UUID and logs only new play instances.
Captures full metadata (Label, Year, Composers, Genres).

Requirements:
    pip install requests

Usage:
    python fiplogger.py [--interval SECONDS] [--db PATH] [--verbose]
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

try:
    import requests
except ImportError:
    print("Error: 'requests' package not found. Install it with: pip install requests")
    sys.exit(1)

# API Endpoint
API_URL = "https://api.radiofrance.fr/livemeta/pull/7"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Register SQLite3 Adapter/Converter
def adapt_datetime(val):
    return val.isoformat()

def convert_datetime(val):
    return datetime.fromisoformat(val.decode())

# Registreer de adapter en converter
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)

class FIPDatabase:
    """Handles database operations."""
    
    def __init__(self, db_path: str = "fiplogger.db"):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._connect()
        self._create_tables()
    
    def _connect(self) -> None:
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"Connected to database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def _create_tables(self) -> None:
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS song_plays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                play_start_time TIMESTAMP NOT NULL,
                
                -- Core Metadata
                title TEXT,
                artist TEXT,
                album TEXT,
                duration INTEGER,
                uuid TEXT, -- Unique ID from Radio France API
                
                -- Extended Metadata from 'meta' object
                label TEXT,
                year INTEGER,
                composers TEXT,   -- JSON string of composers/authors
                cover_url TEXT,
                station_id TEXT DEFAULT 'fip',
                
                -- Raw payload
                raw_payload TEXT
            )
        ''')
        
        # Indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_uuid ON song_plays(uuid)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_start_time ON song_plays(play_start_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_artist ON song_plays(artist)')
        
        self.conn.commit()
        logger.info("Database schema verified")

    def insert_play(self, song_data: Dict[str, Any], start_time: datetime) -> bool:
        """Insert a new play instance."""
        if not self.conn:
            return False
        
        cursor = self.conn.cursor()
        
        try:
            # Extract core fields
            title = song_data.get('title')
            artist = song_data.get('authors')
            album = song_data.get('titreAlbum')
            duration = int(song_data.get('end')) - int(song_data.get('start'))
            uuid = song_data.get('uuid')
            cover_url = song_data.get('visual')
            label = song_data.get('label')
            year = song_data.get('anneeEditionMusique')
            composers = song_data.get('composers', [])
            
            # Convert lists to JSON strings for storage
            composers_str = json.dumps(composers) if isinstance(composers, list) else str(composers)
            
            raw_payload = json.dumps(song_data)
            
            cursor.execute('''
                INSERT INTO song_plays 
                (play_start_time, title, artist, album, duration, uuid, label, year, composers, cover_url, raw_payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                start_time, title, artist, album, duration, uuid, label, year, composers_str, cover_url, raw_payload
            ))
            
            self.conn.commit()
            logger.info(f"NEW PLAY LOGGED: {title} - {artist} (UUID: {uuid})")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            self.conn.rollback()
            return False

    def get_most_played(self, limit: int = 10) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT title, artist, COUNT(*) as play_count
            FROM song_plays
            GROUP BY title, artist
            ORDER BY play_count DESC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM song_plays')
        total = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(DISTINCT title) FROM song_plays')
        unique = cursor.fetchone()[0]
        return {'total_plays': total, 'unique_songs': unique}

    def close(self):
        if self.conn:
            self.conn.close()


class FIPDirectLogger:
    """
    Polls Radio France API directly.
    Detects changes via UUID.
    """
    
    def __init__(self, db: FIPDatabase, interval: int = 10, verbose: bool = False):
        self.db = db
        self.interval = interval
        self.verbose = verbose
        self.running = False
        
        # Track the UUID of the song currently being listened to in this session
        self.current_session_uuid: Optional[str] = None

    def _fetch_live_meta(self) -> Optional[Dict[str, Any]]:
        """Fetch data from Radio France API."""
        try:
            # Add a user agent to be polite
            headers = {'User-Agent': 'Linux UPnP/1.0 Sonos/82.2-59204 (ZPS15)'}
            response = requests.get(API_URL, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API Request failed: {e}")
            return None
        except json.JSONDecodeError:
            logger.error("Invalid JSON response from API")
            return None

    def _extract_now_playing(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract the current 'steps' object from the API response."""
        if not data or 'steps' not in data:
            return None
            
        level = data['levels'][0]
        uid = level['items'][level['position']]
        step = data['steps'][uid]
        return step

    def poll_once(self) -> bool:
        data = self._fetch_live_meta()
        if not data:
            return False

        now_playing = self._extract_now_playing(data)
        if not now_playing:
            logger.warning("No 'steps' data in API response")
            return False

        current_uuid = now_playing.get('uuid')
        #now = datetime.now()
        now = datetime.fromtimestamp(now_playing.get('start'))

        # LOGIC:
        # 1. If no UUID seen yet in this session, log it but don't store in db.
        # 2. If UUID matches the one in memory, skip (still playing).
        # 3. If UUID differs, log the new song and update db.

        if self.current_session_uuid is None:
            self.current_session_uuid = current_uuid
            if self.verbose:
                logger.info(f"Session started. Currently playing UUID: {current_uuid} (Waiting for change...)")
            return False

        if current_uuid == self.current_session_uuid:
            if self.verbose:
                logger.info(f"Still playing UUID: {current_uuid} (Skipping)")
            return False
        
        # SONG CHANGED!
        success = self.db.insert_play(now_playing, now)
        self.interval = (int(now_playing.get('end')) - int(now_playing.get('start'))) / 2
        
        if success:
            self.current_session_uuid = current_uuid
            if self.verbose:
                title = now_playing.get('title', 'Unknown')
                artist = now_playing.get('authors', 'Unknown')
                logger.info(f"Song changed! Logged: {title} - {artist}")
        
        return success

    def start(self):
        self.running = True
        logger.info(f"Starting Direct API Logger (interval: {self.interval}s)...")
        try:
            while self.running:
                self.poll_once()
                logger.info(f"Sleeping for {self.interval}s...")
                time.sleep(self.interval)
        except KeyboardInterrupt:
            logger.info("Stopped by user.")
            self.running = False


def main():
    parser = argparse.ArgumentParser(description='FIP Direct API Logger')
    parser.add_argument('--interval', '-i', type=int, default=30, help='Polling interval (seconds). Default 10.')
    parser.add_argument('--db', '-d', type=str, default='fiplogger.db', help='Database path')
    parser.add_argument('--logfile', '-l', type=str, default='fiplogger.log', help='Logfile path')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--stats', action='store_true', help='Show stats and exit')
    parser.add_argument('--top', type=int, default=0, help='Show top N songs and exit')
    
    args = parser.parse_args()
    
    # Add logging to file
    if args.logfile:
        file_handler = logging.FileHandler(args.logfile)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logging.getLogger().addHandler(file_handler)
        logger.info(f"Logging to file: {args.logfile}")
    
    # Safety check for interval
    if args.interval < 5:
        logger.warning("Interval too short (< 5s). Setting to 5s to avoid rate limiting.")
        args.interval = 5

    try:
        db = FIPDatabase(db_path=args.db)
        
        if args.stats:
            stats = db.get_stats()
            print(f"\nTotal Plays Logged: {stats['total_plays']}")
            print(f"Unique Songs: {stats['unique_songs']}")
            db.close()
            return

        if args.top > 0:
            top_songs = db.get_most_played(args.top)
            print(f"\nTop {args.top} Most Played Songs:")
            for i, s in enumerate(top_songs, 1):
                print(f"{i}. {s['title']} - {s['artist']} ({s['play_count']} times)")
            db.close()
            return

        fiplogger = FIPDirectLogger(db, args.interval, args.verbose)
        fiplogger.start()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if 'db' in locals():
            db.close()

if __name__ == '__main__':
    main()
