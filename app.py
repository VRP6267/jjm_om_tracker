#!/usr/bin/env python3
"""
Multi-District JJM O&M Tracker
VERSION 1.3.3 - FINAL, STABLE VERSION
Features:
1. Role-based multi-user authentication with flexible Agency+Multi-Block filtering for Engineers
2. Admin panel with scheme count preview for Engineer assignments
3. Personalized dashboards with data filtered by user role, agency, and multiple blocks
4. Engineers can be assigned to specific Agency across multiple blocks
5. WhatsApp functionality available to Engineers only in Issues Dashboard and Problem Schemes
6. Import Data restricted to Managers and above
7. Real-time scheme count validation during user assignment
8. Case-insensitive block assignment in admin panel
9. Corrected state management in the "Add User" form
10. Data cleaning on import to remove leading/trailing spaces
"""

import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, timezone
import io
import os
import numpy as np
import matplotlib.pyplot as plt
from urllib.parse import quote
import hashlib
import secrets
import base64

# --- Page Configuration & Timezone ---
st.set_page_config(
    page_title="Multi-District JJM O&M Tracker",
    page_icon="🚰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Timezone Configuration for IST (UTC+5:30) ---
IST = timezone(timedelta(hours=5, minutes=30))

# --- Database Path ---
script_dir = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(script_dir, "simplified_jjm_tracker.db")

# --- Core Functions ---

def hash_password(password):
    """Hash password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hashed_password):
    """Verify password against hash"""
    return hashlib.sha256(password.encode()).hexdigest() == hashed_password

def init_database():
    """Initialize the simplified database schema with agency support"""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Districts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS districts (
                district_id TEXT PRIMARY KEY,
                district_name TEXT NOT NULL,
                district_code TEXT UNIQUE NOT NULL
            )
        ''')
        
        # Admin users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_users (
                admin_id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name TEXT,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # District users table (engineers) - Enhanced with agency support
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS district_users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                district_id TEXT NOT NULL,
                username TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                full_name TEXT NOT NULL,
                email TEXT,
                role TEXT NOT NULL,
                assigned_block TEXT,
                assigned_agency TEXT,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (district_id) REFERENCES districts(district_id),
                UNIQUE (username)
            )
        ''')

        # Check if assigned_agency column exists, if not add it
        cursor.execute("PRAGMA table_info(district_users)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'assigned_agency' not in columns:
            cursor.execute("ALTER TABLE district_users ADD COLUMN assigned_agency TEXT")

        # Schemes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schemes (
                scheme_id TEXT,
                district_id TEXT,
                sr_no INTEGER, 
                block TEXT, 
                agency TEXT, 
                scheme_name TEXT, 
                has_tw2 BOOLEAN DEFAULT 0, 
                agency_submitted_date DATE, 
                tpia_verified_date DATE, 
                ee_verified_date DATE,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (scheme_id, district_id)
            )
        ''')
        
        # Components table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS components (
                component_id INTEGER PRIMARY KEY,
                component_name TEXT NOT NULL,
                component_group TEXT NOT NULL,
                site_type TEXT NOT NULL, 
                entry_type TEXT NOT NULL, 
                unit TEXT 
            )
        ''')
        
        # Progress table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS progress (
                progress_id INTEGER PRIMARY KEY,
                district_id TEXT NOT NULL,
                scheme_id TEXT NOT NULL,
                component_id INTEGER NOT NULL,
                target_value REAL,
                achieved_value REAL,
                progress_percent REAL,
                days_remaining INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(district_id, scheme_id, component_id)
            )
        ''')
        
        # Issues table - simplified
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS issues (
                issue_id INTEGER PRIMARY KEY,
                district_id TEXT NOT NULL,
                scheme_id TEXT NOT NULL,
                component_id INTEGER NOT NULL,
                issue_category TEXT NOT NULL,
                issue_description TEXT NOT NULL,
                severity TEXT NOT NULL,
                reported_by TEXT,
                expected_resolution_date DATE,
                is_resolved BOOLEAN DEFAULT 0,
                reported_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # WhatsApp contacts - simplified
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS whatsapp_contacts (
                contact_id INTEGER PRIMARY KEY,
                district_id TEXT NOT NULL,
                contact_name TEXT NOT NULL,
                contact_role TEXT NOT NULL,
                phone_number TEXT NOT NULL,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        
        # Create default admin
        cursor.execute("SELECT COUNT(*) FROM admin_users")
        if cursor.fetchone()[0] == 0:
            admin_id = secrets.token_urlsafe(16)
            admin_password_hash = hash_password("admin123")
            cursor.execute('''
                INSERT INTO admin_users (admin_id, username, password_hash, full_name)
                VALUES (?, ?, ?, ?)
            ''', (admin_id, "admin", admin_password_hash, "System Administrator"))
        
        # Create and populate delay settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS delay_settings (
                setting_name TEXT PRIMARY KEY,
                delay_days INTEGER NOT NULL
            )
        ''')
        
        cursor.execute("SELECT COUNT(*) FROM delay_settings")
        if cursor.fetchone()[0] == 0:
            default_settings = [
                ('critical_issues', 14), ('high_issues', 7),
                ('material_issues', 10), ('payment_issues', 21),
                ('contractor_issues', 14)
            ]
            cursor.executemany('INSERT INTO delay_settings (setting_name, delay_days) VALUES (?, ?)', default_settings)

        conn.commit()

def load_default_components():
    """Load essential components"""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM components")
        if cursor.fetchone()[0] == 0:
            components_list = [
                ('Distribution Line', 'Distribution Line', 'main', 'metric', 'Km'), ('FHTC Installation', 'Distribution Line', 'main', 'metric', 'Nos.'),
                ('Grouting of FHTC', 'Distribution Line', 'main', 'task', '%'), ('Stand Posts Installation', 'Distribution Line', 'main', 'metric', 'Nos.'),
                ('Sluice Valves - Masonry Type', 'Distribution Line', 'main', 'metric', 'Nos.'), ('Sluice Valves - Surface Box Type', 'Distribution Line', 'main', 'metric', 'Nos.'),
                ('Air Valves', 'Distribution Line', 'main', 'metric', 'Nos.'), ('Pressure Release Valves', 'Distribution Line', 'main', 'metric', 'Nos.'),
                ('Scour Valves', 'Distribution Line', 'main', 'metric', 'Nos.'), ('Fire Hydrant Valves', 'Distribution Line', 'main', 'metric', 'Nos.'),
                ('OHT Structure', 'OHT', 'main', 'task', '%'), ('OHT Staircase', 'OHT', 'main', 'task', '%'),
                ('Tank Installation/Dome Completion', 'OHT', 'main', 'task', '%'), ('Structure/Tank Painting', 'OHT', 'main', 'task', '%'),
                ('SWSM Logo on Tank', 'OHT', 'main', 'task', '%'), ('Lightning Arrester Installation', 'OHT', 'main', 'task', '%'),
                ('Lightning Arrester Earthing', 'OHT', 'main', 'task', '%'), ('Railing Installation', 'OHT', 'main', 'task', '%'),
                ('Railing Painting', 'OHT', 'main', 'task', '%'), ('Vertical DI/DF Installation', 'OHT', 'main', 'task', '%'),
                ('Inlet Pipe Connected to TW', 'OHT', 'main', 'task', '%'), ('Outlet Pipe Connected to DL', 'OHT', 'main', 'task', '%'),
                ('Inlet Valve', 'OHT', 'main', 'metric', 'Nos.'), ('Outlet Valve', 'OHT', 'main', 'metric', 'Nos.'),
                ('Direct Supply Valve', 'OHT', 'main', 'metric', 'Nos.'), ('Washout Valve', 'OHT', 'main', 'metric', 'Nos.'),
                ('Apron', 'OHT', 'main', 'task', '%'), ('Flooring', 'OHT', 'main', 'task', '%'),
                ('Submersible Pump Capacity', 'Pump House', 'main', 'metric', 'HP'), ('Water Discharge', 'Pump House', 'main', 'metric', 'LPM'),
                ('Pump House Construction', 'Pump House', 'main', 'task', '%'), ('Bypass Chamber Construction', 'Pump House', 'main', 'task', '%'),
                ('Plinth Protection', 'Pump House', 'main', 'task', '%'), ('Pump House Painting', 'Pump House', 'main', 'task', '%'),
                ('Doors Installed', 'Pump House', 'main', 'task', '%'), ('Doors Painting', 'Pump House', 'main', 'task', '%'),
                ('Windows Installed', 'Pump House', 'main', 'task', '%'), ('Windows Painting', 'Pump House', 'main', 'task', '%'),
                ('Girder Installation', 'Pump House', 'main', 'task', '%'), ('Internal Cabling', 'Pump House', 'main', 'task', '%'),
                ('Cable Trays Installed', 'Pump House', 'main', 'task', '%'), ('Internal & External Lighting', 'Pump House', 'main', 'task', '%'),
                ('DG Capacity', 'Pump House', 'main', 'metric', 'KVA'), ('DG Foundation', 'Pump House', 'main', 'task', '%'),
                ('DG Installed on Foundation', 'Pump House', 'main', 'task', '%'), ('DG Earthing', 'Pump House', 'main', 'task', '%'),
                ('DG Connected to RTU Panel', 'Pump House', 'main', 'task', '%'), ('DI Piping to Bypass Chamber', 'Pump House', 'main', 'task', '%'),
                ('RTU Panel Installed', 'Pump House', 'main', 'task', '%'), ('RTU Panel Earthing', 'Pump House', 'main', 'task', '%'),
                ('Chlorine Dosing System Installed', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Chlorine Dosing System Functional', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Hypo Chlorine', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Chlorination Sensors', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Chlorine System Connected to RTU', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Hydrostatic Level Sensor Installed', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Level Sensor at 25.5M in TW', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Level Sensor Connected to RTU', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Pressure Transmitter Installed', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Pressure Transmitter Connected to RTU', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Inlet Flowmeter Installed', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Inlet Flowmeter Connected to RTU', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Outlet Flowmeter Installed', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Outlet Flowmeter Connected to RTU', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Actuator Valves Installed', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Actuator Valves Connected to RTU', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Radar Level Sensor Installed', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Radar Level Sensor Connected to RTU', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Automation Software Installed', 'Sensors & Instrumentation', 'main', 'task', '%'), ('Automation Software Tested', 'Sensors & Instrumentation', 'main', 'task', '%'),
                ('Boundary Wall Length', 'Boundary Wall', 'main', 'metric', 'Meters'), ('Boundary Wall Painting', 'Boundary Wall', 'main', 'task', '%'),
                ('Main Gate Installed', 'Boundary Wall', 'main', 'task', '%'), ('Main Gate Painting', 'Boundary Wall', 'main', 'task', '%'),
                ('Wicket Gate Installed', 'Boundary Wall', 'main', 'task', '%'), ('Wicket Gate Painting', 'Boundary Wall', 'main', 'task', '%'),
                ('Solar Structure Installed', 'Solar Plant', 'main', 'task', '%'), ('Solar Panels Installed', 'Solar Plant', 'main', 'task', '%'),
                ('Solar Panels Alignment', 'Solar Plant', 'main', 'task', '%'), ('Solar Cabling', 'Solar Plant', 'main', 'task', '%'),
                ('Solar Cable Connected to RTU', 'Solar Plant', 'main', 'task', '%'), ('Solar Earthing', 'Solar Plant', 'main', 'task', '%'),
                ('Solar Lightning Arrester', 'Solar Plant', 'main', 'task', '%'), ('Solar Interlocking Constructed', 'Solar Plant', 'main', 'task', '%'),
                ('Interlocking Road Constructed', 'Campus Development', 'main', 'task', '%'), ('Interlocking Road Area', 'Campus Development', 'main', 'metric', 'Sq.M'),
                ('Recharge Pit Constructed', 'Campus Development', 'main', 'task', '%'), ('Solar Street Lights Installed', 'Campus Development', 'main', 'task', '%'),
                ('Solar Street Lights Count', 'Campus Development', 'main', 'metric', 'Nos.'), ('Landscaping', 'Campus Development', 'main', 'task', '%'),
                ('Campus Leveled', 'Campus Development', 'main', 'task', '%'), ('Campus Free of Debris', 'Campus Development', 'main', 'task', '%'),
                ('Sign Board', 'Campus Development', 'main', 'task', '%'), ('OHT Drain Constructed', 'Campus Development', 'main', 'task', '%'),
                ('OHT Drain Connected to Recharge Pit', 'Campus Development', 'main', 'task', '%'), ('Bypass Chamber Drain Constructed', 'Campus Development', 'main', 'task', '%'),
                ('Bypass Drain Connected to Recharge Pit', 'Campus Development', 'main', 'task', '%'), ('HGJ Certification', 'Final Certification', 'main', 'metric', 'Villages'),
                ('Road Restoration Certificate', 'Final Certification', 'main', 'metric', 'Villages'), ('Submersible Pump Capacity', 'Pump House TW-2', 'tw2', 'metric', 'HP'),
                ('Water Discharge', 'Pump House TW-2', 'tw2', 'metric', 'LPM'), ('Pump House Construction', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Bypass Chamber Construction', 'Pump House TW-2', 'tw2', 'task', '%'), ('Plinth Protection', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Pump House Painting', 'Pump House TW-2', 'tw2', 'task', '%'), ('Doors Installed', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Doors Painting', 'Pump House TW-2', 'tw2', 'task', '%'), ('Windows Installed', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Windows Painting', 'Pump House TW-2', 'tw2', 'task', '%'), ('Girder Installation', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Internal Cabling', 'Pump House TW-2', 'tw2', 'task', '%'), ('Cable Trays Installed', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Internal & External Lighting', 'Pump House TW-2', 'tw2', 'task', '%'), ('DG Capacity', 'Pump House TW-2', 'tw2', 'metric', 'KVA'),
                ('DG Foundation', 'Pump House TW-2', 'tw2', 'task', '%'), ('DG Installed on Foundation', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('DG Earthing', 'Pump House TW-2', 'tw2', 'task', '%'), ('DG Connected to RTU Panel', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('DI Piping to Bypass Chamber', 'Pump House TW-2', 'tw2', 'task', '%'), ('RTU Panel Installed', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('RTU Panel Earthing', 'Pump House TW-2', 'tw2', 'task', '%'), ('Chlorine Dosing System Installed', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Chlorine Dosing System Functional', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Hypo Chlorine', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Chlorination Sensors', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Chlorine System Connected to RTU', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Hydrostatic Level Sensor Installed', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Level Sensor at 25.5M in TW', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Level Sensor Connected to RTU', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Pressure Transmitter Installed', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Pressure Transmitter Connected to RTU', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Inlet Flowmeter Installed', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Inlet Flowmeter Connected to RTU', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Outlet Flowmeter Installed', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Outlet Flowmeter Connected to RTU', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Actuator Valves Installed', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Actuator Valves Connected to RTU', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Radar Level Sensor Installed', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Radar Level Sensor Connected to RTU', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Automation Software Installed', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'),
                ('Automation Software Tested', 'Sensors & Instrumentation TW-2', 'tw2', 'task', '%'), ('Boundary Wall Length', 'Boundary Wall TW-2', 'tw2', 'metric', 'Meters'),
                ('Boundary Wall Painting', 'Boundary Wall TW-2', 'tw2', 'task', '%'), ('Main Gate Installed', 'Boundary Wall TW-2', 'tw2', 'task', '%'),
                ('Main Gate Painting', 'Boundary Wall TW-2', 'tw2', 'task', '%'), ('Wicket Gate Installed', 'Boundary Wall TW-2', 'tw2', 'task', '%'),
                ('Wicket Gate Painting', 'Boundary Wall TW-2', 'tw2', 'task', '%'), ('Solar Structure Installed', 'Solar Plant TW-2', 'tw2', 'task', '%'),
                ('Solar Panels Installed', 'Solar Plant TW-2', 'tw2', 'task', '%'), ('Solar Panels Alignment', 'Solar Plant TW-2', 'tw2', 'task', '%'),
                ('Solar Cabling', 'Solar Plant TW-2', 'tw2', 'task', '%'), ('Solar Cable Connected to RTU', 'Solar Plant TW-2', 'tw2', 'task', '%'),
                ('Solar Earthing', 'Solar Plant TW-2', 'tw2', 'task', '%'), ('Solar Lightning Arrester', 'Solar Plant TW-2', 'tw2', 'task', '%'),
                ('Solar Interlocking Constructed', 'Solar Plant TW-2', 'tw2', 'task', '%'), ('Interlocking Road Constructed', 'Campus Development TW-2', 'tw2', 'task', '%'),
                ('Interlocking Road Area', 'Campus Development TW-2', 'tw2', 'metric', 'Sq.M'), ('Recharge Pit Constructed', 'Campus Development TW-2', 'tw2', 'task', '%'),
                ('Solar Street Lights Installed', 'Campus Development TW-2', 'tw2', 'task', '%'), ('Solar Street Lights Count', 'Campus Development TW-2', 'tw2', 'metric', 'Nos.'),
                ('Landscaping', 'Campus Development TW-2', 'tw2', 'task', '%'), ('Campus Leveled', 'Campus Development TW-2', 'tw2', 'task', '%'),
                ('Campus Free of Debris', 'Campus Development TW-2', 'tw2', 'task', '%'), ('Sign Board', 'Campus Development TW-2', 'tw2', 'task', '%'),
                ('OHT Drain Constructed', 'Campus Development TW-2', 'tw2', 'task', '%'), ('OHT Drain Connected to Recharge Pit', 'Campus Development TW-2', 'tw2', 'task', '%'),
                ('Bypass Chamber Drain Constructed', 'Campus Development TW-2', 'tw2', 'task', '%'), ('Bypass Drain Connected to Recharge Pit', 'Campus Development TW-2', 'tw2', 'task', '%'),
            ]
            cursor.executemany('INSERT INTO components (component_name, component_group, site_type, entry_type, unit) VALUES (?, ?, ?, ?, ?)', components_list)
            conn.commit()

# --- Enhanced Helper Functions ---

def parse_assigned_blocks(assigned_block):
    """Parse comma-separated block assignments into a clean list"""
    if not assigned_block or assigned_block.upper() == "ALL":
        return []
    return [block.strip().upper() for block in assigned_block.split(',') if block.strip()]

def get_scheme_count_for_assignment(district_id, agency, blocks):
    """Get scheme count for a specific district, agency, and blocks combination"""
    with sqlite3.connect(DB_PATH) as conn:
        query = "SELECT COUNT(*) as count FROM schemes WHERE district_id = ?"
        params = [district_id]
        
        if agency and agency.upper() != "ALL":
            query += " AND UPPER(agency) = ?"
            params.append(agency.upper())
        
        if blocks:
            placeholders = ','.join('?' * len(blocks))
            query += f" AND UPPER(block) IN ({placeholders})"
            params.extend(blocks)
        
        result = pd.read_sql_query(query, conn, params=params)
        return result.iloc[0]['count'] if not result.empty else 0

def get_available_agencies_for_district(district_id):
    """Get list of available agencies in a district"""
    with sqlite3.connect(DB_PATH) as conn:
        result = pd.read_sql_query(
            "SELECT DISTINCT agency FROM schemes WHERE district_id = ? ORDER BY agency",
            conn, params=(district_id,)
        )
        return result['agency'].tolist() if not result.empty else []

def get_available_blocks_for_district(district_id):
    """Get list of available blocks in a district, standardized to uppercase."""
    with sqlite3.connect(DB_PATH) as conn:
        result = pd.read_sql_query(
            "SELECT DISTINCT UPPER(block) as block FROM schemes WHERE district_id = ? ORDER BY block",
            conn, params=(district_id,)
        )
        return result['block'].tolist() if not result.empty else []

def format_assignment_display(assigned_block, assigned_agency):
    """Format assignment for clear display"""
    if not assigned_agency or assigned_agency.upper() == "ALL":
        agency_text = "All Agencies"
    else:
        agency_text = assigned_agency
    
    if not assigned_block or assigned_block.upper() == "ALL":
        block_text = "All Blocks"
    else:
        blocks = parse_assigned_blocks(assigned_block)
        if len(blocks) == 1:
            block_text = blocks[0]
        else:
            block_text = f"{', '.join(blocks[:2])}{'...' if len(blocks) > 2 else ''}"
    
    return f"{agency_text} | {block_text}"

# --- Authentication ---

def authenticate_district_user(username, password):
    """Authenticate a district user with enhanced role-based filtering."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                u.user_id, u.full_name, u.username, u.password_hash, u.role, u.assigned_block, u.assigned_agency, u.is_active,
                d.district_id, d.district_name, d.district_code
            FROM district_users u
            JOIN districts d ON u.district_id = d.district_id
            WHERE u.username = ?
        ''', (username,))
        result = cursor.fetchone()
        
        if result and result['is_active'] and verify_password(password, result['password_hash']):
            return dict(result)
    return None

def authenticate_admin(username, password):
    """Authenticate admin user"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM admin_users WHERE username = ?', (username,))
        result = cursor.fetchone()
        
        if result and verify_password(password, result['password_hash']):
            return dict(result)
    return None

def check_user_has_agency_assignment(user_data):
    """Check if Engineer has proper agency assignment"""
    if user_data.get('role') == 'Engineer' and not user_data.get('assigned_agency'):
        return False
    return True

# --- Data Functions ---

def get_delay_settings():
    """Fetches delay settings from the database."""
    with sqlite3.connect(DB_PATH) as conn:
        settings_df = pd.read_sql_query("SELECT * FROM delay_settings", conn)
        defaults = {
            'critical_issues': 14, 'high_issues': 7,
            'material_issues': 10, 'payment_issues': 21,
            'contractor_issues': 14
        }
        if not settings_df.empty:
            settings = pd.Series(settings_df.delay_days.values, index=settings_df.setting_name).to_dict()
            defaults.update(settings)
        return defaults

def get_scheme_data_with_issues(user_data):
    """Get scheme data with progress and issue impact, filtered by user role, agency, and multiple blocks."""
    delay_penalties = get_delay_settings()
    
    query = "SELECT * FROM schemes"
    params = []
    
    role = user_data.get('role')
    district_id = user_data.get('district_id')
    assigned_block = user_data.get('assigned_block')
    assigned_agency = user_data.get('assigned_agency')
    
    # Build filtering conditions based on role
    conditions = []
    
    if role == 'Engineer':
        # Engineers see only their assigned district, blocks, and agency
        conditions.append("district_id = ?")
        params.append(district_id)
        
        # Handle multiple blocks assignment
        if assigned_block and assigned_block.upper() != "ALL":
            blocks = parse_assigned_blocks(assigned_block)
            if blocks:
                placeholders = ','.join('?' * len(blocks))
                conditions.append(f"UPPER(block) IN ({placeholders})")
                params.extend(blocks)
        
        # Handle agency assignment for Engineers
        if assigned_agency and assigned_agency.upper() != "ALL":
            conditions.append("UPPER(agency) = ?")
            params.append(assigned_agency.upper())
            
    elif role == 'Manager / Coordinator':
        # Managers see all agencies in their assigned district/blocks
        conditions.append("district_id = ?")
        params.append(district_id)
        
        if assigned_block and assigned_block.upper() != "ALL":
            blocks = parse_assigned_blocks(assigned_block)
            if blocks:
                placeholders = ','.join('?' * len(blocks))
                conditions.append(f"UPPER(block) IN ({placeholders})")
                params.extend(blocks)
    
    elif role == 'Corporate':
        # Corporate users see everything
        pass
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    with sqlite3.connect(DB_PATH) as conn:
        schemes_df = pd.read_sql_query(query, conn, params=params)
        if schemes_df.empty: 
            return pd.DataFrame()
        
        districts_to_query = tuple(schemes_df['district_id'].unique())
        if not districts_to_query:
            return schemes_df 
        
        placeholders = ','.join('?' * len(districts_to_query))
        
        progress_query = f"SELECT p.scheme_id, AVG(CASE c.entry_type WHEN 'metric' THEN (CAST(p.achieved_value AS REAL) / NULLIF(p.target_value, 0)) * 100 ELSE p.progress_percent END) as avg_progress, MAX(p.days_remaining) as max_days_remaining FROM progress p JOIN components c ON p.component_id = c.component_id WHERE p.district_id IN ({placeholders}) GROUP BY p.scheme_id"
        issues_query = f"SELECT scheme_id, COUNT(*) as total_issues, COUNT(CASE WHEN is_resolved = 0 THEN 1 END) as open_issues, COUNT(CASE WHEN severity = 'Critical' AND is_resolved = 0 THEN 1 END) as critical_issues, COUNT(CASE WHEN severity = 'High' AND is_resolved = 0 THEN 1 END) as high_issues, COUNT(CASE WHEN issue_category = 'Material not delivered' AND is_resolved = 0 THEN 1 END) as material_issues, COUNT(CASE WHEN issue_category = 'Payment issues' AND is_resolved = 0 THEN 1 END) as payment_issues, COUNT(CASE WHEN issue_category = 'Contractor not working' AND is_resolved = 0 THEN 1 END) as contractor_issues FROM issues WHERE district_id IN ({placeholders}) GROUP BY scheme_id"
        
        progress_df = pd.read_sql_query(progress_query, conn, params=districts_to_query)
        issues_df = pd.read_sql_query(issues_query, conn, params=districts_to_query)
        
        if not progress_df.empty:
            full_df = pd.merge(schemes_df, progress_df, on="scheme_id", how="left")
        else:
            full_df = schemes_df
            full_df['avg_progress'] = 0
            full_df['max_days_remaining'] = 0
        
        if not issues_df.empty:
            full_df = pd.merge(full_df, issues_df, on="scheme_id", how="left")
        
        issue_cols = ['total_issues', 'open_issues', 'critical_issues', 'high_issues', 'material_issues', 'payment_issues', 'contractor_issues']
        for col in issue_cols:
            if col not in full_df.columns:
                full_df[col] = 0
            else:
                full_df[col] = full_df[col].fillna(0).astype(int)
        
        full_df['avg_progress'] = full_df['avg_progress'].fillna(0).round(1)
        full_df['max_days_remaining'] = full_df['max_days_remaining'].fillna(0)
        
        full_df['issue_delay_days'] = (
            full_df['critical_issues'] * delay_penalties.get('critical_issues', 14) +
            full_df['high_issues'] * delay_penalties.get('high_issues', 7) +
            full_df['material_issues'] * delay_penalties.get('material_issues', 10) +
            full_df['payment_issues'] * delay_penalties.get('payment_issues', 21) +
            full_df['contractor_issues'] * delay_penalties.get('contractor_issues', 14)
        )
        
        full_df['adjusted_days_remaining'] = full_df['max_days_remaining'] + full_df['issue_delay_days']
        
        full_df['risk_level'] = full_df.apply(lambda row:
            'High Risk' if row['critical_issues'] > 0 or row['open_issues'] >= 3
            else 'Medium Risk' if row['open_issues'] > 0 
            else 'Low Risk', axis=1)
        
        conditions = [(full_df['ee_verified_date'].notna()), (full_df['avg_progress'] >= 100), (full_df['avg_progress'] > 0)]
        choices = ['In O&M', 'Ready for Inspection', 'In Progress']
        full_df['status'] = np.select(conditions, choices, default='Not Started')
        
        return full_df

def create_analytics_report(df, forecast_df):
    """Creates district-specific analytics report."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        summary_df = df['status'].value_counts().reset_index()
        summary_df.columns = ['Status', 'Number of Schemes']
        summary_df.to_excel(writer, sheet_name='Status Summary', index=False)
        
        if not forecast_df.empty:
            df_for_export = forecast_df.copy()
            for col in df_for_export.select_dtypes(include=['datetimetz']).columns:
                df_for_export[col] = df_for_export[col].dt.tz_localize(None)
            df_for_export.to_excel(writer, sheet_name='O&M Forecast', index=False)
        
        for status in ['Ready for Inspection', 'In Progress', 'In O&M']:
            status_df = df[df['status'] == status]
            if not status_df.empty:
                status_df.to_excel(writer, sheet_name=f'{status} List', index=False)
    
    output.seek(0)
    return output

def create_problem_report_excel(problem_schemes_df):
    """Creates an Excel report for problem schemes with detailed issues."""
    if problem_schemes_df.empty:
        return None

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        summary_cols = ['scheme_name', 'block', 'agency', 'avg_progress', 'open_issues', 'critical_issues', 'risk_level']
        problem_schemes_df[summary_cols].to_excel(writer, sheet_name='Problem Schemes Summary', index=False)

        problem_scheme_ids = tuple(problem_schemes_df['scheme_id'].unique())
        
        if problem_scheme_ids:
            with sqlite3.connect(DB_PATH) as conn:
                issues_query = f"SELECT s.scheme_name, c.component_name, i.issue_category, i.issue_description, i.severity, i.reported_by, i.reported_date FROM issues i JOIN schemes s ON i.scheme_id = s.scheme_id JOIN components c ON i.component_id = c.component_id WHERE i.scheme_id IN ({','.join(['?']*len(problem_scheme_ids))}) AND i.is_resolved = 0 ORDER BY s.scheme_name, i.reported_date DESC"
                issue_details_df = pd.read_sql_query(issues_query, conn, params=problem_scheme_ids)
                
                if not issue_details_df.empty:
                    issue_details_df['reported_date'] = pd.to_datetime(issue_details_df['reported_date'], format='mixed', errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
                    issue_details_df.to_excel(writer, sheet_name='All Issue Details', index=False)

    output.seek(0)
    return output

def create_issues_report_excel(issues_df):
    """Creates an Excel report for a given dataframe of issues."""
    if issues_df.empty:
        return None
    
    output = io.BytesIO()
    df_to_export = issues_df.copy()
    
    export_cols = {
        'scheme_name': 'Scheme', 'block': 'Block', 'component_name': 'Component', 'issue_category': 'Category',
        'issue_description': 'Description', 'severity': 'Severity', 'reported_by': 'Reported By',
        'reported_date': 'Reported Date', 'expected_resolution_date': 'Expected Resolution'
    }
    df_to_export = df_to_export[[col for col in export_cols if col in df_to_export.columns]].rename(columns=export_cols)
    
    if 'Reported Date' in df_to_export.columns:
        df_to_export['Reported Date'] = df_to_export['Reported Date'].dt.strftime('%d/%m/%Y %H:%M')
    if 'Expected Resolution' in df_to_export.columns:
        df_to_export['Expected Resolution'] = pd.to_datetime(df_to_export['Expected Resolution']).dt.strftime('%d/%m/%Y')
        
    df_to_export.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)
    return output

def create_whatsapp_summary_message(scheme_name, issues_df):
    """Creates a consolidated WhatsApp summary message for a scheme's issues."""
    message = f"🚨 *JJM SCHEME ALERT* 🚨\n\n"
    message += f"Summary of open issues for scheme: **{scheme_name}**\n\n"
    message += f"**Total Open Issues: {len(issues_df)}**\n\n"
    
    message += "*Key Problems:*\n"
    severity_order = {'Critical': 0, 'High': 1, 'Medium': 2, 'Low': 3}
    sorted_issues = issues_df.sort_values(by='severity', key=lambda s: s.map(severity_order))
    
    for i, issue in sorted_issues.head(3).iterrows():
        message += f"- *{issue['component_name']}* ({issue['severity']}): {issue['issue_description'][:40]}...\n"
    
    if len(sorted_issues) > 3:
        message += f"- ...and {len(sorted_issues) - 3} other issues.\n"
        
    message += "\nPlease review the dashboard for full details."
    return message

# --- UI Functions ---

def show_login_page():
    """Display login page"""
    st.title("🚰 Multi-District JJM O&M Tracker")
    st.markdown("---")
    
    login_type = st.radio("Login as:", ["District User / Engineer", "System Administrator"], horizontal=True)
    
    if login_type == "District User / Engineer":
        st.subheader("👤 District User Login")
        
        with st.form("district_user_login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit_button = st.form_submit_button("🔐 Login")
            
            if submit_button:
                if username and password:
                    user_data = authenticate_district_user(username, password)
                    if user_data:
                        # Check if Engineer has proper agency assignment
                        if not check_user_has_agency_assignment(user_data):
                            st.error("❌ Your account is missing agency assignment. Please contact your administrator to update your profile.")
                            return
                        
                        st.session_state.authenticated = True
                        st.session_state.user_type = "district_user"
                        st.session_state.user_data = user_data
                        st.success(f"Welcome, {user_data['full_name']}!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid username or password, or account is inactive.")
                else:
                    st.error("❌ Please enter both username and password.")

    else:
        st.subheader("👨‍💼 Administrator Login")
        
        with st.form("admin_login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit_button = st.form_submit_button("🔐 Admin Login")
            
            if submit_button and username and password:
                admin_data = authenticate_admin(username, password)
                if admin_data:
                    st.session_state.authenticated = True
                    st.session_state.user_type = "admin"
                    st.session_state.admin_data = admin_data
                    st.success(f"Welcome, {admin_data['full_name']}!")
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials!")
    
    st.markdown("---")
    st.subheader("📞 Need Help?")
    st.info("Contact your System Administrator for login credentials.")

def show_whatsapp_sender(message_content, district_id, key_prefix=""):
    """Displays a UI to select contacts and send a pre-formatted WhatsApp message."""
    st.markdown("---")
    st.subheader(f"📱 Send WhatsApp Notification")

    with sqlite3.connect(DB_PATH) as conn:
        contacts_df = pd.read_sql_query(
            "SELECT contact_name, phone_number, contact_role FROM whatsapp_contacts WHERE district_id = ? AND is_active = 1",
            conn, params=(district_id,)
        )

    if contacts_df.empty:
        st.warning("No WhatsApp contacts found. Please ask your manager to add contacts in the 'WhatsApp Contacts' page.")
        return

    contact_options = {f"{row['contact_name']} ({row['contact_role']})": row['phone_number'] for _, row in contacts_df.iterrows()}
    selected_contacts = st.multiselect("Select recipients:", options=list(contact_options.keys()), key=f"{key_prefix}_whatsapp_recipients")

    if selected_contacts:
        for contact_display_name in selected_contacts:
            phone_number = contact_options[contact_display_name]
            recipient_name = contact_display_name.split(' (')[0]
            
            final_message = f"Hello {recipient_name.split(' ')[0]},\n\n{message_content}"
            whatsapp_link = f"https://wa.me/{phone_number}?text={quote(final_message)}"
            st.markdown(f"Click to send to **{contact_display_name}**: [📱 Send]({whatsapp_link})")

def show_email_sender(report_generator, report_data, file_name, subject, key_prefix=""):
    """Displays a UI to send a report via email."""
    st.markdown("---")
    st.subheader(f"📧 Share Report via Email")
    
    recipient_email = st.text_input("Recipient Email Address(es), comma-separated", key=f"{key_prefix}_email_recipient")
    
    if recipient_email:
        excel_data = report_generator(report_data)
        
        if excel_data:
            st.download_button(
                label="1. Download the Report File",
                data=excel_data,
                file_name=file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            body = f"""
Dear Team,

Please find the requested JJM report attached.

Report: {subject}

(This email was prepared by the JJM O&M Tracker.)
"""
            mailto_link = f"mailto:{recipient_email}?subject={quote(subject)}&body={quote(body)}"
            st.markdown(f"**2. <a href='{mailto_link}' target='_blank'>Click here to open a pre-filled draft in your email app</a>**, then attach the downloaded file.", unsafe_allow_html=True)
        else:
            st.warning("Could not generate the report file.")

def show_dashboard(user_data):
    """Main dashboard, filtered by user role with enhanced assignment display."""
    st.title(f"🏠 {user_data['district_name']} - Dashboard")
    
    # Show detailed assignment info for Engineers
    if user_data.get('role') == 'Engineer':
        assigned_blocks = parse_assigned_blocks(user_data.get('assigned_block'))
        assigned_agency = user_data.get('assigned_agency', 'N/A')
        
        if assigned_blocks:
            block_display = ', '.join(assigned_blocks)
        else:
            block_display = user_data.get('assigned_block', 'N/A')
        
        # Get scheme count for Engineer's assignment
        scheme_count = get_scheme_count_for_assignment(
            user_data['district_id'], 
            assigned_agency, 
            assigned_blocks
        )
        
        st.info(f"**Your Assignment:** {assigned_agency} | {block_display} | **{scheme_count} schemes**")
    
    df = get_scheme_data_with_issues(user_data)
    
    if not df.empty:
        status_counts = df['status'].value_counts()
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Schemes", len(df))
        col2.metric("Ready for Inspection", status_counts.get('Ready for Inspection', 0))
        col3.metric("In Progress", status_counts.get('In Progress', 0))
        col4.metric("In O&M", status_counts.get('In O&M', 0))
        
        st.subheader("⚠️ Risk Overview")
        risk_counts = df['risk_level'].value_counts()
        col1, col2, col3 = st.columns(3)
        col1.metric("🔴 High Risk", risk_counts.get('High Risk', 0))
        col2.metric("🟡 Medium Risk", risk_counts.get('Medium Risk', 0))
        col3.metric("🟢 Low Risk", risk_counts.get('Low Risk', 0))
        
        st.subheader("📊 Schemes Overview")
        display_df = df[['scheme_name', 'block', 'agency', 'avg_progress', 'open_issues', 'risk_level', 'status']].copy()
        display_df.columns = ['Scheme Name', 'Block', 'Agency', 'Progress %', 'Open Issues', 'Risk Level', 'Status']
        
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No schemes found for your assigned scope. Please contact your administrator.")

def show_progress_entry(user_data):
    """Enhanced progress entry with issue reporting and multi-block support"""
    district_id = user_data['district_id']
    district_name = user_data['district_name']
    reporter_name = user_data['full_name']
    role = user_data.get('role')
    assigned_block = user_data.get('assigned_block')
    assigned_agency = user_data.get('assigned_agency')

    st.title(f"✏️ {district_name} - Progress Entry")
    
    # Build query with role-based filtering supporting multiple blocks
    with sqlite3.connect(DB_PATH) as conn:
        query = "SELECT scheme_id, scheme_name, block, agency, has_tw2 FROM schemes WHERE district_id = ?"
        params = [district_id]
        
        if role == 'Engineer':
            # Handle multiple blocks assignment for Engineers
            if assigned_block and assigned_block.upper() != "ALL":
                blocks = parse_assigned_blocks(assigned_block)
                if blocks:
                    placeholders = ','.join('?' * len(blocks))
                    query += f" AND UPPER(block) IN ({placeholders})"
                    params.extend(blocks)
            
            if assigned_agency and assigned_agency.upper() != "ALL":
                query += " AND UPPER(agency) = ?"
                params.append(assigned_agency.upper())
                
        elif role == 'Manager / Coordinator':
            if assigned_block and assigned_block.upper() != "ALL":
                blocks = parse_assigned_blocks(assigned_block)
                if blocks:
                    placeholders = ','.join('?' * len(blocks))
                    query += f" AND UPPER(block) IN ({placeholders})"
                    params.extend(blocks)
        
        # Show detailed assignment info for Engineers
        if role == 'Engineer':
            assigned_blocks = parse_assigned_blocks(assigned_block)
            scheme_count = get_scheme_count_for_assignment(district_id, assigned_agency, assigned_blocks)
            
            if assigned_blocks:
                block_display = ', '.join(assigned_blocks)
            else:
                block_display = assigned_block or 'N/A'
                
            st.info(f"**Your Assignment:** {assigned_agency or 'N/A'} | {block_display} | **{scheme_count} schemes**")

        schemes_df = pd.read_sql_query(query, conn, params=params)
    
    if not schemes_df.empty:
        scheme_options = { f"{row['scheme_name']} ({row['block']}) - {row['agency']}": row['scheme_id'] for _, row in schemes_df.iterrows() }
        selected_display = st.selectbox("Select Scheme:", list(scheme_options.keys()))
        if selected_display:
            selected_scheme_id = scheme_options[selected_display]
            scheme_info = schemes_df[schemes_df['scheme_id'] == selected_scheme_id].iloc[0]
            
            site_type = "main"
            if scheme_info['has_tw2']:
                selected_site_name = st.radio("Choose site:", ["Main Site", "TW-2 Site"], horizontal=True)
                site_type = "tw2" if selected_site_name == "TW-2 Site" else "main"
            
            st.subheader(f"📋 {scheme_info['scheme_name']} - {site_type.upper()} Site")
            
            with sqlite3.connect(DB_PATH) as conn:
                components_df = pd.read_sql_query( "SELECT * FROM components WHERE site_type = ?", conn, params=(site_type,))
                progress_df = pd.read_sql_query("SELECT * FROM progress WHERE scheme_id = ? AND district_id = ?", conn, params=(selected_scheme_id, district_id))
            
            component_groups = components_df['component_group'].unique().tolist()
            if not component_groups:
                st.warning("No components found for this site type.")
                return
            tabs = st.tabs(component_groups)
            
            for i, group_name in enumerate(component_groups):
                with tabs[i]:
                    with st.form(key=f"form_{group_name}_{site_type}"):
                        st.markdown(f"### {group_name}")
                        
                        updates = []
                        issues = []
                        group_comps = components_df[components_df['component_group'] == group_name]
                        
                        for _, comp in group_comps.iterrows():
                            current_progress = progress_df[progress_df['component_id'] == comp['component_id']]
                            
                            st.markdown(f"**🔧 {comp['component_name']}**")
                            col1, col2 = st.columns([3, 1])
                            
                            with col1:
                                if comp['entry_type'] == 'metric':
                                    c1, c2, c3, c4 = st.columns([2, 2, 1, 2])
                                    target = c1.number_input(f"Target ({comp['unit']})", value=float(current_progress['target_value'].iloc[0] or 0) if not current_progress.empty else 0.0, key=f"target_{comp['component_id']}")
                                    achieved = c2.number_input(f"Achieved ({comp['unit']})", value=float(current_progress['achieved_value'].iloc[0] or 0) if not current_progress.empty else 0.0, key=f"achieved_{comp['component_id']}")
                                    progress_val = (achieved / target * 100) if target > 0 else 0
                                    c3.metric("Progress", f"{progress_val:.1f}%")
                                    days = c4.number_input("Days Left", value=int(current_progress['days_remaining'].iloc[0] or 0) if not current_progress.empty else 0, key=f"days_m_{comp['component_id']}")
                                    updates.append({'type': 'metric', 'comp_id': comp['component_id'], 'target': target, 'achieved': achieved, 'days': days})
                                
                                else:
                                    c1, c2 = st.columns([3, 2])
                                    progress_percent = c1.slider("Progress %", 0, 100, int(current_progress['progress_percent'].iloc[0] or 0) if not current_progress.empty else 0, key=f"prog_{comp['component_id']}")
                                    days = c2.number_input("Days Left", value=int(current_progress['days_remaining'].iloc[0] or 0) if not current_progress.empty else 0, key=f"days_t_{comp['component_id']}")
                                    updates.append({'type': 'task', 'comp_id': comp['component_id'], 'percent': progress_percent, 'days': days})

                            with col2:
                                st.markdown("**🚨 Report Issue (Optional)**")
                                issue_categories = ["Material not delivered", "Contractor not working", "Payment issues", "Equipment problems", "Weather delays", "Quality issues", "Approval delays", "Other"]
                                issue_category = st.selectbox("Issue Type", issue_categories, key=f"cat_{comp['component_id']}")
                                issue_description = st.text_area("Issue Details", placeholder="Describe the issue to log it...", key=f"desc_{comp['component_id']}", height=70)
                                severity = st.selectbox("Severity", ["Low", "Medium", "High", "Critical"], index=1, key=f"sev_{comp['component_id']}")
                                
                                if issue_description and issue_description.strip():
                                    issues.append({
                                        'component_id': comp['component_id'], 'category': issue_category,
                                        'description': issue_description, 'severity': severity,
                                        'days_remaining': days,
                                    })
                            st.markdown("---")
                        
                        if st.form_submit_button(f"💾 Save {group_name}", type="primary"):
                            with sqlite3.connect(DB_PATH) as conn:
                                for u in updates:
                                    if u['type'] == 'metric':
                                        conn.execute('INSERT OR REPLACE INTO progress (district_id, scheme_id, component_id, target_value, achieved_value, days_remaining, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?)',
                                                     (district_id, selected_scheme_id, u['comp_id'], u['target'], u['achieved'], u['days'], datetime.now(IST)))
                                    else:
                                        conn.execute('INSERT OR REPLACE INTO progress (district_id, scheme_id, component_id, progress_percent, days_remaining, last_updated) VALUES (?, ?, ?, ?, ?, ?)',
                                                     (district_id, selected_scheme_id, u['comp_id'], u['percent'], u['days'], datetime.now(IST)))
                                
                                for issue in issues:
                                    resolution_days = issue.get('days_remaining', 7)
                                    resolution_date = datetime.now(IST).date() + timedelta(days=resolution_days)
                                    conn.execute('INSERT INTO issues (district_id, scheme_id, component_id, issue_category, issue_description, severity, reported_by, expected_resolution_date, reported_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                                 (district_id, selected_scheme_id, issue['component_id'], issue['category'], issue['description'], issue['severity'], reporter_name, resolution_date, datetime.now(IST)))
                                conn.commit()
                            
                            st.success(f"✅ {group_name} progress and any new issues have been saved!")
                            st.rerun()
    else:
        st.info("No schemes found for your assigned scope. Please contact your administrator or import schemes for this scope.")

def show_analytics(user_data):
    """Analytics page for district with multi-block support"""
    district_id = user_data['district_id']
    district_name = user_data['district_name']
    st.title(f"📈 {district_name} - Smart Analytics")
    
    df = get_scheme_data_with_issues(user_data)
    if df.empty:
        st.warning("No data available to analyze. Please import schemes and enter progress.")
        return
    
    col1, col2, col3 = st.columns([2, 2, 1])
    blocks = ["All Blocks"] + sorted(df['block'].unique().tolist())
    selected_block = col1.selectbox("Filter by Block", blocks)
    
    if selected_block != "All Blocks":
        df = df[df['block'] == selected_block].copy()
    
    agencies = ["All Agencies"] + sorted(df['agency'].unique().tolist())
    selected_agency = col2.selectbox("Filter by Agency", agencies)
    
    if selected_agency != "All Agencies":
        df = df[df['agency'] == selected_agency].copy()
    
    buffer_days = col3.number_input("Verification Buffer (days)", 0, 60, 20, 1)
    
    tab1, tab2, tab3 = st.tabs(["Smart Forecast", "Issue Impact Analysis", "Charts & Visuals"])
    
    forecast_df = df[df['status'].isin(['In Progress', 'Ready for Inspection'])].copy()
    
    if not forecast_df.empty:
        today = datetime.now(IST)
        forecast_df['physical_completion_date'] = forecast_df['adjusted_days_remaining'].apply(lambda d: today + timedelta(days=d))
        forecast_df['forecasted_om_date'] = forecast_df['physical_completion_date'] + timedelta(days=buffer_days)
        forecast_df['issue_delay_impact'] = forecast_df['adjusted_days_remaining'] - forecast_df['max_days_remaining']
    
    with tab1:
        st.subheader("🎯 Smart Forecast (Issue-Adjusted)")
        
        if not forecast_df.empty:
            total_delay = forecast_df['issue_delay_impact'].sum()
            schemes_with_issues = len(forecast_df[forecast_df['open_issues'] > 0])
            
            col1, col2, col3 = st.columns(3)
            delay_penalties = get_delay_settings()
            help_text = f"""This is the sum of all issue-related delays across all schemes.
The delay for each issue is calculated based on its severity and category using the current settings:
- High Severity: +{delay_penalties['high_issues']} days
- Critical Severity: +{delay_penalties['critical_issues']} days
- Payment Issue: +{delay_penalties['payment_issues']} days
- Contractor Issue: +{delay_penalties['contractor_issues']} days
- Material Issue: +{delay_penalties['material_issues']} days
"""
            col1.metric("Total Issue Delay", f"{total_delay} days", help=help_text)
            col2.metric("Schemes with Issues", schemes_with_issues)
            col3.metric("Avg Delay per Scheme", f"{total_delay/len(forecast_df):.1f} days")
            
            st.markdown("---")
            
            display_cols = ['scheme_name', 'block', 'agency', 'avg_progress', 'open_issues', 'risk_level', 'max_days_remaining', 'issue_delay_impact', 'adjusted_days_remaining', 'physical_completion_date', 'forecasted_om_date']
            forecast_display = forecast_df[display_cols].copy()
            forecast_display.columns = ['Scheme Name', 'Block', 'Agency', 'Progress %', 'Open Issues', 'Risk Level', 'Original Days', 'Issue Delay', 'Adjusted Days', 'Est. Completion', 'Forecasted O&M Start']
            
            forecast_display['Est. Completion'] = pd.to_datetime(forecast_display['Est. Completion']).dt.strftime('%d/%m/%Y')
            forecast_display['Forecasted O&M Start'] = pd.to_datetime(forecast_display['Forecasted O&M Start']).dt.strftime('%d/%m/%Y')

            st.dataframe(forecast_display, use_container_width=True)
        else:
            st.info("No schemes are currently in progress to forecast.")
    
    with tab2:
        st.subheader("🚨 Issue Impact Analysis")
        
        total_schemes = len(df)
        schemes_with_issues = len(df[df['open_issues'] > 0])
        critical_schemes = len(df[df['critical_issues'] > 0])
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Schemes with Issues", f"{schemes_with_issues}/{total_schemes}")
        col2.metric("Critical Issue Schemes", critical_schemes)
        col3.metric("Issue-free Schemes", total_schemes - schemes_with_issues)
        
        if schemes_with_issues > 0:
            st.markdown("### 📊 Issue Type Impact")
            issue_impact = {
                'Material Issues': df['material_issues'].sum(),
                'Payment Issues': df['payment_issues'].sum(), 
                'Contractor Issues': df['contractor_issues'].sum(),
                'Critical Issues': df['critical_issues'].sum(),
                'High Priority Issues': df['high_issues'].sum()
            }
            for issue_type, count in issue_impact.items():
                if count > 0:
                    st.markdown(f"**{issue_type}:** {count} schemes affected")
            
            st.markdown("### ⚠️ Risk Distribution")
            risk_df = df['risk_level'].value_counts().reset_index()
            risk_df.columns = ['Risk Level', 'Number of Schemes']
            st.bar_chart(risk_df.set_index('Risk Level')['Number of Schemes'])
    
    with tab3:
        st.subheader("📊 Enhanced Visuals")
        
        fig1, ax1 = plt.subplots(figsize=(10, 6))
        risk_status_crosstab = pd.crosstab(df['risk_level'], df['status'])
        risk_status_crosstab.plot(kind='bar', ax=ax1, stacked=True)
        ax1.set_title(f"{district_name} - Risk Level vs Status")
        ax1.set_ylabel("Number of Schemes")
        ax1.tick_params(axis='x', rotation=45)
        plt.tight_layout()
        st.pyplot(fig1)
        
        if not forecast_df.empty:
            fig2, ax2 = plt.subplots(figsize=(12, 6))
            x_pos = range(len(forecast_df))
            ax2.bar([x - 0.2 for x in x_pos], forecast_df['max_days_remaining'], 0.4, label='Original Timeline', alpha=0.7)
            ax2.bar([x + 0.2 for x in x_pos], forecast_df['adjusted_days_remaining'], 0.4, label='Issue-Adjusted Timeline', alpha=0.7)
            ax2.set_xlabel('Schemes')
            ax2.set_ylabel('Days Remaining')
            ax2.set_title('Timeline Impact of Issues')
            ax2.legend()
            ax2.set_xticks(x_pos)
            ax2.set_xticklabels([name[:15] + '...' if len(name) > 15 else name for name in forecast_df['scheme_name']], rotation=45)
            plt.tight_layout()
            st.pyplot(fig2)
    
    st.markdown("---")
    st.subheader("📥 Download Enhanced Analytics Report")
    excel_data = create_analytics_report(df, forecast_df[display_cols] if not forecast_df.empty else pd.DataFrame())
    st.download_button(
        label="📥 Download Smart Analytics Report (Excel)",
        data=excel_data,
        file_name=f"{district_name}_Smart_JJM_Analytics_{datetime.now(IST).strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def show_issues_dashboard(user_data):
    """Issues dashboard page for district with WhatsApp functionality for Engineers and multi-block support"""
    district_id = user_data['district_id']
    district_name = user_data['district_name']
    role = user_data.get('role')

    st.title(f"🚨 {district_name} - Issues Dashboard")
    
    # Build query with role-based filtering for issues supporting multiple blocks
    with sqlite3.connect(DB_PATH) as conn:
        base_query = '''
            SELECT i.*, s.scheme_name, s.block, s.agency, c.component_name, c.component_group
            FROM issues i
            JOIN schemes s ON i.scheme_id = s.scheme_id AND i.district_id = s.district_id
            JOIN components c ON i.component_id = c.component_id
            WHERE i.district_id = ?
        '''
        params = [district_id]
        
        # Apply role-based filtering for Engineers with multi-block support
        if role == 'Engineer':
            assigned_block = user_data.get('assigned_block')
            assigned_agency = user_data.get('assigned_agency')
            
            if assigned_block and assigned_block.upper() != "ALL":
                blocks = parse_assigned_blocks(assigned_block)
                if blocks:
                    placeholders = ','.join('?' * len(blocks))
                    base_query += f" AND UPPER(s.block) IN ({placeholders})"
                    params.extend(blocks)
            
            if assigned_agency and assigned_agency.upper() != "ALL":
                base_query += " AND UPPER(s.agency) = ?"
                params.append(assigned_agency.upper())
        
        elif role == 'Manager / Coordinator':
            assigned_block = user_data.get('assigned_block')
            if assigned_block and assigned_block.upper() != "ALL":
                blocks = parse_assigned_blocks(assigned_block)
                if blocks:
                    placeholders = ','.join('?' * len(blocks))
                    base_query += f" AND UPPER(s.block) IN ({placeholders})"
                    params.extend(blocks)
        
        base_query += " ORDER BY s.scheme_name, i.reported_date DESC"
        issues_df = pd.read_sql_query(base_query, conn, params=params)
    
    if issues_df.empty:
        st.info("No issues reported yet for your assigned scope. Issues will appear here when reported through Progress Entry.")
        return
    
    issues_df['reported_date'] = pd.to_datetime(issues_df['reported_date'], format='mixed', errors='coerce')
    issues_df['expected_resolution_date'] = pd.to_datetime(issues_df['expected_resolution_date'], errors='coerce')
    
    col1, col2, col3, col4 = st.columns(4)
    total_issues = len(issues_df)
    open_issues = len(issues_df[issues_df['is_resolved'] == 0])
    critical_issues = len(issues_df[(issues_df['severity'] == 'Critical') & (issues_df['is_resolved'] == 0)])
    resolved_issues = len(issues_df[issues_df['is_resolved'] == 1])
    
    col1.metric("Total Issues", total_issues)
    col2.metric("Open Issues", open_issues, delta=f"-{resolved_issues} resolved")
    col3.metric("Critical Open", critical_issues)
    col4.metric("Resolved Issues", resolved_issues)
    
    st.subheader("🔍 Filter Issues")
    col1, col2, col3 = st.columns(3)
    
    status_options = ["All", "Open", "Resolved"]
    selected_status = col1.selectbox("Status", status_options)
    
    severity_options = ["All"] + sorted(issues_df['severity'].unique().tolist())
    selected_severity = col2.selectbox("Severity", severity_options)
    
    category_options = ["All"] + sorted(issues_df['issue_category'].unique().tolist())
    selected_category = col3.selectbox("Issue Category", category_options)
    
    filtered_df = issues_df.copy()
    
    if selected_status == "Open":
        filtered_df = filtered_df[filtered_df['is_resolved'] == 0]
    elif selected_status == "Resolved":
        filtered_df = filtered_df[filtered_df['is_resolved'] == 1]
    
    if selected_severity != "All":
        filtered_df = filtered_df[filtered_df['severity'] == selected_severity]
    
    if selected_category != "All":
        filtered_df = filtered_df[filtered_df['issue_category'] == selected_category]
    
    st.subheader(f"📋 Issues List ({len(filtered_df)} issues)")

    st.markdown('<a href="#latest_issue">⬇️ Go to Latest Issue</a>', unsafe_allow_html=True)

    if not filtered_df.empty:
        for scheme_name, scheme_issues in filtered_df.groupby('scheme_name'):
            open_issue_count = len(scheme_issues[scheme_issues['is_resolved']==0])
            expander_title = f"**{scheme_name}** - {open_issue_count} Open Issue(s)"
            with st.expander(expander_title):
                
                if open_issue_count > 0:
                    if st.button("Send Scheme Summary Alert", key=f"notify_scheme_{scheme_name}"):
                        open_issues_df = scheme_issues[scheme_issues['is_resolved'] == 0]
                        summary_message = create_whatsapp_summary_message(scheme_name, open_issues_df)
                        st.session_state.message_to_send = summary_message
                        st.session_state.message_key_prefix = f"summary_{scheme_name}"
                
                if st.session_state.get('message_to_send') and st.session_state.get('message_key_prefix') == f"summary_{scheme_name}":
                    show_whatsapp_sender(st.session_state.message_to_send, district_id, key_prefix=st.session_state.message_key_prefix)

                for idx, issue in scheme_issues.iterrows():
                    severity_emoji = {'Critical': '🚨', 'High': '⚠️', 'Medium': '🔔', 'Low': '📢'}
                    status_emoji = '✅' if issue['is_resolved'] else '🔴'
                    
                    st.markdown(f"**{status_emoji} {severity_emoji.get(issue['severity'])} {issue['component_name']} ({issue['severity']})**")
                    st.write(f"**Description:** {issue['issue_description']} (Reported by: {issue['reported_by']} on {issue['reported_date'].strftime('%d/%m/%Y')})")

                    if st.button("Resolve", key=f"resolve_{issue['issue_id']}", disabled=issue['is_resolved']):
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.execute("UPDATE issues SET is_resolved = 1 WHERE issue_id = ?", (issue['issue_id'],))
                        st.success("Issue resolved!")
                        st.rerun()
                    st.markdown("---")

        st.markdown("<div id='latest_issue'></div>", unsafe_allow_html=True)
    
    else:
        st.info("No issues match the current filters.")

    st.markdown("---")
    st.subheader("📥 Download Filtered Issues List")
    excel_data = create_issues_report_excel(filtered_df)
    if excel_data:
        st.download_button(
            label="📥 Download Issues Report (Excel)",
            data=excel_data,
            file_name=f"{district_name}_Issues_Report_{datetime.now(IST).strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

def show_verification(user_data):
    """O&M Verification page for district with multi-block support"""
    district_id = user_data['district_id']
    district_name = user_data['district_name']
    st.title(f"📝 {district_name} - O&M Verification Tracking")
    
    df = get_scheme_data_with_issues(user_data)
    if not df.empty:
        df_to_edit = df[['scheme_id', 'scheme_name', 'block', 'agency', 'agency_submitted_date', 'tpia_verified_date', 'ee_verified_date']].copy()
        
        for col in ['agency_submitted_date', 'tpia_verified_date', 'ee_verified_date']:
            df_to_edit[col] = pd.to_datetime(df_to_edit[col], errors='coerce')
        
        edited_df = st.data_editor(
            df_to_edit, 
            key="verification_editor", 
            column_config={
                "scheme_id": st.column_config.TextColumn(disabled=True),
                "scheme_name": st.column_config.TextColumn(disabled=True),
                "agency_submitted_date": st.column_config.DateColumn("Agency Submitted", format="DD/MM/YYYY"),
                "tpia_verified_date": st.column_config.DateColumn("TPIA Verified", format="DD/MM/YYYY"),
                "ee_verified_date": st.column_config.DateColumn("EE Verified", format="DD/MM/YYYY")
            }, 
            use_container_width=True, 
            hide_index=True
        )
        
        if st.button("💾 Save Verification Dates", type="primary"):
            with sqlite3.connect(DB_PATH) as conn:
                for _, row in edited_df.iterrows():
                    agency_date = row['agency_submitted_date'].strftime('%Y-%m-%d') if pd.notna(row['agency_submitted_date']) else None
                    tpia_date = row['tpia_verified_date'].strftime('%Y-%m-%d') if pd.notna(row['tpia_verified_date']) else None
                    ee_date = row['ee_verified_date'].strftime('%Y-%m-%d') if pd.notna(row['ee_verified_date']) else None
                    
                    conn.execute('''
                        UPDATE schemes 
                        SET agency_submitted_date=?, tpia_verified_date=?, ee_verified_date=? 
                        WHERE scheme_id=? AND district_id=?
                    ''', (agency_date, tpia_date, ee_date, row['scheme_id'], district_id))
                conn.commit()
            st.success("Verification dates saved!")
            st.rerun()
    else:
        st.info("No schemes found to track verification.")

def show_problem_schemes(user_data):
    """Show schemes with progress vs timeline issues with WhatsApp functionality for Engineers and multi-block support"""
    district_id = user_data['district_id']
    district_name = user_data['district_name']
    role = user_data.get('role')
    st.title(f"🚨 {district_name} - Problem Schemes")
    
    df = get_scheme_data_with_issues(user_data)
    if df.empty:
        st.info("No scheme data available.")
        return
    
    problem_schemes = df[
        ((df['avg_progress'] >= 80) & (df['adjusted_days_remaining'] > 60)) |
        ((df['avg_progress'] >= 60) & (df['adjusted_days_remaining'] > 90)) |
        (df['adjusted_days_remaining'] > 120) |
        (df['critical_issues'] > 0) |
        (df['open_issues'] >= 3)
    ].copy()
    
    if problem_schemes.empty:
        st.success("🎉 No problematic schemes found! All schemes have realistic timelines and manageable issues.")
        return
    
    st.info(f"Found {len(problem_schemes)} schemes that require attention.")
    
    st.markdown('<a href="#latest_problem">⬇️ Go to Last Scheme</a>', unsafe_allow_html=True)

    for index, scheme in problem_schemes.iterrows():
        expander_title = f"**{scheme['scheme_name']}** (Block: {scheme['block']}) - {scheme['open_issues']} Open Issue(s)"
        with st.expander(expander_title):
            with sqlite3.connect(DB_PATH) as conn:
                issues_query = """
                    SELECT 
                        c.component_name, c.component_group, i.issue_category,
                        i.issue_description, i.severity, i.reported_by, i.reported_date
                    FROM issues i JOIN components c ON i.component_id = c.component_id
                    WHERE i.scheme_id = ? AND i.district_id = ? AND i.is_resolved = 0
                    ORDER BY c.component_group, i.reported_date DESC
                """
                scheme_issues_df = pd.read_sql_query(issues_query, conn, params=(scheme['scheme_id'], district_id))

            if not scheme_issues_df.empty:
                if st.button("Send Scheme Summary Alert", key=f"notify_prob_scheme_{scheme['scheme_id']}"):
                    summary_message = create_whatsapp_summary_message(scheme['scheme_name'], scheme_issues_df)
                    st.session_state.message_to_send = summary_message
                    st.session_state.message_key_prefix = f"summary_prob_{scheme['scheme_id']}"

                if st.session_state.get('message_to_send') and st.session_state.get('message_key_prefix') == f"summary_prob_{scheme['scheme_id']}":
                    show_whatsapp_sender(st.session_state.message_to_send, district_id, key_prefix=st.session_state.message_key_prefix)
                
                for component_group, issues in scheme_issues_df.groupby('component_group'):
                    st.markdown(f"#### Issues in: {component_group}")
                    for idx, issue in issues.iterrows():
                        st.info(f"- **Component:** {issue['component_name']} ({issue['severity']})\n- **Description:** {issue['issue_description']}")
            else:
                st.warning("This scheme is flagged as a problem, but no open issues were found. This might be due to a long timeline.")

    st.markdown("<div id='latest_problem'></div>", unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📥 Download Detailed Problem Report")
    
    excel_data = create_problem_report_excel(problem_schemes)
    if excel_data:
        st.download_button(
            label="📥 Download Problem Report (Excel)",
            data=excel_data,
            file_name=f"{district_name}_Problem_Schemes_Report_{datetime.now(IST).strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

def show_whatsapp_contacts(user_data):
    """WhatsApp contacts management - Only for Managers and above"""
    district_id = user_data['district_id']
    district_name = user_data['district_name']
    role = user_data.get('role')
    
    # Check if user has permission
    if role == 'Engineer':
        st.error("❌ Access Denied: Engineers cannot manage WhatsApp contacts. This feature is available only to Managers and above.")
        st.info("💡 **Note:** You can still send WhatsApp alerts from the Issues Dashboard and Problem Schemes pages.")
        return
    
    st.title(f"📱 {district_name} - WhatsApp Contacts")
    
    st.subheader("➕ Add New Contact")
    with st.form("add_contact_form"):
        col1, col2 = st.columns(2)
        contact_name = col1.text_input("Contact Name")
        contact_role = col2.text_input("Role/Designation")
        phone_number = st.text_input("WhatsApp Number (with country code)", placeholder="e.g., 919876543210")
        
        if st.form_submit_button("➕ Add Contact", type="primary"):
            if contact_name and contact_role and phone_number:
                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute('INSERT INTO whatsapp_contacts (district_id, contact_name, contact_role, phone_number) VALUES (?, ?, ?, ?)',
                                 (district_id, contact_name, contact_role, phone_number))
                    conn.commit()
                st.success(f"✅ Contact {contact_name} added successfully!")
                st.rerun()
            else:
                st.error("Please fill all fields.")
    
    with sqlite3.connect(DB_PATH) as conn:
        contacts_df = pd.read_sql_query("SELECT * FROM whatsapp_contacts WHERE district_id = ? AND is_active = 1", conn, params=(district_id,))
    
    if not contacts_df.empty:
        st.subheader("📋 Current Contacts")
        for _, contact in contacts_df.iterrows():
            col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
            col1.write(f"**{contact['contact_name']}**")
            col2.write(contact['contact_role'])
            col3.write(contact['phone_number'])
            test_message = f"Hello {contact['contact_name']}, this is a test message from JJM O&M Tracker for {district_name}."
            whatsapp_link = f"https://wa.me/{contact['phone_number']}?text={quote(test_message)}"
            col4.markdown(f"[📱 Test]({whatsapp_link})")
    else:
        st.info("No contacts added yet. Add your first contact above.")

def show_import_data(user_data):
    """Import schemes data - Only for Managers and above"""
    district_id = user_data['district_id']
    district_name = user_data['district_name']
    role = user_data.get('role')
    
    # Check if user has permission
    if role == 'Engineer':
        st.error("❌ Access Denied: Engineers cannot import data. This feature is available only to Managers and above.")
        st.info("💡 **Note:** Please contact your Manager/Coordinator to import scheme data.")
        return
    
    st.title(f"📁 {district_name} - Import Schemes")
    
    st.subheader("Import JJM Schemes from Excel")
    st.info("Upload your Excel file with scheme details. The system will auto-detect headers.")
    
    uploaded_file = st.file_uploader("Choose an Excel file", type=['xlsx', 'xls'])
    
    if uploaded_file:
        try:
            df_no_header = pd.read_excel(uploaded_file, header=None)
            header_row_index = -1
            
            for i, row in df_no_header.head(10).iterrows():
                row_str = ' '.join(str(x) for x in row.values).lower()
                if 'block' in row_str and 'agency' in row_str and 'scheme' in row_str:
                    header_row_index = i
                    break
            
            if header_row_index != -1:
                df = pd.read_excel(uploaded_file, header=header_row_index)
                cols = df.columns
                df.rename(columns={cols[0]: 'sr_no', cols[1]: 'block', cols[2]: 'agency', cols[3]: 'scheme_name', cols[4]: 'scheme_id'}, inplace=True)
                df = df.dropna(subset=['sr_no', 'block', 'agency', 'scheme_name', 'scheme_id'])
                
                st.write("Preview of schemes to import:")
                st.dataframe(df.head(10))
                
                if st.button("📥 Import Schemes", type="primary", use_container_width=True):
                    with st.spinner("Importing..."):
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.execute("DELETE FROM schemes WHERE district_id = ?", (district_id,))
                            conn.execute("DELETE FROM progress WHERE district_id = ?", (district_id,))
                            
                            for _, row in df.iterrows():
                                has_tw2 = 'TW-2' in str(row['scheme_name']).upper()
                                clean_name = str(row['scheme_name']).replace(' TW-2', '').replace(' tw-2', '')
                                
                                conn.execute('INSERT OR REPLACE INTO schemes (scheme_id, district_id, sr_no, block, agency, scheme_name, has_tw2) VALUES (?, ?, ?, ?, ?, ?, ?)',
                                             (str(row['scheme_id']).strip(), district_id, int(row['sr_no']), row['block'].strip(), row['agency'].strip(), clean_name, has_tw2))
                            conn.commit()
                    
                    st.success(f"🎉 Successfully imported {len(df)} schemes!")
                    st.rerun()
            else:
                st.error("Could not find header row with Block, Agency, and Scheme columns.")
        
        except Exception as e:
            st.error(f"❌ Import failed: {e}")

def show_admin_panel():
    """Admin panel for managing districts and users with enhanced multi-block agency support"""
    st.title("👨‍💼 System Administration")
    
    admin_data = st.session_state.admin_data
    st.sidebar.markdown(f"**Logged in as:** {admin_data['full_name']}")
    
    if st.sidebar.button("🚪 Logout"):
        for key in ['authenticated', 'user_type', 'admin_data', 'user_data']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["District Management", "User Management", "System Statistics", "Forecast Settings", "Admin Settings"])
    
    with tab1:
        st.subheader("🏛️ District Management")
        
        with st.expander("➕ Add New District"):
            with st.form("add_district_form"):
                col1, col2 = st.columns(2)
                district_name = col1.text_input("District Name", placeholder="e.g., Ayodhya")
                district_code = col2.text_input("District Code", placeholder="e.g., AYODHYA01")
                
                if st.form_submit_button("Add District", type="primary"):
                    district_id = secrets.token_urlsafe(16)
                    try:
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.execute('INSERT INTO districts (district_id, district_name, district_code) VALUES (?, ?, ?)',
                                         (district_id, district_name, district_code))
                            conn.commit()
                        st.success(f"✅ District '{district_name}' added successfully!")
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.error("❌ District code already exists!")
        
        st.subheader("📋 Existing Districts")
        with sqlite3.connect(DB_PATH) as conn:
            districts_df = pd.read_sql_query("SELECT district_id, district_name, district_code FROM districts ORDER BY district_name", conn)
        
        if not districts_df.empty:
            st.dataframe(districts_df.drop('district_id', axis=1), use_container_width=True)

    with tab2:
        st.subheader("👥 User Management")
        with sqlite3.connect(DB_PATH) as conn:
            all_users = pd.read_sql_query("""
                SELECT u.user_id, u.full_name, u.username, u.assigned_block, u.assigned_agency, u.role, d.district_name, u.is_active 
                FROM district_users u 
                JOIN districts d ON u.district_id = d.district_id 
                ORDER BY d.district_name, u.role, u.full_name
            """, conn)
        
        # Enhanced display with assignment details and scheme counts
        if not all_users.empty:
            display_df = all_users.copy()
            display_df['assignment_display'] = display_df.apply(lambda row: format_assignment_display(row['assigned_block'], row['assigned_agency']), axis=1)
            display_df['assigned_agency'] = display_df['assigned_agency'].fillna('❌ NOT SET')
            
            # Show the user management table
            cols_to_show = ['full_name', 'username', 'role', 'district_name', 'assignment_display', 'is_active']
            display_df_renamed = display_df[cols_to_show].copy()
            display_df_renamed.columns = ['Full Name', 'Username', 'Role', 'District', 'Assignment', 'Active']
            st.dataframe(display_df_renamed, use_container_width=True)
            
            # Show warning for Engineers without agency assignment
            engineers_without_agency = all_users[(all_users['role'] == 'Engineer') & (all_users['assigned_agency'].isna())]
            if not engineers_without_agency.empty:
                st.warning(f"⚠️ {len(engineers_without_agency)} Engineer(s) don't have agency assignment and cannot login!")

        col1, col2 = st.columns(2)
        with col1:
            with st.expander("➕ Add New User", expanded=True):
                with sqlite3.connect(DB_PATH) as conn:
                    districts_df = pd.read_sql_query("SELECT district_id, district_name FROM districts", conn)
                
                if not districts_df.empty:
                    district_map = pd.Series(districts_df.district_id.values, index=districts_df.district_name).to_dict()
                    
                    # --- Form for data entry and submission ---
                    with st.form("add_user_form", clear_on_submit=True):
                        # ALL WIDGETS ARE NOW INSIDE THE FORM
                        selected_district_name = st.selectbox("Assign to District", options=districts_df['district_name'].tolist())
                        role = st.selectbox("Role", ["Engineer", "Manager / Coordinator", "Corporate"])
                        
                        full_name = st.text_input("Full Name")
                        username = st.text_input("Username")
                        password = st.text_input("Password", type="password")
                        email = st.text_input("Email")
                        
                        assigned_block = "ALL"
                        assigned_agency = "ALL"

                        if role == "Engineer":
                            selected_district_id = district_map[selected_district_name]
                            
                            st.markdown("#### Block Assignment Details")
                            available_blocks = get_available_blocks_for_district(selected_district_id)
                            if available_blocks:
                                selected_blocks = st.multiselect("Select Blocks (one or more):", available_blocks)
                                assigned_block = ','.join(selected_blocks) if selected_blocks else ""
                            else:
                                st.info("This Engineer will be assigned to ALL blocks (No specific blocks found).")
                                assigned_block = "ALL"

                            st.markdown("#### Agency Assignment")
                            available_agencies = get_available_agencies_for_district(selected_district_id)
                            if available_agencies:
                                assigned_agency = st.selectbox("Select Agency (Required for Engineers):", [""] + available_agencies)
                            else:
                                st.warning("No agencies available. Engineer cannot be assigned until data is imported.")
                                assigned_agency = ""
                        else:
                            st.info(f"The '{role}' will be assigned to ALL blocks and agencies in this district.")
                        
                        submitted = st.form_submit_button("Create User")

                        if submitted:
                            error_messages = []
                            if not full_name: error_messages.append("Full Name is required.")
                            if not username: error_messages.append("Username is required.")
                            if not password: error_messages.append("Password is required.")
                            
                            if role == "Engineer":
                                if not assigned_block: error_messages.append("Engineers must be assigned to at least one block.")
                                if not assigned_agency: error_messages.append("Engineers must be assigned to a specific agency.")
                            
                            if not error_messages:
                                password_hash = hash_password(password)
                                selected_district_id = district_map[selected_district_name]
                                try:
                                    with sqlite3.connect(DB_PATH) as conn:
                                        conn.execute('INSERT INTO district_users (district_id, username, password_hash, full_name, email, role, assigned_block, assigned_agency) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                                                     (selected_district_id, username, password_hash, full_name, email, role, assigned_block.upper(), assigned_agency))
                                        conn.commit()
                                    st.success(f"User '{username}' created successfully!")
                                    st.rerun()
                                except sqlite3.IntegrityError:
                                    st.error(f"Username '{username}' already exists.")
                                except Exception as e:
                                    st.error(f"An unexpected error occurred: {e}")
                            else:
                                for error in error_messages:
                                    st.error(error)
                else:
                    st.warning("No districts found. Please add a district first.")

        with col2:
            with st.expander("✏️ Edit User / Reset Password"):
                if not all_users.empty:
                    with st.form("edit_user_form"):
                        user_list = all_users.set_index('user_id')[['username', 'full_name', 'role']].apply(lambda x: f"{x['username']} ({x['full_name']}) - {x['role']}", axis=1).to_dict()
                        selected_user_id = st.selectbox("Select User to Edit", options=list(user_list.keys()), format_func=lambda x: user_list[x])
                        
                        # Get current user data
                        current_user = all_users[all_users['user_id'] == selected_user_id].iloc[0]
                        
                        new_password = st.text_input("New Password (leave blank to keep current)", type="password")
                        is_active = st.checkbox("Is Active?", value=bool(current_user['is_active']))
                        
                        # Get district info for the user
                        with sqlite3.connect(DB_PATH) as conn:
                            user_district = pd.read_sql_query("SELECT district_id FROM district_users WHERE user_id = ?", conn, params=(selected_user_id,)).iloc[0]['district_id']
                        
                        # Enhanced Block Assignment Editing
                        st.markdown("#### Update Block Assignment")
                        available_blocks = get_available_blocks_for_district(user_district)
                        current_blocks = parse_assigned_blocks(current_user['assigned_block'])
                        
                        if available_blocks:
                            if current_user['assigned_block'] and current_user['assigned_block'].upper() == "ALL":
                                edit_assignment_type = st.radio("Block Assignment Type:", ["All Blocks", "Specific Blocks"], index=0, horizontal=True, key="edit_blocks")
                            else:
                                edit_assignment_type = st.radio("Block Assignment Type:", ["All Blocks", "Specific Blocks"], index=1, horizontal=True, key="edit_blocks")
                            
                            if edit_assignment_type == "All Blocks":
                                new_assigned_block = "ALL"
                            else:
                                new_selected_blocks = st.multiselect("Update Block Assignment:", available_blocks, default=current_blocks, key="edit_blocks_multi")
                                new_assigned_block = ','.join(new_selected_blocks) if new_selected_blocks else current_user['assigned_block']
                        else:
                            new_assigned_block = current_user['assigned_block']
                        
                        # Enhanced Agency Assignment Editing
                        st.markdown("#### Update Agency Assignment")
                        available_agencies = get_available_agencies_for_district(user_district)
                        if current_user['role'] == 'Engineer':
                            if available_agencies:
                                current_agency_index = available_agencies.index(current_user['assigned_agency']) if current_user['assigned_agency'] in available_agencies else 0
                                new_agency = st.selectbox("Update Agency Assignment", available_agencies, index=current_agency_index, key="edit_agency")
                            else:
                                new_agency = st.text_input("Update Agency Assignment", value=current_user['assigned_agency'] or "", placeholder="e.g., SCL, Hetvi")
                            st.info("⚠️ Engineers MUST have a specific agency assigned")
                        else:
                            if available_agencies:
                                agency_options = ["ALL"] + available_agencies
                                current_agency = current_user['assigned_agency'] or "ALL"
                                current_agency_index = agency_options.index(current_agency) if current_agency in agency_options else 0
                                new_agency = st.selectbox("Update Agency Assignment", agency_options, index=current_agency_index, key="edit_agency")
                            else:
                                new_agency = st.text_input("Update Agency Assignment (optional)", value=current_user['assigned_agency'] or "", placeholder="e.g., SCL, Hetvi, or ALL")
                        
                        # Real-time scheme count preview for edits
                        if new_assigned_block and new_agency and available_blocks and available_agencies:
                            preview_blocks = parse_assigned_blocks(new_assigned_block) if new_assigned_block != "ALL" else []
                            scheme_count = get_scheme_count_for_assignment(user_district, new_agency, preview_blocks)
                            if scheme_count > 0:
                                st.success(f"📊 **Preview:** Updated assignment will give access to **{scheme_count} schemes**")
                            else:
                                st.warning(f"⚠️ **Preview:** No schemes found for this assignment combination")
                        
                        if st.form_submit_button("Update User"):
                            # Validate Engineer agency assignment
                            if current_user['role'] == 'Engineer' and (not new_agency or new_agency.upper() == "ALL"):
                                st.error("❌ Engineers must be assigned to a specific agency (cannot be 'ALL' or empty)")
                                return
                            
                            with sqlite3.connect(DB_PATH) as conn:
                                if new_password:
                                    conn.execute("UPDATE district_users SET password_hash = ?, is_active = ?, assigned_block = ?, assigned_agency = ? WHERE user_id = ?", 
                                               (hash_password(new_password), is_active, new_assigned_block.upper(), new_agency, selected_user_id))
                                else:
                                    conn.execute("UPDATE district_users SET is_active = ?, assigned_block = ?, assigned_agency = ? WHERE user_id = ?", 
                                               (is_active, new_assigned_block.upper(), new_agency, selected_user_id))
                                conn.commit()
                            st.success("User updated!")
                            st.rerun()
                else:
                    st.info("No users have been created yet.")

    with tab3:
        st.subheader("📊 System Statistics")
        with sqlite3.connect(DB_PATH) as conn:
            total_districts = pd.read_sql_query("SELECT COUNT(*) as count FROM districts", conn).iloc[0]['count']
            total_schemes = pd.read_sql_query("SELECT COUNT(*) as count FROM schemes", conn).iloc[0]['count']
            total_users = pd.read_sql_query("SELECT COUNT(*) as count FROM district_users", conn).iloc[0]['count']
            engineers_without_agency = pd.read_sql_query("SELECT COUNT(*) as count FROM district_users WHERE role = 'Engineer' AND (assigned_agency IS NULL OR assigned_agency = '')", conn).iloc[0]['count']
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Districts", total_districts)
            col2.metric("Total Schemes", total_schemes)
            col3.metric("Total Users", total_users)
            col4.metric("Engineers Missing Agency", engineers_without_agency, delta="❌" if engineers_without_agency > 0 else "✅")
            
            if total_schemes > 0:
                district_stats = pd.read_sql_query("SELECT d.district_name, COUNT(s.scheme_id) as scheme_count FROM districts d LEFT JOIN schemes s ON d.district_id = s.district_id GROUP BY d.district_id, d.district_name ORDER BY scheme_count DESC", conn)
                st.subheader("District-wise Scheme Distribution")
                st.bar_chart(district_stats.set_index('district_name')['scheme_count'])
            
            # User role distribution
            if total_users > 0:
                user_role_stats = pd.read_sql_query("SELECT role, COUNT(*) as user_count FROM district_users GROUP BY role ORDER BY user_count DESC", conn)
                st.subheader("User Role Distribution")
                st.bar_chart(user_role_stats.set_index('role')['user_count'])
            
            # Assignment analysis for Engineers
            if total_users > 0:
                st.subheader("Engineer Assignment Analysis")
                engineer_stats = pd.read_sql_query("""
                    SELECT 
                        d.district_name,
                        u.assigned_agency,
                        u.assigned_block,
                        COUNT(*) as engineer_count
                    FROM district_users u 
                    JOIN districts d ON u.district_id = d.district_id 
                    WHERE u.role = 'Engineer' AND u.assigned_agency IS NOT NULL
                    GROUP BY d.district_name, u.assigned_agency, u.assigned_block
                    ORDER BY d.district_name, u.assigned_agency
                """, conn)
                
                if not engineer_stats.empty:
                    for district in engineer_stats['district_name'].unique():
                        district_engineers = engineer_stats[engineer_stats['district_name'] == district]
                        st.markdown(f"**{district}:**")
                        for _, eng in district_engineers.iterrows():
                            blocks_display = eng['assigned_block'] if eng['assigned_block'] != 'ALL' else 'All Blocks'
                            st.markdown(f"- {eng['assigned_agency']} | {blocks_display} | {eng['engineer_count']} engineer(s)")
    
    with tab4:
        st.subheader("⚙️ Forecast Settings")
        st.info("Set the number of penalty days to add to the timeline for different types of issues.")
        delay_settings = get_delay_settings()
        
        with st.form("delay_settings_form"):
            st.markdown("##### Severity-Based Delays")
            delay_settings['high_issues'] = st.number_input("Delay for 'High' Severity Issues (days)", min_value=0, value=delay_settings['high_issues'])
            delay_settings['critical_issues'] = st.number_input("Delay for 'Critical' Severity Issues (days)", min_value=0, value=delay_settings['critical_issues'])
            
            st.markdown("---")
            st.markdown("##### Category-Based Delays")
            delay_settings['material_issues'] = st.number_input("Delay for 'Material not delivered' Issues (days)", min_value=0, value=delay_settings['material_issues'])
            delay_settings['contractor_issues'] = st.number_input("Delay for 'Contractor not working' Issues (days)", min_value=0, value=delay_settings['contractor_issues'])
            delay_settings['payment_issues'] = st.number_input("Delay for 'Payment issues' (days)", min_value=0, value=delay_settings['payment_issues'])

            if st.form_submit_button("💾 Save Delay Settings", type="primary"):
                with sqlite3.connect(DB_PATH) as conn:
                    for name, days in delay_settings.items():
                        conn.execute("UPDATE delay_settings SET delay_days = ? WHERE setting_name = ?", (days, name))
                    conn.commit()
                st.success("✅ Delay settings have been updated!")
                st.rerun()

    with tab5:
        st.subheader("⚙️ Admin Settings")
        with st.form("admin_password_form"):
            current_password = st.text_input("Current Password", type="password")
            new_admin_password = st.text_input("New Password", type="password")
            confirm_admin_password = st.text_input("Confirm New Password", type="password")
            
            if st.form_submit_button("🔐 Change Admin Password", type="primary"):
                admin_username = admin_data['username']
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT password_hash FROM admin_users WHERE username = ?", (admin_username,))
                    stored_hash = cursor.fetchone()[0]
                
                if verify_password(current_password, stored_hash):
                    if new_admin_password == confirm_admin_password and len(new_admin_password) >= 6:
                        new_hash = hash_password(new_admin_password)
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.execute('UPDATE admin_users SET password_hash = ? WHERE username = ?', (new_hash, admin_username))
                            conn.commit()
                        st.success("✅ Admin password changed successfully!")
                        st.info("Please logout and login again with your new password.")
                    else:
                        st.error("❌ New passwords don't match or too short (min 6 characters)!")
                else:
                    st.error("❌ Current password is incorrect!")

def show_district_app():
    """Main district application with role-based navigation and enhanced assignment display"""
    user_data = st.session_state.user_data
    role = user_data.get('role')
    
    st.sidebar.title(f"🚰 {user_data['district_name']}")
    st.sidebar.markdown(f"**User:** {user_data['full_name']}")
    st.sidebar.markdown(f"**Role:** {role}")
    
    # Enhanced assignment display in sidebar
    if role == 'Engineer':
        assigned_blocks = parse_assigned_blocks(user_data.get('assigned_block'))
        assigned_agency = user_data.get('assigned_agency', 'N/A')
        
        if assigned_blocks:
            if len(assigned_blocks) <= 2:
                block_display = ', '.join(assigned_blocks)
            else:
                block_display = f"{', '.join(assigned_blocks[:2])}... (+{len(assigned_blocks)-2} more)"
        else:
            block_display = user_data.get('assigned_block', 'N/A')
        
        # Get scheme count for sidebar display
        scheme_count = get_scheme_count_for_assignment(
            user_data['district_id'], 
            assigned_agency, 
            assigned_blocks
        )
        
        st.sidebar.markdown(f"**Agency:** {assigned_agency}")
        st.sidebar.markdown(f"**Blocks:** {block_display}")
        st.sidebar.markdown(f"**Schemes:** {scheme_count}")
    else:
        st.sidebar.markdown(f"**Block:** {user_data.get('assigned_block', 'N/A')}")

    if st.sidebar.button("🚪 Logout"):
        for key in ['authenticated', 'user_type', 'user_data', 'message_to_send', 'message_key_prefix']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    
    # Role-based navigation menu
    base_pages = ["Dashboard", "Progress Entry", "Issues Dashboard", "Analytics", "O&M Verification", "Problem Schemes"]
    
    # Add restricted pages based on role
    if role in ['Manager / Coordinator', 'Corporate']:
        base_pages.extend(["WhatsApp Contacts", "Import Data"])
    
    page = st.sidebar.selectbox("Navigate to:", base_pages)
    
    # Show role restrictions info for Engineers
    if role == 'Engineer':
        with st.sidebar.expander("ℹ️ Role Restrictions"):
            st.info("""
            **Engineer Access:**
            ✅ All analysis and progress entry
            ✅ WhatsApp alerts in Issues & Problem pages
            ❌ WhatsApp contacts management
            ❌ Data import (contact your manager)
            """)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Version 1.3.3 with Import Fix**")
    st.sidebar.markdown("**Published by V R Patruni**")
    
    # Route to appropriate page
    if page == "Dashboard":
        show_dashboard(user_data)
    elif page == "Progress Entry":
        show_progress_entry(user_data)
    elif page == "Issues Dashboard":
        show_issues_dashboard(user_data)
    elif page == "Analytics":
        show_analytics(user_data)
    elif page == "O&M Verification":
        show_verification(user_data)
    elif page == "Problem Schemes":
        show_problem_schemes(user_data)
    elif page == "WhatsApp Contacts":
        show_whatsapp_contacts(user_data)
    elif page == "Import Data":
        show_import_data(user_data)

def main():
    """Main application entry point"""
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    init_database()
    load_default_components()
    
    if not st.session_state.authenticated:
        show_login_page()
    else:
        if st.session_state.user_type == "admin":
            show_admin_panel()
        elif st.session_state.user_type == "district_user":
            show_district_app()

if __name__ == "__main__":
    main()
