#!/usr/bin/env python3
"""
Multi-District JJM O&M Readiness Tracker - Cloud Version
VERSION 15.0 - Multi-District Support
Features:
1. District-specific login system
2. Complete data isolation between districts
3. District-specific dashboards and analytics
4. Admin panel for district management
5. Cloud deployment ready
"""

import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import io
import os
import numpy as np
import matplotlib.pyplot as plt
from openpyxl.drawing.image import Image as OpenpyxlImage
import hashlib
import secrets

# --- Page Configuration ---
st.set_page_config(
    page_title="Multi-District JJM O&M Tracker",
    page_icon="🚰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- ROBUST DATABASE PATH ---
script_dir = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(script_dir, "multi_district_jjm_tracker.db")

# --- Authentication & Session Management ---

def hash_password(password):
    """Hash password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hashed_password):
    """Verify password against hash"""
    return hashlib.sha256(password.encode()).hexdigest() == hashed_password

def init_auth_database():
    """Initialize authentication and district management tables"""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Districts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS districts (
                district_id TEXT PRIMARY KEY,
                district_name TEXT NOT NULL,
                district_code TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                contact_person TEXT,
                contact_email TEXT,
                contact_phone TEXT,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        
        # Admin users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_users (
                admin_id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name TEXT,
                email TEXT,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        
        # Create default admin if not exists
        cursor.execute("SELECT COUNT(*) FROM admin_users")
        if cursor.fetchone()[0] == 0:
            admin_id = secrets.token_urlsafe(16)
            admin_password_hash = hash_password("admin123")  # Default password
            cursor.execute('''
                INSERT INTO admin_users (admin_id, username, password_hash, full_name, email)
                VALUES (?, ?, ?, ?, ?)
            ''', (admin_id, "admin", admin_password_hash, "System Administrator", "admin@jjm.gov.in"))
        
        conn.commit()

def authenticate_district(district_code, password):
    """Authenticate district user"""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT district_id, district_name, password_hash, is_active 
            FROM districts WHERE district_code = ?
        ''', (district_code,))
        result = cursor.fetchone()
        
        if result and result[3] and verify_password(password, result[2]):
            # Update last login
            cursor.execute('''
                UPDATE districts SET last_login = CURRENT_TIMESTAMP 
                WHERE district_code = ?
            ''', (district_code,))
            conn.commit()
            return {"district_id": result[0], "district_name": result[1], "district_code": district_code}
    return None

def authenticate_admin(username, password):
    """Authenticate admin user"""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT admin_id, full_name, password_hash, is_active 
            FROM admin_users WHERE username = ?
        ''', (username,))
        result = cursor.fetchone()
        
        if result and result[3] and verify_password(password, result[2]):
            # Update last login
            cursor.execute('''
                UPDATE admin_users SET last_login = CURRENT_TIMESTAMP 
                WHERE username = ?
            ''', (username,))
            conn.commit()
            return {"admin_id": result[0], "full_name": result[1], "username": username}
    return None

# --- Core Database Functions (Modified for Multi-District) ---

def init_database():
    """Initializes the multi-district database schema."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # District-specific settings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS district_settings (
                district_id TEXT,
                setting_key TEXT,
                setting_value TEXT,
                PRIMARY KEY (district_id, setting_key)
            )
        ''')
        
        # District-specific schemes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schemes (
                scheme_id TEXT,
                district_id TEXT,
                sr_no INTEGER, 
                block TEXT, 
                agency TEXT, 
                scheme_name TEXT, 
                has_tw2 BOOLEAN, 
                agency_submitted_date DATE, 
                tpia_verified_date DATE, 
                ee_verified_date DATE,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (scheme_id, district_id)
            )
        ''')
        
        # Components table (shared across districts)
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
        
        # District-specific progress
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
                remarks TEXT,
                last_updated TIMESTAMP,
                UNIQUE(district_id, scheme_id, component_id)
            )
        ''')
        
        conn.commit()

def load_default_components():
    """Populates the components table with the full checklist."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM components")
        if cursor.fetchone()[0] == 0:
            components_list = [
                ('Pipe Laying', 'Distribution Line', 'main', 'metric', 'Km'),
                ('FHTC Installation', 'Distribution Line', 'main', 'metric', 'Nos.'),
                ('Stand Post Installation', 'Distribution Line', 'main', 'metric', 'Nos.'),
                ('Grouting of FHTC', 'Distribution Line', 'main', 'task', '%'),
                ('Sluice Valves', 'Distribution Line', 'main', 'metric', 'Nos.'),
                ('Air Valves', 'Distribution Line', 'main', 'metric', 'Nos.'),
                ('Fire Hydrant', 'Distribution Line', 'main', 'metric', 'Nos.'),
                
                ('OHT Structure', 'OHT', 'main', 'task', '%'),
                ('OHT Staircase', 'OHT', 'main', 'task', '%'),
                ('Tank/Dome Installation', 'OHT', 'main', 'task', '%'),
                ('Structure/Tank Painting', 'OHT', 'main', 'task', '%'),
                ('SWSM Logo', 'OHT', 'main', 'task', '%'),
                ('Lightening Arrester Installation', 'OHT', 'main', 'task', '%'),
                ('Lightening Arrester Earthing', 'OHT', 'main', 'task', '%'),
                ('Railing Installation', 'OHT', 'main', 'task', '%'),
                ('Railing Painting', 'OHT', 'main', 'task', '%'),
                ('Inlet/Outlet/Washout Piping', 'OHT', 'main', 'task', '%'),
                ('All Valve Chambers', 'OHT', 'main', 'task', '%'),
                ('Apron & Flooring', 'OHT', 'main', 'task', '%'),

                ('Pump House Civil Construction', 'Pump House', 'main', 'task', '%'),
                ('Doors Installation & Painting', 'Pump House', 'main', 'task', '%'),
                ('Windows Installation & Painting', 'Pump House', 'main', 'task', '%'),
                ('Pump & Motor Installation', 'Pump House', 'main', 'task', '%'),
                ('Internal Cabling & Lighting', 'Pump House', 'main', 'task', '%'),
                ('DG Set Foundation & Installation', 'Pump House', 'main', 'task', '%'),
                ('DG Set Earthing', 'Pump House', 'main', 'task', '%'),
                ('RTU Panel & VFD Installation', 'Pump House', 'main', 'task', '%'),
                ('Chlorine Dosing System', 'Pump House', 'main', 'task', '%'),
                ('Sensors & Flowmeters', 'Pump House', 'main', 'task', '%'),

                ('Boundary Wall Construction', 'Boundary Wall', 'main', 'task', '%'),
                ('Boundary Wall Painting', 'Boundary Wall', 'main', 'task', '%'),
                ('Main Gate Installation & Painting', 'Boundary Wall', 'main', 'task', '%'),
                ('Wicket Gate Installation & Painting', 'Boundary Wall', 'main', 'task', '%'),

                ('Structure Installation', 'Solar Plant', 'main', 'task', '%'),
                ('Panels Installation & Alignment', 'Solar Plant', 'main', 'task', '%'),
                ('Cabling & RTU Connection', 'Solar Plant', 'main', 'task', '%'),
                ('Plant Earthing', 'Solar Plant', 'main', 'task', '%'),
                ('Lightening Arrester', 'Solar Plant', 'main', 'task', '%'),
                ('Interlocking Work', 'Solar Plant', 'main', 'task', '%'),
                
                ('Interlocking Road', 'Campus Development', 'main', 'task', '%'),
                ('Recharge Pit', 'Campus Development', 'main', 'task', '%'),
                ('Solar Street Lights', 'Campus Development', 'main', 'task', '%'),
                ('Landscaping & Debris Removal', 'Campus Development', 'main', 'task', '%'),
                ('Site Sign Board', 'Campus Development', 'main', 'task', '%'),
                ('All Site Drains', 'Campus Development', 'main', 'task', '%'),

                ('HGJ Certification', 'Final Certification', 'main', 'metric', 'Villages'),
                ('Road Restoration Certificate', 'Final Certification', 'main', 'metric', 'Villages'),

                ('Pump House Construction', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Doors & Windows', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Pump & Motor Installation', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Electrical & DG Set', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Automation & Sensors', 'Pump House TW-2', 'tw2', 'task', '%'),
                ('Boundary Wall & Gates', 'Site Development TW-2', 'tw2', 'task', '%'),
                ('Solar Plant', 'Site Development TW-2', 'tw2', 'task', '%'),
            ]
            cursor.executemany('INSERT INTO components (component_name, component_group, site_type, entry_type, unit) VALUES (?, ?, ?, ?, ?)', components_list)
            conn.commit()

def get_full_scheme_data(district_id):
    """Gets all scheme and progress data for a specific district."""
    with sqlite3.connect(DB_PATH) as conn:
        schemes_df = pd.read_sql_query("SELECT * FROM schemes WHERE district_id = ?", conn, params=(district_id,))
        if schemes_df.empty: 
            return pd.DataFrame()
        
        progress_query = """
            SELECT p.scheme_id, p.component_id, p.days_remaining, c.entry_type,
                   CASE c.entry_type 
                   WHEN 'metric' THEN (CAST(p.achieved_value AS REAL) / NULLIF(p.target_value, 0)) * 100 
                   ELSE p.progress_percent END as calculated_progress
            FROM progress p 
            JOIN components c ON p.component_id = c.component_id
            WHERE p.district_id = ?"""
        
        progress_df = pd.read_sql_query(progress_query, conn, params=(district_id,))
        
        if not progress_df.empty:
            agg_progress = progress_df.groupby('scheme_id').agg(
                avg_progress=('calculated_progress', 'mean'), 
                max_days_remaining=('days_remaining', 'max')
            ).reset_index()
            full_df = pd.merge(schemes_df, agg_progress, on="scheme_id", how="left")
        else:
            full_df = schemes_df
            full_df['avg_progress'], full_df['max_days_remaining'] = 0, 0
        
        full_df['avg_progress'] = full_df['avg_progress'].fillna(0).round(1)
        full_df['max_days_remaining'] = full_df['max_days_remaining'].fillna(0)
        
        conditions = [
            (full_df['ee_verified_date'].notna()),
            (full_df['agency_submitted_date'].notna()),
            (full_df['avg_progress'] >= 100),
            (full_df['avg_progress'] > 0)
        ]
        choices = ['In O&M', 'Under Verification', 'Ready for Inspection', 'In Progress']
        full_df['status'] = np.select(conditions, choices, default='Not Started')
        
        return full_df

def load_district_settings(district_id):
    """Load settings for a specific district"""
    with sqlite3.connect(DB_PATH) as conn:
        try:
            settings = pd.read_sql_query(
                "SELECT setting_key, setting_value FROM district_settings WHERE district_id = ?", 
                conn, params=(district_id,)
            ).set_index('setting_key')['setting_value'].to_dict()
        except:
            settings = {}
        
        if 'company_name' not in settings:
            settings = {'company_name': 'JJM Implementation Agency'}
            save_district_settings(district_id, settings['company_name'])
        
        return settings.get('company_name')

def save_district_settings(district_id, company_name):
    """Save settings for a specific district"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO district_settings (district_id, setting_key, setting_value) VALUES (?, 'company_name', ?)",
            (district_id, company_name)
        )
        conn.commit()

def create_analytics_report(df, forecast_df, district_name):
    """Creates district-specific analytics report."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Summary sheet
        summary_df = df['status'].value_counts().reset_index()
        summary_df.columns = ['Status', 'Number of Schemes']
        summary_df.to_excel(writer, sheet_name='Status Summary', index=False)
        
        # Forecast sheet
        if not forecast_df.empty:
            forecast_df.to_excel(writer, sheet_name='O&M Forecast', index=False)
        
        # Status-wise lists
        for status in ['Ready for Inspection', 'Under Verification', 'In O&M']:
            status_df = df[df['status'] == status]
            if not status_df.empty:
                status_df.to_excel(writer, sheet_name=f'{status} List', index=False)
    
    output.seek(0)
    return output

# --- Authentication UI ---

def show_login_page():
    """Display login page for districts and admin"""
    st.title("🚰 Multi-District JJM O&M Tracker")
    st.markdown("---")
    
    # Login type selector
    login_type = st.radio("Login as:", ["District User", "System Administrator"], horizontal=True)
    
    if login_type == "District User":
        st.subheader("🏛️ District Login")
        st.info("Enter your district credentials to access your O&M tracker.")
        
        with st.form("district_login_form"):
            district_code = st.text_input("District Code", placeholder="e.g., AYODHYA001")
            password = st.text_input("Password", type="password")
            submit_button = st.form_submit_button("🔐 Login", type="primary", use_container_width=True)
            
            if submit_button:
                if district_code and password:
                    district_data = authenticate_district(district_code, password)
                    if district_data:
                        st.session_state.authenticated = True
                        st.session_state.user_type = "district"
                        st.session_state.district_data = district_data
                        st.success(f"Welcome, {district_data['district_name']}!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid district code or password!")
                else:
                    st.error("Please enter both district code and password.")
    
    else:  # Admin login
        st.subheader("👨‍💼 Administrator Login")
        st.info("System administrators can manage districts and view overall statistics.")
        
        with st.form("admin_login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit_button = st.form_submit_button("🔐 Admin Login", type="primary", use_container_width=True)
            
            if submit_button:
                if username and password:
                    admin_data = authenticate_admin(username, password)
                    if admin_data:
                        st.session_state.authenticated = True
                        st.session_state.user_type = "admin"
                        st.session_state.admin_data = admin_data
                        st.success(f"Welcome, {admin_data['full_name']}!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid username or password!")
                else:
                    st.error("Please enter both username and password.")
    
    # Help section
    st.markdown("---")
    st.subheader("📞 Need Help?")
    st.info("""
    **For District Users:**
    - Contact your JJM coordinator for district code and password
    - Each district has unique login credentials
    
    **For Administrators:**
    - Default credentials: username=`admin`, password=`admin123`
    - Change default password after first login
    
    **Technical Support:**
    - Email: support@jjm.gov.in
    - Phone: 1800-XXX-XXXX
    """)

def show_admin_panel():
    """Display admin panel for managing districts"""
    st.title("👨‍💼 System Administration")
    
    admin_data = st.session_state.admin_data
    st.sidebar.markdown(f"**Logged in as:** {admin_data['full_name']}")
    
    if st.sidebar.button("🚪 Logout"):
        for key in ['authenticated', 'user_type', 'admin_data']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    
    tab1, tab2, tab3, tab4 = st.tabs(["District Management", "System Statistics", "Admin Settings", "Danger Zone"])
    
    with tab1:
        st.subheader("🏛️ District Management")
        
        # Add new district
        with st.expander("➕ Add New District"):
            with st.form("add_district_form"):
                col1, col2 = st.columns(2)
                district_name = col1.text_input("District Name", placeholder="e.g., Ayodhya")
                district_code = col2.text_input("District Code", placeholder="e.g., AYODHYA001")
                
                col3, col4 = st.columns(2)
                contact_person = col3.text_input("Contact Person")
                contact_email = col4.text_input("Contact Email")
                contact_phone = st.text_input("Contact Phone")
                
                password = st.text_input("District Password", type="password")
                confirm_password = st.text_input("Confirm Password", type="password")
                
                if st.form_submit_button("Add District", type="primary"):
                    if password == confirm_password and len(password) >= 6:
                        district_id = secrets.token_urlsafe(16)
                        password_hash = hash_password(password)
                        
                        try:
                            with sqlite3.connect(DB_PATH) as conn:
                                conn.execute('''
                                    INSERT INTO districts (district_id, district_name, district_code, password_hash, 
                                                         contact_person, contact_email, contact_phone)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                ''', (district_id, district_name, district_code, password_hash, 
                                     contact_person, contact_email, contact_phone))
                                conn.commit()
                            st.success(f"✅ District '{district_name}' added successfully!")
                            st.rerun()
                        except sqlite3.IntegrityError:
                            st.error("❌ District code already exists!")
                    else:
                        st.error("❌ Passwords don't match or password too short (min 6 characters)!")
        
        # List existing districts with management options
        st.subheader("📋 Existing Districts")
        with sqlite3.connect(DB_PATH) as conn:
            districts_df = pd.read_sql_query('''
                SELECT district_id, district_name, district_code, contact_person, contact_email, 
                       created_date, last_login, is_active
                FROM districts ORDER BY district_name
            ''', conn)
        
        if not districts_df.empty:
            st.dataframe(districts_df.drop('district_id', axis=1), use_container_width=True)
            
            # District management actions
            st.subheader("🔧 District Management Actions")
            
            # Select district for actions
            district_options = {f"{row['district_name']} ({row['district_code']})": row['district_id'] 
                              for _, row in districts_df.iterrows() if row['is_active']}
            
            if district_options:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**🔑 Change District Password**")
                    selected_district = st.selectbox("Select District:", list(district_options.keys()), key="change_pass")
                    
                    with st.form("change_password_form"):
                        new_password = st.text_input("New Password", type="password")
                        confirm_password = st.text_input("Confirm New Password", type="password")
                        
                        if st.form_submit_button("🔑 Change Password", type="primary"):
                            if new_password == confirm_password and len(new_password) >= 6:
                                district_id = district_options[selected_district]
                                new_password_hash = hash_password(new_password)
                                
                                with sqlite3.connect(DB_PATH) as conn:
                                    conn.execute('''
                                        UPDATE districts SET password_hash = ? WHERE district_id = ?
                                    ''', (new_password_hash, district_id))
                                    conn.commit()
                                
                                st.success(f"✅ Password changed for {selected_district}")
                                st.rerun()
                            else:
                                st.error("❌ Passwords don't match or too short (min 6 characters)!")
                
                with col2:
                    st.markdown("**🗑️ Delete District**")
                    selected_district_del = st.selectbox("Select District to Delete:", list(district_options.keys()), key="delete_dist")
                    
                    with st.form("delete_district_form"):
                        st.warning("⚠️ This will permanently delete the district and ALL its data!")
                        confirm_delete = st.text_input("Type 'DELETE' to confirm:")
                        
                        if st.form_submit_button("🗑️ Delete District", type="secondary"):
                            if confirm_delete == "DELETE":
                                district_id = district_options[selected_district_del]
                                
                                with sqlite3.connect(DB_PATH) as conn:
                                    # Delete district data
                                    conn.execute("DELETE FROM progress WHERE district_id = ?", (district_id,))
                                    conn.execute("DELETE FROM schemes WHERE district_id = ?", (district_id,))
                                    conn.execute("DELETE FROM district_settings WHERE district_id = ?", (district_id,))
                                    conn.execute("DELETE FROM districts WHERE district_id = ?", (district_id,))
                                    conn.commit()
                                
                                st.success(f"✅ District {selected_district_del} deleted successfully!")
                                st.rerun()
                            else:
                                st.error("❌ Please type 'DELETE' to confirm!")
            else:
                st.info("No active districts to manage.")
        else:
            st.info("No districts registered yet.")
    
    with tab2:
        st.subheader("📊 System Statistics")
        
        with sqlite3.connect(DB_PATH) as conn:
            # Overall statistics
            total_districts = pd.read_sql_query("SELECT COUNT(*) as count FROM districts WHERE is_active = 1", conn).iloc[0]['count']
            total_schemes = pd.read_sql_query("SELECT COUNT(*) as count FROM schemes", conn).iloc[0]['count']
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Active Districts", total_districts)
            col2.metric("Total Schemes", total_schemes)
            col3.metric("System Uptime", "99.9%")  # Placeholder
            
            # District-wise scheme count
            if total_schemes > 0:
                district_stats = pd.read_sql_query('''
                    SELECT d.district_name, COUNT(s.scheme_id) as scheme_count
                    FROM districts d
                    LEFT JOIN schemes s ON d.district_id = s.district_id
                    WHERE d.is_active = 1
                    GROUP BY d.district_id, d.district_name
                    ORDER BY scheme_count DESC
                ''', conn)
                
                st.subheader("District-wise Scheme Distribution")
                st.bar_chart(district_stats.set_index('district_name')['scheme_count'])
    
    with tab3:
        st.subheader("⚙️ Admin Settings")
        
        # Change admin password
        st.markdown("**🔐 Change Admin Password**")
        with st.form("admin_password_form"):
            current_password = st.text_input("Current Password", type="password")
            new_admin_password = st.text_input("New Password", type="password")
            confirm_admin_password = st.text_input("Confirm New Password", type="password")
            
            if st.form_submit_button("🔐 Change Admin Password", type="primary"):
                # Verify current password
                admin_username = admin_data['username']
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT password_hash FROM admin_users WHERE username = ?", (admin_username,))
                    stored_hash = cursor.fetchone()[0]
                
                if verify_password(current_password, stored_hash):
                    if new_admin_password == confirm_admin_password and len(new_admin_password) >= 6:
                        new_hash = hash_password(new_admin_password)
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.execute('''
                                UPDATE admin_users SET password_hash = ? WHERE username = ?
                            ''', (new_hash, admin_username))
                            conn.commit()
                        st.success("✅ Admin password changed successfully!")
                        st.info("Please logout and login again with your new password.")
                    else:
                        st.error("❌ New passwords don't match or too short (min 6 characters)!")
                else:
                    st.error("❌ Current password is incorrect!")
        
        # System configuration
        st.markdown("---")
        st.markdown("**🔧 System Configuration**")
        st.info("Additional system-wide settings can be configured here in future updates.")
    
    with tab4:
        st.subheader("⚠️ Danger Zone")
        st.error("**WARNING: These actions are irreversible!**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🗑️ Clear All District Data**")
            with st.form("clear_all_data_form"):
                st.warning("This will delete ALL districts and their data!")
                confirm_clear = st.text_input("Type 'CLEAR ALL DATA' to confirm:")
                
                if st.form_submit_button("🗑️ Clear All Data", type="secondary"):
                    if confirm_clear == "CLEAR ALL DATA":
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.execute("DELETE FROM progress")
                            conn.execute("DELETE FROM schemes") 
                            conn.execute("DELETE FROM district_settings")
                            conn.execute("DELETE FROM districts")
                            conn.commit()
                        st.success("✅ All district data cleared!")
                        st.rerun()
                    else:
                        st.error("❌ Please type 'CLEAR ALL DATA' to confirm!")
        
        with col2:
            st.markdown("**🔄 Reset Database**")
            with st.form("reset_database_form"):
                st.warning("This will reset the entire application!")
                confirm_reset = st.text_input("Type 'RESET DATABASE' to confirm:")
                
                if st.form_submit_button("🔄 Reset Database", type="secondary"):
                    if confirm_reset == "RESET DATABASE":
                        if os.path.exists(DB_PATH):
                            os.remove(DB_PATH)
                        st.success("✅ Database reset! Please refresh the page.")
                        st.info("The application will restart with default admin credentials.")
                    else:
                        st.error("❌ Please type 'RESET DATABASE' to confirm!")

# --- Main Application ---

def show_district_app():
    """Display the main application for district users"""
    district_data = st.session_state.district_data
    district_id = district_data['district_id']
    district_name = district_data['district_name']
    
    # Sidebar
    st.sidebar.title(f"🚰 {district_name}")
    st.sidebar.markdown(f"**District Code:** {district_data['district_code']}")
    
    company_name = load_district_settings(district_id)
    st.sidebar.markdown(f"**Agency:** {company_name}")
    
    if st.sidebar.button("🚪 Logout"):
        for key in ['authenticated', 'user_type', 'district_data']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    
    page = st.sidebar.selectbox("Navigate to:", [
        "Dashboard", "Progress Entry", "O&M Verification", 
        "Analytics", "Schemes", "Import Data", "Settings"
    ])
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Version 15.0 - Multi-District**")
    
    # Page routing (same as original but with district_id parameter)
    if page == "Dashboard":
        show_dashboard(district_id, district_name)
    elif page == "Progress Entry":
        show_progress_entry(district_id, district_name)
    elif page == "O&M Verification":
        show_verification(district_id, district_name)
    elif page == "Analytics":
        show_analytics(district_id, district_name)
    elif page == "Schemes":
        show_schemes(district_id, district_name)
    elif page == "Import Data":
        show_import_data(district_id, district_name)
    elif page == "Settings":
        show_district_settings(district_id, district_name)

def show_dashboard(district_id, district_name):
    """Dashboard page for district"""
    st.title(f"🏠 {district_name} - Dashboard")
    
    df = get_full_scheme_data(district_id)
    if not df.empty:
        status_counts = df['status'].value_counts()
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Schemes", len(df))
        col2.metric("Ready for Inspection", status_counts.get('Ready for Inspection', 0))
        col3.metric("Under O&M Verification", status_counts.get('Under Verification', 0))
        col4.metric("In O&M", status_counts.get('In O&M', 0))
        
        st.subheader("📊 All Schemes Overview")
        display_df = df[['scheme_name', 'block', 'agency', 'avg_progress', 'status']].rename(
            columns={'avg_progress': 'Progress %'}
        )
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No schemes found. Please go to the 'Import Data' page to get started.")

def show_progress_entry(district_id, district_name):
    """Progress entry page for district"""
    st.title(f"✏️ {district_name} - Progress Entry")
    
    with sqlite3.connect(DB_PATH) as conn:
        schemes_df = pd.read_sql_query(
            "SELECT scheme_id, scheme_name, block, has_tw2 FROM schemes WHERE district_id = ?", 
            conn, params=(district_id,)
        )
    
    if not schemes_df.empty:
        scheme_options = {
            f"{row['scheme_id']} - {row['scheme_name']} ({row['block']}){' [Multi-Site]' if row['has_tw2'] else ''}": row['scheme_id'] 
            for _, row in schemes_df.iterrows()
        }
        
        selected_display = st.selectbox("Select Scheme:", list(scheme_options.keys()))
        if selected_display:
            selected_scheme_id = scheme_options[selected_display]
            scheme_info = schemes_df[schemes_df['scheme_id'] == selected_scheme_id].iloc[0]
            
            site_type = "main"
            if bool(scheme_info['has_tw2']):
                selected_site_name = st.radio("Choose site to update:", ["Main Site", "TW-2 Site"], horizontal=True)
                site_type = "tw2" if selected_site_name == "TW-2 Site" else "main"
            
            st.subheader(f"Updating Progress for: *{scheme_info['scheme_name']}* - **{site_type.upper()} Site**")
            
            with sqlite3.connect(DB_PATH) as conn:
                components_df = pd.read_sql_query(
                    "SELECT * FROM components WHERE site_type = ?", 
                    conn, params=(site_type,)
                )
                progress_df = pd.read_sql_query(
                    "SELECT * FROM progress WHERE scheme_id = ? AND district_id = ?", 
                    conn, params=(selected_scheme_id, district_id)
                )
            
            component_groups = components_df['component_group'].unique().tolist()
            tabs = st.tabs(component_groups)
            
            for i, group_name in enumerate(component_groups):
                with tabs[i]:
                    with st.form(key=f"form_{group_name}_{site_type}"):
                        updates = []
                        group_comps = components_df[components_df['component_group'] == group_name]
                        
                        for _, comp in group_comps.iterrows():
                            current_progress = progress_df[progress_df['component_id'] == comp['component_id']]
                            
                            if comp['entry_type'] == 'metric':
                                c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 1, 2])
                                c1.write(f"{comp['component_name']}")
                                
                                target = c2.number_input(
                                    f"Target ({comp['unit']})", 
                                    value=float(current_progress['target_value'].iloc[0] or 0) if not current_progress.empty else 0.0, 
                                    key=f"target_{comp['component_id']}", 
                                    format="%.2f"
                                )
                                achieved = c3.number_input(
                                    f"Achieved ({comp['unit']})", 
                                    value=float(current_progress['achieved_value'].iloc[0] or 0) if not current_progress.empty else 0.0, 
                                    key=f"achieved_{comp['component_id']}", 
                                    format="%.2f"
                                )
                                progress_val = (achieved / target * 100) if target > 0 else 0
                                c4.markdown(f"<p style='text-align: center; font-weight: bold; margin-top: 28px;'>{progress_val:.1f}%</p>", unsafe_allow_html=True)
                                days = c5.number_input(
                                    "Days Left", 
                                    value=int(current_progress['days_remaining'].iloc[0] or 0) if not current_progress.empty else 0, 
                                    key=f"days_m_{comp['component_id']}"
                                )
                                updates.append({
                                    'type': 'metric', 
                                    'comp_id': comp['component_id'], 
                                    'target': target, 
                                    'achieved': achieved, 
                                    'days': days
                                })
                            
                            elif comp['entry_type'] == 'task':
                                c1, c2, c3 = st.columns([5, 3, 2])
                                c1.write(f"{comp['component_name']}")
                                progress_percent = c2.slider(
                                    "Progress %", 
                                    0, 100, 
                                    int(current_progress['progress_percent'].iloc[0] or 0) if not current_progress.empty else 0, 
                                    key=f"prog_{comp['component_id']}"
                                )
                                days = c3.number_input(
                                    "Days Left", 
                                    value=int(current_progress['days_remaining'].iloc[0] or 0) if not current_progress.empty else 0, 
                                    key=f"days_t_{comp['component_id']}"
                                )
                                updates.append({
                                    'type': 'task', 
                                    'comp_id': comp['component_id'], 
                                    'percent': progress_percent, 
                                    'days': days
                                })
                        
                        if st.form_submit_button(f"💾 Save Progress for {group_name}", type="primary"):
                            with sqlite3.connect(DB_PATH) as conn:
                                for u in updates:
                                    if u['type'] == 'metric':
                                        conn.execute('''
                                            INSERT OR REPLACE INTO progress 
                                            (district_id, scheme_id, component_id, target_value, achieved_value, days_remaining, last_updated) 
                                            VALUES (?, ?, ?, ?, ?, ?, ?)
                                        ''', (district_id, selected_scheme_id, u['comp_id'], u['target'], u['achieved'], u['days'], datetime.now()))
                                    elif u['type'] == 'task':
                                        conn.execute('''
                                            INSERT OR REPLACE INTO progress 
                                            (district_id, scheme_id, component_id, progress_percent, days_remaining, last_updated) 
                                            VALUES (?, ?, ?, ?, ?, ?)
                                        ''', (district_id, selected_scheme_id, u['comp_id'], u['percent'], u['days'], datetime.now()))
                                conn.commit()
                            st.success(f"Progress for {group_name} saved!")
                            st.rerun()
    else:
        st.info("No schemes found. Please import your schemes first.")

def show_verification(district_id, district_name):
    """O&M Verification page for district"""
    st.title(f"📝 {district_name} - O&M Verification Tracking")
    
    df = get_full_scheme_data(district_id)
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
                "agency_submitted_date": st.column_config.DateColumn("Agency Submitted", format="YYYY-MM-DD"),
                "tpia_verified_date": st.column_config.DateColumn("TPIA Verified", format="YYYY-MM-DD"),
                "ee_verified_date": st.column_config.DateColumn("EE Verified", format="YYYY-MM-DD")
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

def show_analytics(district_id, district_name):
    """Analytics page for district"""
    st.title(f"📈 {district_name} - Analytics")
    
    df = get_full_scheme_data(district_id)
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
    
    tab1, tab2 = st.tabs(["Forecast & Summaries", "Charts & Visuals"])
    
    forecast_df = df[df['status'].isin(['In Progress', 'Ready for Inspection'])].copy()
    display_cols = []
    
    if not forecast_df.empty:
        today = datetime.now()
        forecast_df['physical_completion_date'] = forecast_df['max_days_remaining'].apply(lambda d: today + timedelta(days=d))
        forecast_df['forecasted_om_date'] = forecast_df['physical_completion_date'] + timedelta(days=buffer_days)
        display_cols = ['scheme_name', 'block', 'agency', 'avg_progress', 'physical_completion_date', 'forecasted_om_date']
    
    with tab1:
        st.subheader("Key Status Metrics")
        status_counts = df['status'].value_counts()
        c1, c2, c3 = st.columns(3)
        c1.metric("Ready for Inspection", status_counts.get('Ready for Inspection', 0))
        c2.metric("Under O&M Verification", status_counts.get('Under Verification', 0))
        c3.metric("Currently in O&M", status_counts.get('In O&M', 0))
        
        st.markdown("---")
        st.subheader("O&M Forecast")
        if not forecast_df.empty:
            st.dataframe(
                forecast_df[display_cols].rename(columns={
                    'avg_progress': 'Progress %',
                    'physical_completion_date': 'Est. Physical Completion',
                    'forecasted_om_date': 'Forecasted O&M Start'
                }),
                use_container_width=True
            )
        else:
            st.info("No schemes are currently in progress to forecast.")
    
    with tab2:
        st.subheader("Visuals")
        fig1, ax1 = plt.subplots()
        status_counts = df['status'].value_counts()
        
        if not status_counts.empty:
            ax1.pie(status_counts, labels=status_counts.index, autopct='%1.1f%%', startangle=90)
            ax1.axis('equal')
            ax1.set_title(f"{district_name} - Scheme Status Distribution")
            st.pyplot(fig1)
        else:
            st.info("No status data to display chart.")
        
        if not forecast_df.empty:
            fig2, ax2 = plt.subplots(figsize=(10, 6))
            monthly_summary_chart = forecast_df.groupby(forecast_df['forecasted_om_date'].dt.to_period('M')).size()
            monthly_summary_chart.index = monthly_summary_chart.index.strftime('%Y-%B')
            
            ax2.bar(monthly_summary_chart.index, monthly_summary_chart.values)
            ax2.set_title(f"{district_name} - O&M Forecast by Month")
            ax2.set_ylabel("Number of Schemes")
            ax2.tick_params(axis='x', rotation=45)
            plt.tight_layout()
            st.pyplot(fig2)
        
        st.info("To save a chart, right-click it and choose 'Save image as...'")
    
    st.markdown("---")
    st.subheader("Download Analytics Report")
    excel_data = create_analytics_report(df, forecast_df[display_cols] if not forecast_df.empty else pd.DataFrame(), district_name)
    st.download_button(
        label="📥 Download Analytics Report (Excel)",
        data=excel_data,
        file_name=f"{district_name}_JJM_Analytics_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def show_schemes(district_id, district_name):
    """Schemes page for district"""
    st.title(f"📋 {district_name} - All Schemes")
    
    schemes_df = get_full_scheme_data(district_id)
    if not schemes_df.empty:
        st.dataframe(schemes_df, use_container_width=True)
    else:
        st.info("No schemes in the database for your district.")

def show_import_data(district_id, district_name):
    """Import data page for district"""
    st.title(f"📁 {district_name} - Import & Manage Data")
    
    st.subheader("Import JJM Schemes from Excel")
    st.info("This tool auto-detects your header row and imports schemes specific to your district.")
    
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
                df.rename(columns={
                    cols[0]: 'sr_no',
                    cols[1]: 'block',
                    cols[2]: 'agency',
                    cols[3]: 'scheme_name',
                    cols[4]: 'scheme_id'
                }, inplace=True)
                
                df = df.dropna(subset=['sr_no', 'block', 'agency', 'scheme_name', 'scheme_id'])
                
                st.write("Preview of schemes to import:")
                st.dataframe(df.head(10))
                
                if st.button("📥 Import Schemes (Replace All for Your District)", type="primary", use_container_width=True):
                    with st.spinner("Importing..."):
                        with sqlite3.connect(DB_PATH) as conn:
                            # Delete existing schemes and progress for this district only
                            conn.execute("DELETE FROM schemes WHERE district_id = ?", (district_id,))
                            conn.execute("DELETE FROM progress WHERE district_id = ?", (district_id,))
                            
                            # Import new schemes
                            for _, row in df.iterrows():
                                has_tw2 = 'TW-2' in str(row['scheme_name']).upper()
                                clean_name = str(row['scheme_name']).replace(' TW-2', '').replace(' tw-2', '')
                                
                                conn.execute('''
                                    INSERT OR REPLACE INTO schemes 
                                    (scheme_id, district_id, sr_no, block, agency, scheme_name, has_tw2) 
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    str(row['scheme_id']).strip(), 
                                    district_id, 
                                    int(row['sr_no']), 
                                    row['block'], 
                                    row['agency'], 
                                    clean_name, 
                                    has_tw2
                                ))
                            conn.commit()
                    
                    st.success(f"🎉 Successfully imported {len(df)} schemes for {district_name}!")
                    st.rerun()
            else:
                st.error("Could not automatically find the header row.")
        
        except Exception as e:
            st.error(f"❌ Import failed: {e}")
    
    st.markdown("---")
    st.subheader("🗑️ District Data Management")
    
    if st.button("🗑️ Clear All Data for Your District", type="secondary"):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DELETE FROM schemes WHERE district_id = ?", (district_id,))
            conn.execute("DELETE FROM progress WHERE district_id = ?", (district_id,))
            conn.commit()
        st.success(f"All data cleared for {district_name}. You can now import fresh data.")
        st.rerun()

def show_district_settings(district_id, district_name):
    """Settings page for district"""
    st.title(f"⚙️ {district_name} - Settings")
    
    with st.form("district_settings_form"):
        company_name = load_district_settings(district_id)
        new_company_name = st.text_input("Implementation Agency Name", value=company_name)
        
        if st.form_submit_button("Save Settings"):
            save_district_settings(district_id, new_company_name)
            st.success("Settings saved successfully!")
            st.rerun()
    
    st.markdown("---")
    st.subheader("📊 District Information")
    
    with sqlite3.connect(DB_PATH) as conn:
        district_info = pd.read_sql_query('''
            SELECT district_name, district_code, contact_person, contact_email, 
                   contact_phone, created_date, last_login
            FROM districts WHERE district_id = ?
        ''', conn, params=(district_id,))
    
    if not district_info.empty:
        info = district_info.iloc[0]
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"""
            **District:** {info['district_name']}
            **Code:** {info['district_code']}
            **Contact Person:** {info['contact_person'] or 'Not set'}
            """)
        
        with col2:
            st.info(f"""
            **Email:** {info['contact_email'] or 'Not set'}
            **Phone:** {info['contact_phone'] or 'Not set'}
            **Last Login:** {info['last_login'] or 'Never'}
            """)

# --- Main Application Entry Point ---

def main():
    """Main application entry point"""
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    # Initialize databases
    init_auth_database()
    init_database()
    load_default_components()
    
    # Route to appropriate interface
    if not st.session_state.authenticated:
        show_login_page()
    else:
        if st.session_state.user_type == "admin":
            show_admin_panel()
        elif st.session_state.user_type == "district":
            show_district_app()

if __name__ == "__main__":
    main()