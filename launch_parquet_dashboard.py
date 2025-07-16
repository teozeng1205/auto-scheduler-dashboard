#!/usr/bin/env python3
"""
Launcher script for the Enhanced Autoscheduler Data Explorer
Now supports both JSON and Parquet pipelines!
"""

import subprocess
import sys
import os
import webbrowser
import time

def check_data_files():
    """Check if the required data files exist"""
    json_file = 'combined_all_data_grouped.csv'
    parquet_file = 'combined_all_parquet_data_grouped.csv'
    
    json_exists = os.path.exists(json_file)
    parquet_exists = os.path.exists(parquet_file)
    
    print("📊 Data File Status:")
    print(f"  JSON Pipeline: {'✅ Available' if json_exists else '❌ Missing'} ({json_file})")
    print(f"  Parquet Pipeline: {'✅ Available' if parquet_exists else '❌ Missing'} ({parquet_file})")
    
    if not json_exists and not parquet_exists:
        print("\n❌ No data files found!")
        print("\n🔧 To generate data files:")
        print("\n  For JSON Pipeline:")
        print("    1. python download_from_s3.py")
        print("    2. python combine_all_data.py")
        print("    3. python group_identical_rows.py")
        print("\n  For Parquet Pipeline:")
        print("    1. python download_parquet_from_s3.py")
        print("    2. python combine_all_parquet_data.py")
        print("    3. python group_identical_parquet_rows.py")
        return False
    
    if not json_exists:
        print("\n⚠️  JSON pipeline data not available - only Parquet pipeline will work")
    if not parquet_exists:
        print("\n⚠️  Parquet pipeline data not available - only JSON pipeline will work")
    
    return True

def install_dependencies():
    """Install required Python packages"""
    try:
        import streamlit
        import pandas
        import plotly
        print("✅ All dependencies are installed")
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("📦 Installing required packages...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
            print("✅ Dependencies installed successfully")
            return True
        except subprocess.CalledProcessError:
            print("❌ Failed to install dependencies")
            return False

def launch_streamlit():
    """Launch the Streamlit application"""
    print("\n🚀 Launching Enhanced Autoscheduler Data Explorer...")
    print("🔄 New feature: Switch between JSON and Parquet pipelines!")
    print("📊 The dashboard will open in your browser")
    print("🛑 Press Ctrl+C to stop the application")
    
    try:
        # Open browser after a short delay
        def open_browser():
            time.sleep(3)
            webbrowser.open('http://localhost:8501')
        
        import threading
        threading.Thread(target=open_browser, daemon=True).start()
        
        # Run Streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "streamlit_scheduler_explorer.py",
            "--server.port", "8501",
            "--server.address", "localhost"
        ])
    except KeyboardInterrupt:
        print("\n👋 Application stopped by user")
    except Exception as e:
        print(f"❌ Error launching application: {e}")

def main():
    """Main launcher function"""
    print("🛫 ENHANCED AUTOSCHEDULER DATA EXPLORER LAUNCHER")
    print("=" * 60)
    print("🆕 NEW: Supports both JSON and Parquet pipeline data!")
    print("🔄 Switch between data sources in the dashboard sidebar")
    print("=" * 60)
    
    # Check if data files exist
    if not check_data_files():
        return
    
    # Install dependencies
    if not install_dependencies():
        return
    
    # Launch application
    launch_streamlit()

if __name__ == "__main__":
    main() 