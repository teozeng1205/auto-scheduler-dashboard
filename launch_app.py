#!/usr/bin/env python3
"""
Launcher script for the Autoscheduler Data Explorer Streamlit Application
"""

import subprocess
import sys
import os
import webbrowser
import time

def check_data_file():
    """Check if the required data file exists"""
    if not os.path.exists('combined_all_data_grouped.csv'):
        print("❌ Required data file 'combined_all_data_grouped.csv' not found!")
        print("\n🔧 Please run the following commands first:")
        print("   1. python combine_all_data.py")
        print("   2. python group_identical_rows.py")
        return False
    return True

def install_dependencies():
    """Install required dependencies"""
    print("📦 Installing dependencies...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                      check=True, capture_output=True)
        print("✅ Dependencies installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def launch_streamlit():
    """Launch the Streamlit application"""
    print("🚀 Launching Autoscheduler Data Explorer...")
    print("📊 The application will open in your browser automatically")
    print("🌐 If not, navigate to: http://localhost:8501")
    print("\n⏹️  Press Ctrl+C to stop the application")
    
    try:
        # Launch Streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "streamlit_scheduler_explorer.py",
            "--server.headless=false"
        ])
    except KeyboardInterrupt:
        print("\n👋 Application stopped by user")
    except Exception as e:
        print(f"❌ Error launching application: {e}")

def main():
    """Main launcher function"""
    print("🛫 AUTOSCHEDULER DATA EXPLORER LAUNCHER")
    print("=" * 50)
    
    # Check if data file exists
    if not check_data_file():
        return
    
    print("✅ Data file found: combined_all_data_grouped.csv")
    
    # Install dependencies
    if not install_dependencies():
        return
    
    # Launch application
    launch_streamlit()

if __name__ == "__main__":
    main() 