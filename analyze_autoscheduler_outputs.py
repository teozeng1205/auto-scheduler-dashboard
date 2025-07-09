#!/usr/bin/env python3
"""
Comprehensive Analysis of Autoscheduler Outputs with Visualizations
Analyzes the grouped data from combined_all_data_grouped.csv to provide insights into
flight data collection scheduling patterns with detailed time analysis and visualizations.
"""

import pandas as pd  # type: ignore
import numpy as np
from collections import Counter, defaultdict
import matplotlib.pyplot as plt  # type: ignore
import seaborn as sns  # type: ignore
import warnings
warnings.filterwarnings('ignore')

# Set up matplotlib for file output (no display)
import matplotlib
matplotlib.use('Agg')

def convert_time_to_hour_minute(time_value):
    """Convert time values like 500, 1030, 2359 to hour:minute format"""
    if pd.isna(time_value):
        return None
    
    time_int = int(time_value)
    hours = time_int // 100
    minutes = time_int % 100
    
    # Handle edge cases and validation
    if hours > 23 or minutes > 59:
        return None
    
    return f"{hours:02d}:{minutes:02d}"


def get_time_category(time_value):
    """Categorize time into periods of the day"""
    if pd.isna(time_value):
        return "Unknown"
    
    time_int = int(time_value)
    hours = time_int // 100
    
    if 0 <= hours < 6:
        return "Early Morning (00-06)"
    elif 6 <= hours < 12:
        return "Morning (06-12)"
    elif 12 <= hours < 18:
        return "Afternoon (12-18)"
    elif 18 <= hours < 24:
        return "Evening (18-24)"
    else:
        return "Invalid Time"


def get_hour_from_time(time_value):
    """Extract hour from time value for plotting"""
    if pd.isna(time_value):
        return None
    return int(time_value) // 100


def create_visualizations(df):
    """Create comprehensive visualizations of scheduling patterns"""
    
    print("\n📊 CREATING VISUALIZATIONS...")
    print("-" * 40)
    
    # Set up the style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create a figure with multiple subplots
    fig = plt.figure(figsize=(20, 24))
    
    # 1. Collection Frequency Distribution (Pie Chart)
    ax1 = plt.subplot(4, 3, 1)
    freq_data = df.groupby('collection_frequency')['row_count'].sum()
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
    wedges, texts, autotexts = ax1.pie(freq_data.values, labels=freq_data.index, 
                                       autopct='%1.1f%%', colors=colors, startangle=90)
    ax1.set_title('Collection Frequency Distribution\n(by Total Records)', fontsize=12, fontweight='bold')
    
    # 2. Top Sites by Volume (Horizontal Bar)
    ax2 = plt.subplot(4, 3, 2)
    provider_data = df.groupby('site')['row_count'].sum().sort_values(ascending=True).tail(8)
    bars = ax2.barh(range(len(provider_data)), provider_data.values, color='skyblue')
    ax2.set_yticks(range(len(provider_data)))
    ax2.set_yticklabels(provider_data.index)
    ax2.set_xlabel('Total Records (millions)')
    ax2.set_title('Top Sites by Volume', fontsize=12, fontweight='bold')
    
    # Add value labels on bars
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax2.text(width + 50000, bar.get_y() + bar.get_height()/2, 
                f'{width/1000000:.1f}M', ha='left', va='center', fontsize=10)
    
    # 3. Start Time Distribution (24-hour)
    ax3 = plt.subplot(4, 3, 3)
    if 'timeBox_startTime_time' in df.columns:
        df['start_hour'] = df['timeBox_startTime_time'].apply(get_hour_from_time)
        hourly_data = df.groupby('start_hour')['row_count'].sum()
        
        # Create 24-hour array
        hours = list(range(24))
        counts = [hourly_data.get(h, 0) for h in hours]
        
        bars = ax3.bar(hours, counts, color='lightcoral', alpha=0.7)
        ax3.set_xlabel('Hour of Day')
        ax3.set_ylabel('Total Records (millions)')
        ax3.set_title('Scheduling Distribution by Hour', fontsize=12, fontweight='bold')
        ax3.set_xticks([0, 6, 12, 18, 24])
        ax3.set_xticklabels(['00:00', '06:00', '12:00', '18:00', '24:00'])
        
        # Add value labels on significant bars
        for i, bar in enumerate(bars):
            if bar.get_height() > 100000:
                ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50000,
                        f'{bar.get_height()/1000000:.1f}M', ha='center', va='bottom', fontsize=10)
    
    # 4. Time Category Distribution
    ax4 = plt.subplot(4, 3, 4)
    if 'start_time_category' in df.columns:
        time_cat_data = df.groupby('start_time_category')['row_count'].sum().sort_values(ascending=False)
        bars = ax4.bar(range(len(time_cat_data)), time_cat_data.values, color='lightgreen', alpha=0.8)
        ax4.set_xticks(range(len(time_cat_data)))
        ax4.set_xticklabels([cat.replace(' (', '\n(') for cat in time_cat_data.index], rotation=45, ha='right')
        ax4.set_ylabel('Total Records (millions)')
        ax4.set_title('Records by Time Period', fontsize=12, fontweight='bold')
        
        # Add percentage labels
        total_records = time_cat_data.sum()
        for i, bar in enumerate(bars):
            pct = (bar.get_height() / total_records) * 100
            ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50000,
                    f'{pct:.1f}%', ha='center', va='bottom', fontsize=10)
    
    # 5. Window Duration Distribution
    ax5 = plt.subplot(4, 3, 5)
    if 'window_duration_minutes' in df.columns:
        duration_data = df[df['window_duration_minutes'].notna()].groupby('window_duration_minutes')['row_count'].sum().sort_values(ascending=False).head(8)
        bars = ax5.bar(range(len(duration_data)), duration_data.values, color='orange', alpha=0.7)
        
        # Create labels in hours:minutes format
        duration_labels = []
        for minutes in duration_data.index:
            hours = int(minutes) // 60
            mins = int(minutes) % 60
            duration_labels.append(f'{hours}h{mins:02d}m')
        
        ax5.set_xticks(range(len(duration_data)))
        ax5.set_xticklabels(duration_labels, rotation=45, ha='right')
        ax5.set_ylabel('Total Records (millions)')
        ax5.set_title('Most Common Window Durations', fontsize=12, fontweight='bold')
        
        # Add value labels
        for i, bar in enumerate(bars):
            ax5.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50000,
                    f'{bar.get_height()/1000000:.1f}M', ha='center', va='bottom', fontsize=10)
    
    # 6. Top Customers
    ax6 = plt.subplot(4, 3, 6)
    customer_cols = [col for col in df.columns if 'customer' in col.lower() and col.endswith('_owner')]
    if customer_cols:
        main_customer_col = customer_cols[0]
        customer_data = df.groupby(main_customer_col)['row_count'].sum().sort_values(ascending=True).tail(8)
        bars = ax6.barh(range(len(customer_data)), customer_data.values, color='mediumpurple', alpha=0.8)
        ax6.set_yticks(range(len(customer_data)))
        ax6.set_yticklabels([f'Customer {int(c)}' if pd.notna(c) else 'Unknown' for c in customer_data.index])
        ax6.set_xlabel('Total Records (millions)')
        ax6.set_title('Top Customers by Volume', fontsize=12, fontweight='bold')
        
        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax6.text(width + 20000, bar.get_y() + bar.get_height()/2,
                    f'{width/1000000:.1f}M', ha='left', va='center', fontsize=10)
    
    # 7. Configuration Frequency Distribution
    ax7 = plt.subplot(4, 3, 7)
    ranges = ['1-10', '11-100', '101-1K', '1K-10K', '10K+']
    range_counts = []
    count_ranges = [(1, 10), (11, 100), (101, 1000), (1001, 10000), (10001, float('inf'))]
    
    for min_count, max_count in count_ranges:
        if max_count == float('inf'):
            mask = df['row_count'] >= min_count
        else:
            mask = (df['row_count'] >= min_count) & (df['row_count'] <= max_count)
        range_counts.append(mask.sum())
    
    bars = ax7.bar(ranges, range_counts, color='teal', alpha=0.7)
    ax7.set_xlabel('Occurrence Range')
    ax7.set_ylabel('Number of Configurations')
    ax7.set_title('Configuration Frequency Distribution', fontsize=12, fontweight='bold')
    
    # Add value labels
    for bar in bars:
        ax7.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{int(bar.get_height())}', ha='center', va='bottom', fontsize=10)
    
    # 8. Top Collection Plans
    ax8 = plt.subplot(4, 3, 8)
    plan_data = df.groupby('hourly_collection_plan_id')['row_count'].sum().sort_values(ascending=True).tail(8)
    bars = ax8.barh(range(len(plan_data)), plan_data.values, color='gold', alpha=0.8)
    ax8.set_yticks(range(len(plan_data)))
    ax8.set_yticklabels([f'Plan {int(p)}' for p in plan_data.index])
    ax8.set_xlabel('Total Records (thousands)')
    ax8.set_title('Top Collection Plans by Volume', fontsize=12, fontweight='bold')
    
    # Add value labels
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax8.text(width + 10000, bar.get_y() + bar.get_height()/2,
                f'{width/1000:.0f}K', ha='left', va='center', fontsize=10)
    
    # 9. Site Time Preferences Heatmap
    ax9 = plt.subplot(4, 3, 9)
    if 'start_time_category' in df.columns:
        # Create site vs time category heatmap
        provider_time_data = df.groupby(['site', 'start_time_category'])['row_count'].sum().unstack(fill_value=0)
        
        # Get top 6 sites
        top_providers = df.groupby('site')['row_count'].sum().sort_values(ascending=False).head(6).index
        heatmap_data = provider_time_data.loc[top_providers]
        
        # Normalize by row (provider) to show percentages
        heatmap_data_pct = heatmap_data.div(heatmap_data.sum(axis=1), axis=0) * 100
        
        sns.heatmap(heatmap_data_pct, annot=True, fmt='.1f', cmap='YlOrRd', 
                   ax=ax9, cbar_kws={'label': 'Percentage'})
        ax9.set_title('Site Time Preferences (%)', fontsize=12, fontweight='bold')
        ax9.set_xlabel('Time Period')
        ax9.set_ylabel('Site')
    
    # 10. Input File Distribution
    ax10 = plt.subplot(4, 3, 10)
    input_cols = [col for col in df.columns if 'input_filename' in col]
    if input_cols:
        main_input_col = input_cols[0]
        file_data = df.groupby(main_input_col)['row_count'].sum().sort_values(ascending=True).tail(6)
        
        # Truncate long filenames
        file_labels = [f[:20] + '...' if len(f) > 23 else f for f in file_data.index]
        
        bars = ax10.barh(range(len(file_data)), file_data.values, color='coral', alpha=0.8)
        ax10.set_yticks(range(len(file_data)))
        ax10.set_yticklabels(file_labels)
        ax10.set_xlabel('Total Records (millions)')
        ax10.set_title('Top Input Files by Volume', fontsize=12, fontweight='bold')
        
        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax10.text(width + 20000, bar.get_y() + bar.get_height()/2,
                    f'{width/1000000:.1f}M', ha='left', va='center', fontsize=10)
    
    # 11. Daily Time Pattern (if multiple days available)
    ax11 = plt.subplot(4, 3, 11)
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    if date_cols and 'start_hour' in df.columns:
        main_date_col = date_cols[0]
        daily_hourly = df.groupby([main_date_col, 'start_hour'])['row_count'].sum().unstack(fill_value=0)
        
        if len(daily_hourly) > 1:  # Multiple days
            sns.heatmap(daily_hourly, cmap='Blues', ax=ax11, cbar_kws={'label': 'Records'})
            ax11.set_title('Daily Scheduling Patterns', fontsize=12, fontweight='bold')
            ax11.set_xlabel('Hour of Day')
            ax11.set_ylabel('Date')
        else:
            ax11.text(0.5, 0.5, 'Single Day\nData Available', ha='center', va='center', 
                     transform=ax11.transAxes, fontsize=14)
            ax11.set_title('Daily Patterns (Insufficient Data)', fontsize=12, fontweight='bold')
    
    # 12. System Load Over Time
    ax12 = plt.subplot(4, 3, 12)
    if 'start_hour' in df.columns:
        hourly_load = df.groupby('start_hour')['row_count'].sum()
        hours_full = list(range(24))
        loads_full = [hourly_load.get(h, 0) for h in hours_full]
        
        ax12.plot(hours_full, loads_full, marker='o', linewidth=2, markersize=6, color='red')
        ax12.fill_between(hours_full, loads_full, alpha=0.3, color='red')
        ax12.set_xlabel('Hour of Day')
        ax12.set_ylabel('Total Records (millions)')
        ax12.set_title('System Load Throughout Day', fontsize=12, fontweight='bold')
        ax12.set_xticks([0, 6, 12, 18, 24])
        ax12.set_xticklabels(['00:00', '06:00', '12:00', '18:00', '24:00'])
        ax12.grid(True, alpha=0.3)
        
        # Highlight peak hours
        max_load_hour = hourly_load.idxmax() if len(hourly_load) > 0 else 5
        max_load_value = hourly_load.max() if len(hourly_load) > 0 else 0
        ax12.scatter([max_load_hour], [max_load_value], color='red', s=100, zorder=5)
        ax12.annotate(f'Peak: {max_load_hour:02d}:00\n{max_load_value/1000000:.1f}M records', 
                     xy=(max_load_hour, max_load_value), xytext=(max_load_hour+2, max_load_value*0.8),
                     arrowprops=dict(arrowstyle='->', color='red'), fontsize=10, ha='left')
    
    plt.tight_layout(pad=3.0)
    
    # Save the figure
    plt.savefig('autoscheduler_analysis_dashboard.png', dpi=300, bbox_inches='tight')
    print("📊 Comprehensive dashboard saved as: autoscheduler_analysis_dashboard.png")
    
    # Create additional focused visualizations
    create_time_focus_charts(df)
    
    plt.close(fig)  # Clean up


def create_time_focus_charts(df):
    """Create focused time-related visualizations"""
    
    if 'start_hour' not in df.columns:
        return
    
    # Time-focused dashboard
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Detailed hourly distribution
    hourly_data = df.groupby('start_hour')['row_count'].sum()
    hours = list(range(24))
    counts = [hourly_data.get(h, 0) for h in hours]
    
    bars = ax1.bar(hours, counts, color=['red' if c > 1000000 else 'lightblue' for c in counts])
    ax1.set_xlabel('Hour of Day')
    ax1.set_ylabel('Total Records')
    ax1.set_title('Hourly Scheduling Distribution (Detailed)', fontsize=14, fontweight='bold')
    ax1.set_xticks(range(0, 24, 2))
    ax1.grid(True, alpha=0.3)
    
    # Add value labels for significant hours
    for i, count in enumerate(counts):
        if count > 100000:
            ax1.text(i, count + 50000, f'{count/1000000:.1f}M', ha='center', va='bottom', fontsize=9)
    
    # 2. Site scheduling patterns
    if 'start_time_category' in df.columns:
        provider_time = df.groupby(['site', 'start_time_category'])['row_count'].sum().unstack(fill_value=0)
        top_providers = df.groupby('site')['row_count'].sum().sort_values(ascending=False).head(5).index
        
        provider_time_top = provider_time.loc[top_providers]
        provider_time_top.plot(kind='bar', stacked=True, ax=ax2, colormap='Set3')
        ax2.set_title('Scheduling Patterns by Top Sites', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Site')
        ax2.set_ylabel('Total Records')
        ax2.legend(title='Time Period', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax2.tick_params(axis='x', rotation=45)
    
    # 3. Window duration vs time patterns
    if 'window_duration_minutes' in df.columns:
        duration_time = df.groupby(['start_hour', 'window_duration_minutes'])['row_count'].sum().unstack(fill_value=0)
        
        # Show only common durations and hours with data
        common_durations = df['window_duration_minutes'].value_counts().head(5).index
        active_hours = df['start_hour'].value_counts().head(10).index
        
        if len(common_durations) > 0 and len(active_hours) > 0:
            filtered_data = duration_time.loc[active_hours, common_durations].fillna(0)
            sns.heatmap(filtered_data, annot=True, fmt='.0f', cmap='YlOrRd', ax=ax3)
            ax3.set_title('Window Duration vs Start Hour', fontsize=14, fontweight='bold')
            ax3.set_xlabel('Window Duration (minutes)')
            ax3.set_ylabel('Start Hour')
    
    # 4. Load distribution pie chart with time periods
    if 'start_time_category' in df.columns:
        time_load = df.groupby('start_time_category')['row_count'].sum()
        wedges, texts, autotexts = ax4.pie(time_load.values, labels=time_load.index, 
                                          autopct='%1.1f%%', startangle=90, colors=['#FF9999', '#66B2FF', '#99FF99', '#FFCC99'])
        ax4.set_title('System Load Distribution\nby Time Period', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('autoscheduler_time_analysis.png', dpi=300, bbox_inches='tight')
    print("⏰ Time-focused analysis saved as: autoscheduler_time_analysis.png")
    plt.close(fig)


def analyze_autoscheduler_data():
    """Comprehensive analysis of autoscheduler output data with detailed time analysis and visualizations"""
    
    print("🛫 AUTOSCHEDULER OUTPUT ANALYSIS WITH TIME INSIGHTS & VISUALIZATIONS")
    print("=" * 80)
    
    # Load the grouped data
    df = pd.read_csv('combined_all_data_grouped.csv')
    
    # Basic overview
    total_unique_configs = len(df)
    total_original_records = df['row_count'].sum()
    
    print(f"\n📊 DATASET OVERVIEW")
    print(f"Unique configurations: {total_unique_configs:,}")
    print(f"Original total records: {total_original_records:,}")
    print(f"Data reduction: {total_original_records/total_unique_configs:.1f}x compression")
    
    # Collection frequency analysis
    print(f"\n📈 COLLECTION FREQUENCY ANALYSIS")
    print("-" * 40)
    freq_analysis = df.groupby('collection_frequency').agg({
        'row_count': ['sum', 'count', 'mean', 'median', 'max']
    }).round(1)
    freq_analysis.columns = ['Total_Records', 'Unique_Configs', 'Avg_Count', 'Median_Count', 'Max_Count']
    print(freq_analysis)
    
    # ==================== COMPREHENSIVE TIME ANALYSIS ====================
    print(f"\n⏰ COMPREHENSIVE TIME SCHEDULING ANALYSIS")
    print("=" * 50)
    
    # Identify time columns
    time_columns = []
    for col in df.columns:
        if ('time' in col.lower() and 'Time' in col) or col.endswith('_time'):
            time_columns.append(col)
    
    print(f"Identified time columns: {len(time_columns)} columns")
    
    # Focus on main timeBox columns for detailed analysis
    main_start_time = 'timeBox_startTime_time'
    main_end_time = 'timeBox_endTime_time'
    
    if main_start_time in df.columns and main_end_time in df.columns:
        print(f"\n⏰ MAIN SCHEDULING TIME WINDOW ANALYSIS")
        print("-" * 45)
        
        # Create time categories
        df['start_time_category'] = df[main_start_time].apply(get_time_category)
        df['end_time_category'] = df[main_end_time].apply(get_time_category)
        df['start_time_formatted'] = df[main_start_time].apply(convert_time_to_hour_minute)
        df['end_time_formatted'] = df[main_end_time].apply(convert_time_to_hour_minute)
        df['start_hour'] = df[main_start_time].apply(get_hour_from_time)
        
        # Calculate window duration in minutes
        def calc_duration_minutes(start_time, end_time):
            if pd.isna(start_time) or pd.isna(end_time):
                return None
            
            start_int = int(start_time)
            end_int = int(end_time)
            
            start_hours = start_int // 100
            start_minutes = start_int % 100
            end_hours = end_int // 100
            end_minutes = end_int % 100
            
            start_total_minutes = start_hours * 60 + start_minutes
            end_total_minutes = end_hours * 60 + end_minutes
            
            # Handle day rollover
            if end_total_minutes < start_total_minutes:
                end_total_minutes += 24 * 60
            
            return end_total_minutes - start_total_minutes
        
        df['window_duration_minutes'] = df.apply(lambda row: calc_duration_minutes(
            row[main_start_time], row[main_end_time]), axis=1)
        
        # Time distribution analysis
        print("\n📊 START TIME DISTRIBUTION BY CATEGORY")
        start_time_dist = df.groupby('start_time_category').agg({
            'row_count': ['sum', 'count']
        }).round(0)
        start_time_dist.columns = ['Total_Records', 'Unique_Configs']
        start_time_dist['Pct_Records'] = (start_time_dist['Total_Records'] / total_original_records * 100).round(1)
        print(start_time_dist.sort_values('Total_Records', ascending=False))
        
        # Most common specific start times
        print(f"\n🕐 TOP 10 MOST COMMON START TIMES")
        print("-" * 35)
        start_time_summary = df.groupby([main_start_time, 'start_time_formatted']).agg({
            'row_count': 'sum'
        }).sort_values('row_count', ascending=False).head(10)
        
        for (time_val, formatted_time), row in start_time_summary.iterrows():
            records = row['row_count']
            pct = (records / total_original_records) * 100
            print(f"  {formatted_time or 'Invalid'} ({int(time_val)}): {records:,} records ({pct:.1f}%)")
    
    # Create visualizations
    create_visualizations(df)
    
    # Continue with existing analysis sections (abbreviated for space)
    print(f"\n🎉 Analysis complete! Check the generated visualization files:")
    print("  • autoscheduler_analysis_dashboard.png - Comprehensive overview")
    print("  • autoscheduler_time_analysis.png - Time-focused insights")
    
    return df


def generate_autoscheduler_report(df):
    """Generate a comprehensive report file with time analysis and visualization references"""
    
    summary = []
    summary.append("# Autoscheduler Output Analysis Report with Time Insights & Visualizations")
    summary.append(f"Generated from {len(df):,} unique configurations")
    summary.append("")
    
    total_records = df['row_count'].sum()
    summary.append("## Executive Summary")
    summary.append(f"- **Total Original Records**: {total_records:,}")
    summary.append(f"- **Unique Configurations**: {len(df):,}")
    summary.append(f"- **Data Compression**: {total_records/len(df):.1f}x")
    summary.append("")
    
    summary.append("## Generated Visualizations")
    summary.append("- **autoscheduler_analysis_dashboard.png**: Comprehensive 12-panel dashboard")
    summary.append("- **autoscheduler_time_analysis.png**: Time-focused analysis charts")
    summary.append("")
    
    # Collection frequency breakdown
    summary.append("## Collection Frequency Distribution")
    freq_counts = df.groupby('collection_frequency')['row_count'].agg(['sum', 'count'])
    for freq in freq_counts.index:
        total_records_freq = freq_counts.loc[freq, 'sum']
        unique_configs_freq = freq_counts.loc[freq, 'count']
        pct_records = (total_records_freq / total_records) * 100
        summary.append(f"- **{freq.upper()}**: {total_records_freq:,} records ({pct_records:.1f}%) across {unique_configs_freq:,} configurations")
    summary.append("")
    
    # Time analysis summary
    if 'start_time_category' in df.columns:
        summary.append("## Scheduling Time Distribution")
        time_dist = df.groupby('start_time_category')['row_count'].sum().sort_values(ascending=False)
        for time_cat, records in time_dist.items():
            pct = (records / total_records) * 100
            summary.append(f"- **{time_cat}**: {records:,} records ({pct:.1f}%)")
        summary.append("")
    
    # Peak scheduling insight
    if 'start_hour' in df.columns:
        hourly_data = df.groupby('start_hour')['row_count'].sum()
        peak_hour = hourly_data.idxmax()
        peak_records = hourly_data.max()
        peak_pct = (peak_records / total_records) * 100
        summary.append("## Peak Scheduling Time")
        summary.append(f"- **Peak Hour**: {peak_hour:02d}:00 with {peak_records:,} records ({peak_pct:.1f}%)")
        summary.append("")
    
    # Top sites
    summary.append("## Top Sites by Volume")
    provider_stats = df.groupby('site')['row_count'].sum().sort_values(ascending=False)
    for i, (provider, records) in enumerate(provider_stats.head(5).items(), 1):
        pct = (records / total_records) * 100
        summary.append(f"{i}. **{provider}**: {records:,} records ({pct:.1f}%)")
    summary.append("")
    
    # Save summary
    with open('autoscheduler_analysis_report.md', 'w') as f:
        f.write('\n'.join(summary))
    
    print(f"📄 Detailed report saved to: autoscheduler_analysis_report.md")


if __name__ == "__main__":
    df = analyze_autoscheduler_data()
    generate_autoscheduler_report(df)
    print(f"\n🎉 Autoscheduler analysis with visualizations complete!") 