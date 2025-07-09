#!/usr/bin/env python3
"""
Autoscheduler Gantt Chart Visualization - Intensity Only
Creates an intensity heatmap showing provider|site|customer scheduling patterns
across 24 hours for a single day.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# Set up matplotlib for file output
import matplotlib
matplotlib.use('Agg')

def create_gantt_chart_data(df, target_date=None):
    """Create data structure for Gantt chart visualization"""
    
    # Filter for a specific date if provided, otherwise use the most common date
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    if date_cols and target_date is None:
        main_date_col = date_cols[0]
        target_date = df[main_date_col].mode().iloc[0] if len(df[main_date_col].mode()) > 0 else df[main_date_col].iloc[0]
        print(f"Using most common date: {target_date}")
    elif date_cols and target_date:
        main_date_col = date_cols[0]
        df = df[df[main_date_col] == target_date]
        print(f"Filtered for date: {target_date}")
    
    if len(df) == 0:
        print("No data available for the specified date")
        return None, None, None, None
    
    # Create provider|site|customer combinations
    # Use provider, site, and customerCollection_customer columns
    df['provider_site_customer'] = (df['provider'].astype(str) + '|' + 
                                   df['site'].fillna('N/A').astype(str) + '|' + 
                                   df['customerCollection_customer'].fillna('N/A').astype(str))
    
    # Convert time values to hours
    def time_to_hour(time_val):
        if pd.isna(time_val):
            return None
        return int(time_val) // 100
    
    def time_to_decimal_hour(time_val):
        if pd.isna(time_val):
            return None
        time_int = int(time_val)
        hours = time_int // 100
        minutes = time_int % 100
        return hours + minutes / 60.0
    
    # Process start and end times
    start_time_col = 'timeBox_startTime_time'
    end_time_col = 'timeBox_endTime_time'
    
    if start_time_col in df.columns and end_time_col in df.columns:
        df['start_hour'] = df[start_time_col].apply(time_to_hour)
        df['end_hour'] = df[end_time_col].apply(time_to_hour)
        df['start_decimal'] = df[start_time_col].apply(time_to_decimal_hour)
        df['end_decimal'] = df[end_time_col].apply(time_to_decimal_hour)
        
        # Handle day rollover
        df['end_decimal_adjusted'] = df.apply(lambda row: 
            row['end_decimal'] + 24 if row['end_decimal'] < row['start_decimal'] else row['end_decimal'], 
            axis=1)
        
        # Order provider_site_customer combinations by hourly_collection_plan_id
        provider_ordering = df.groupby('provider_site_customer')['hourly_collection_plan_id'].first().sort_values()
        provider_site_customers = provider_ordering.index.tolist()
        hours = list(range(24))
        
        # Initialize matrix
        intensity_matrix = np.zeros((len(provider_site_customers), 24))
        
        # Fill matrix with scheduling data
        for idx, (_, row) in enumerate(df.iterrows()):
            if pd.notna(row['start_decimal']) and pd.notna(row['end_decimal_adjusted']):
                provider_idx = provider_site_customers.index(row['provider_site_customer'])
                start_hour = int(row['start_decimal'])
                end_hour = int(row['end_decimal_adjusted'])
                
                # Mark hours with intensity
                for hour in range(start_hour, min(end_hour + 1, 24)):
                    intensity_matrix[provider_idx, hour] += row['row_count']
                
                # Handle rollover to next day
                if end_hour >= 24:
                    for hour in range(0, end_hour - 24 + 1):
                        intensity_matrix[provider_idx, hour] += row['row_count']
        
        return intensity_matrix, provider_site_customers, target_date, df
    
    return None, None, None, None


def create_intensity_gantt_chart(intensity_matrix, provider_site_customers, target_date, filtered_df):
    """Create intensity heatmap visualization"""
    
    print("\n📊 CREATING INTENSITY GANTT CHART...")
    print("-" * 50)
    
    # Create figure for intensity heatmap only
    fig, ax = plt.subplots(figsize=(16, max(8, len(provider_site_customers) * 0.3)))
    
    # Use log scale for intensity to handle wide range
    log_intensity = np.log1p(intensity_matrix)  # log(1+x) to handle zeros
    
    sns.heatmap(log_intensity,
                xticklabels=[f'{h:02d}:00' for h in range(24)],
                yticklabels=provider_site_customers,
                cmap='Reds',
                cbar_kws={'label': 'Log(Records + 1)'},
                linewidths=0.5,
                linecolor='white',
                ax=ax)
    
    ax.set_title(f'Scheduling Intensity by Hour - Date: {target_date}', 
                  fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Hour of Day', fontsize=12, fontweight='bold')
    ax.set_ylabel('Provider | Site | Customer', fontsize=12, fontweight='bold')
    ax.tick_params(axis='y', rotation=0, labelsize=8)
    ax.tick_params(axis='x', rotation=45, labelsize=10)
    
    # Adjust layout to prevent y-axis labels from being cut off
    plt.tight_layout()
    
    # Save the chart
    filename = f'autoscheduler_intensity_gantt_{target_date}.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"📊 Intensity Gantt chart saved as: {filename}")
    
    plt.close(fig)
    
    return filename


def main():
    """Main function to create intensity Gantt chart visualization"""
    
    print("🛫 AUTOSCHEDULER INTENSITY GANTT CHART GENERATOR")
    print("=" * 60)
    
    # Load the data
    df = pd.read_csv('combined_all_data_grouped.csv')
    print(f"Loaded {len(df):,} unique configurations")
    
    # Create Gantt chart data
    intensity_matrix, provider_site_customers, target_date, filtered_df = create_gantt_chart_data(df)
    
    if intensity_matrix is not None and provider_site_customers is not None and filtered_df is not None:
        print(f"Created scheduling matrix: {len(provider_site_customers)} provider|site|customer combinations × 24 hours")
        print(f"Filtered data contains {len(filtered_df):,} configurations for date {target_date}")
        
        # Create intensity visualization only
        intensity_filename = create_intensity_gantt_chart(intensity_matrix, provider_site_customers, 
                                                         target_date, filtered_df)
        
        print(f"\n🎉 Intensity Gantt chart visualization complete!")
        print(f"Generated file: {intensity_filename}")
        
        # Summary statistics
        print(f"\n📊 SUMMARY STATISTICS FOR {target_date}")
        print("-" * 40)
        
        active_hours = np.sum(intensity_matrix.sum(axis=0) > 0)
        total_provider_site_customers = len(provider_site_customers)
        peak_hour = np.argmax(intensity_matrix.sum(axis=0))
        peak_load = intensity_matrix.sum(axis=0)[peak_hour]
        
        print(f"Active scheduling hours: {active_hours}/24")
        print(f"Provider|Site|Customer combinations: {total_provider_site_customers}")
        print(f"Peak hour: {peak_hour:02d}:00 with {peak_load:,.0f} total records")
        print(f"Average records per active hour: {intensity_matrix.sum()/(active_hours or 1):,.0f}")
        
        # Show top 10 busiest provider|site|customer combinations
        provider_totals = intensity_matrix.sum(axis=1)
        top_indices = np.argsort(provider_totals)[-10:][::-1]
        
        print(f"\nTop 10 busiest Provider|Site|Customer combinations:")
        for i, idx in enumerate(top_indices, 1):
            total_records = provider_totals[idx]
            if total_records > 0:
                print(f"  {i:2d}. {provider_site_customers[idx]}: {total_records:,.0f} records")
        
    else:
        print("❌ Could not create Gantt chart - insufficient time data")


if __name__ == "__main__":
    main() 