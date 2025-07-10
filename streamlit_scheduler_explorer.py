#!/usr/bin/env python3
"""
Streamlit Application for Autoscheduler Data Exploration and Gantt Chart Visualization
Interactive dashboard for exploring and visualizing flight data collection scheduling patterns.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Autoscheduler Data Explorer",
    page_icon="🛫",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
.main > div {
    padding-top: 2rem;
}
.stSelectbox > div > div > div {
    font-size: 14px;
}
.metric-container {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 0.5rem;
    border: 1px solid #e1e5e9;
}
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Load and cache the grouped data"""
    try:
        df = pd.read_csv('combined_all_data_grouped.csv')
        return df
    except FileNotFoundError:
        st.error("❌ File 'combined_all_data_grouped.csv' not found. Please ensure the data processing scripts have been run.")
        st.stop()

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

def time_to_decimal_hour(time_val):
    """Convert time value to decimal hour for plotting"""
    if pd.isna(time_val):
        return None
    time_int = int(time_val)
    hours = time_int // 100
    minutes = time_int % 100
    return hours + minutes / 60.0

def create_gantt_chart_data(df, target_date=None):
    """Create data structure for Gantt chart visualization"""
    
    # Handle date filtering
    if target_date is None and 'timeBox_startTime_date' in df.columns:
        # Use the most common date
        target_date = df['timeBox_startTime_date'].mode().iloc[0] if len(df['timeBox_startTime_date'].mode()) > 0 else df['timeBox_startTime_date'].iloc[0]
    
    if target_date and 'timeBox_startTime_date' in df.columns:
        df_filtered = df[df['timeBox_startTime_date'] == target_date].copy()
    else:
        df_filtered = df.copy()
    
    if len(df_filtered) == 0:
        return None, None, None
    
    # Create provider|site|customer combinations
    df_filtered['provider_site_customer'] = (
        df_filtered['provider'].astype(str) + '|' + 
        df_filtered['site'].fillna('N/A').astype(str) + '|' + 
        df_filtered['customerCollection_customer'].fillna('N/A').astype(str)
    )
    
    # Convert time values to decimal hours
    start_time_col = 'timeBox_startTime_time'
    end_time_col = 'timeBox_endTime_time'
    
    if start_time_col in df_filtered.columns and end_time_col in df_filtered.columns:
        df_filtered['start_decimal'] = df_filtered[start_time_col].apply(time_to_decimal_hour)
        df_filtered['end_decimal'] = df_filtered[end_time_col].apply(time_to_decimal_hour)
        
        # Handle day rollover
        df_filtered['end_decimal_adjusted'] = df_filtered.apply(lambda row: 
            row['end_decimal'] + 24 if row['end_decimal'] < row['start_decimal'] else row['end_decimal'], 
            axis=1)
        
        # Order provider_site_customer combinations by hourly_collection_plan_id
        provider_ordering = df_filtered.groupby('provider_site_customer')['hourly_collection_plan_id'].first().sort_values()
        provider_site_customers = provider_ordering.index.tolist()
        
        hours = list(range(24))
        
        # Initialize matrix
        intensity_matrix = np.zeros((len(provider_site_customers), 24))
        
        # Fill matrix with scheduling data
        for idx, (_, row) in enumerate(df_filtered.iterrows()):
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
        
        return intensity_matrix, provider_site_customers, df_filtered
    
    return None, None, None

def create_interactive_gantt_chart(intensity_matrix, provider_site_customers, df_filtered):
    """Create interactive Gantt chart with intensity using Plotly"""
    
    if intensity_matrix is None or len(provider_site_customers) == 0:
        st.warning("No data available for Gantt chart visualization")
        return None
    
    # Create heatmap data
    hours = [f'{h:02d}:00' for h in range(24)]
    
    # Use log scale for better visualization
    log_intensity = np.log1p(intensity_matrix)
    
    # Pre-format the raw record counts for the hover tooltip
    hover_text = []
    for row in intensity_matrix:
        hover_text.append([f'{val:,.0f}' for val in row])

    # Create the heatmap
    fig = go.Figure(data=go.Heatmap(
        z=log_intensity,
        x=hours,
        y=provider_site_customers,
        colorscale='Reds',
        hoverongaps=False,
        hovertemplate='<b>%{y}</b><br>' +
                     'Hour: %{x}<br>' +
                     'Log(Records + 1): %{z:.2f}<br>' +
                     'Raw Records: %{customdata}<extra></extra>',
        customdata=hover_text,  # Use pre-formatted text
        showscale=True
    ))
    
    fig.update_layout(
        title={
            'text': f'Scheduling Intensity Heatmap - {len(provider_site_customers)} Provider|Site|Customer Combinations',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='Hour of Day',
        yaxis_title='Provider | Site | Customer',
        height=max(400, len(provider_site_customers) * 25),
        font=dict(size=12),
        margin=dict(l=300, r=50, t=80, b=50)
    )
    
    # Update axes
    fig.update_xaxes(tickangle=45)
    fig.update_yaxes(tickfont=dict(size=10))
    
    return fig

def create_summary_charts(df):
    """Create summary charts for data exploration"""
    
    charts = {}
    
    # 1. Collection Frequency Distribution
    freq_data = df.groupby('collection_frequency')['row_count'].sum().reset_index()
    charts['frequency'] = px.pie(
        freq_data, 
        values='row_count', 
        names='collection_frequency',
        title='Distribution by Collection Frequency',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    # 2. Top Providers
    provider_data = df.groupby('site')['row_count'].sum().sort_values(ascending=False).head(10).reset_index()
    charts['providers'] = px.bar(
        provider_data,
        x='row_count',
        y='site',
        orientation='h',
        title='Top 10 Sites by Volume',
        labels={'row_count': 'Total Records', 'site': 'Site'},
        color='row_count',
        color_continuous_scale='Blues'
    )
    
    # 3. Hourly Distribution
    if 'timeBox_startTime_time' in df.columns:
        df_temp = df.copy()
        df_temp['start_hour'] = df_temp['timeBox_startTime_time'].apply(lambda x: int(x) // 100 if pd.notna(x) else None)
        hourly_data = df_temp.groupby('start_hour')['row_count'].sum().reset_index()
        
        charts['hourly'] = px.bar(
            hourly_data,
            x='start_hour',
            y='row_count',
            title='Scheduling Distribution by Hour of Day',
            labels={'start_hour': 'Hour', 'row_count': 'Total Records'},
            color='row_count',
            color_continuous_scale='Viridis'
        )
        charts['hourly'].update_xaxes(tickmode='linear', tick0=0, dtick=2)
    
    # 4. Customer Distribution
    customer_data = df.groupby('customerCollection_customer')['row_count'].sum().sort_values(ascending=False).head(8).reset_index()
    charts['customers'] = px.bar(
        customer_data,
        x='customerCollection_customer',
        y='row_count',
        title='Top Customers by Volume',
        labels={'customerCollection_customer': 'Customer', 'row_count': 'Total Records'},
        color='row_count',
        color_continuous_scale='Oranges'
    )
    charts['customers'].update_xaxes(tickangle=45)
    
    return charts

def main():
    """Main Streamlit application"""
    
    # Header
    st.title("🛫 Autoscheduler Data Explorer")
    st.markdown("**Interactive Dashboard for Flight Data Collection Scheduling Analysis**")
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading data..."):
        df = load_data()
    
    # Sidebar filters
    st.sidebar.header("📊 Data Filters")
    
    # Collection frequency filter
    frequencies = ['All'] + sorted(df['collection_frequency'].unique().tolist())
    selected_frequency = st.sidebar.selectbox(
        "Collection Frequency",
        frequencies,
        index=0
    )
    
    # Provider filter
    providers = ['All'] + sorted(df['provider'].unique().tolist())
    selected_provider_filter = st.sidebar.selectbox(
        "Provider",
        providers,
        index=0
    )
    
    # Site filter
    sites = ['All'] + sorted(df['site'].unique().tolist())
    selected_site = st.sidebar.selectbox(
        "Site",
        sites,
        index=0
    )
    
    # Customer filter
    customers = ['All'] + sorted(df['customerCollection_customer'].unique().tolist())
    selected_customer = st.sidebar.selectbox(
        "Customer",
        customers,
        index=0
    )
    
    # Date filter
    if 'timeBox_startTime_date' in df.columns:
        dates = ['All'] + sorted(df['timeBox_startTime_date'].unique().tolist())
        selected_date = st.sidebar.selectbox(
            "Date",
            dates,
            index=0
        )
    else:
        selected_date = 'All'
    
    # Apply filters
    filtered_df = df.copy()
    
    if selected_frequency != 'All':
        filtered_df = filtered_df[filtered_df['collection_frequency'] == selected_frequency]
    
    if selected_provider_filter != 'All':
        filtered_df = filtered_df[filtered_df['provider'] == selected_provider_filter]
    
    if selected_site != 'All':
        filtered_df = filtered_df[filtered_df['site'] == selected_site]
    
    if selected_customer != 'All':
        filtered_df = filtered_df[filtered_df['customerCollection_customer'] == selected_customer]
    
    if selected_date != 'All':
        filtered_df = filtered_df[filtered_df['timeBox_startTime_date'] == selected_date]
    
    # Display summary metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Total Configurations",
            "{:,}".format(len(filtered_df)),
            delta="{:,}".format(len(filtered_df) - len(df)) + " from total"
        )
    
    with col2:
        total_records = filtered_df['row_count'].sum()
        st.metric(
            "Total Records",
            "{:,}".format(total_records),
            delta=f"{(total_records/df['row_count'].sum()*100-100):.1f}%" if len(df) > 0 else "0%"
        )
    
    with col3:
        unique_providers = filtered_df['provider'].nunique()
        st.metric(
            "Unique Providers",
            "{:,}".format(unique_providers)
        )
    
    with col4:
        unique_sites = filtered_df['site'].nunique()
        st.metric(
            "Unique Sites",
            "{:,}".format(unique_sites)
        )
    
    with col5:
        unique_customers = filtered_df['customerCollection_customer'].nunique()
        st.metric(
            "Unique Customers",
            "{:,}".format(unique_customers)
        )
    
    st.markdown("---")
    
    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Data Overview", "📈 Summary Charts", "📅 Gantt Chart", "🔍 Data Explorer"])
    
    with tab1:
        st.header("Data Overview")
        
        if len(filtered_df) > 0:
            # Show basic statistics
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Collection Frequency Breakdown")
                freq_summary = filtered_df.groupby('collection_frequency').agg({
                    'row_count': ['sum', 'count', 'mean']
                }).round(1)
                freq_summary.columns = ['Total Records', 'Configurations', 'Avg Records/Config']
                # Format numbers with commas
                freq_summary['Total Records'] = freq_summary['Total Records'].apply(lambda x: "{:,}".format(x))
                freq_summary['Configurations'] = freq_summary['Configurations'].apply(lambda x: "{:,}".format(x))
                st.dataframe(freq_summary, use_container_width=True)
            
            with col2:
                st.subheader("Top Sites")
                provider_summary = filtered_df.groupby('site')['row_count'].sum().sort_values(ascending=False).head(5)
                # Format numbers with commas
                provider_summary = provider_summary.apply(lambda x: "{:,}".format(x))
                st.dataframe(provider_summary.to_frame('Total Records'), use_container_width=True)
            
            # Time analysis if available
            if 'timeBox_startTime_time' in filtered_df.columns:
                st.subheader("Time Distribution")
                
                # Convert times to readable format
                filtered_df_temp = filtered_df.copy()
                filtered_df_temp['start_time_formatted'] = filtered_df_temp['timeBox_startTime_time'].apply(convert_time_to_hour_minute)
                filtered_df_temp['end_time_formatted'] = filtered_df_temp['timeBox_endTime_time'].apply(convert_time_to_hour_minute)
                
                time_summary = filtered_df_temp.groupby('start_time_formatted')['row_count'].sum().sort_values(ascending=False).head(10)
                
                # Format the time summary with thousand separators
                time_summary = time_summary.apply(lambda x: "{:,}".format(x))
                
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Top 10 Start Times:**")
                    st.dataframe(time_summary.to_frame('Total Records'), use_container_width=True)
                
                with col2:
                    # Calculate duration statistics
                    durations = []
                    for _, row in filtered_df_temp.iterrows():
                        if pd.notna(row['timeBox_startTime_time']) and pd.notna(row['timeBox_endTime_time']):
                            start_decimal = time_to_decimal_hour(row['timeBox_startTime_time'])
                            end_decimal = time_to_decimal_hour(row['timeBox_endTime_time'])
                            if start_decimal is not None and end_decimal is not None:
                                duration = end_decimal - start_decimal
                                if duration < 0:
                                    duration += 24  # Handle day rollover
                                durations.append(duration)
                    
                    if durations:
                        st.write("**Window Duration Statistics:**")
                        duration_stats = pd.DataFrame({
                            'Statistic': ['Average', 'Median', 'Min', 'Max'],
                            'Hours': [
                                "{:.2f}".format(np.mean(durations)),
                                "{:.2f}".format(np.median(durations)),
                                "{:.2f}".format(np.min(durations)),
                                "{:.2f}".format(np.max(durations))
                            ]
                        })
                        st.dataframe(duration_stats, use_container_width=True, hide_index=True)
        else:
            st.warning("No data matches the selected filters.")
    
    with tab2:
        st.header("Summary Charts")
        
        if len(filtered_df) > 0:
            # Create and display summary charts
            charts = create_summary_charts(filtered_df)
            
            # Display charts in a 2x2 grid
            col1, col2 = st.columns(2)
            
            with col1:
                st.plotly_chart(charts['frequency'], use_container_width=True)
                if 'hourly' in charts:
                    st.plotly_chart(charts['hourly'], use_container_width=True)
            
            with col2:
                st.plotly_chart(charts['providers'], use_container_width=True)
                st.plotly_chart(charts['customers'], use_container_width=True)
        else:
            st.warning("No data matches the selected filters.")
    
    with tab3:
        st.header("Gantt Chart - Scheduling Intensity")
        
        if len(filtered_df) > 0:
            # Date selection for Gantt chart
            gantt_date = None
            if 'timeBox_startTime_date' in filtered_df.columns:
                available_dates = sorted(filtered_df['timeBox_startTime_date'].unique())
                if len(available_dates) > 1:
                    gantt_date = st.selectbox(
                        "Select Date for Gantt Chart",
                        available_dates,
                        index=0
                    )
                else:
                    gantt_date = available_dates[0] if available_dates else None
                    if gantt_date:
                        st.info(f"Showing data for date: {gantt_date}")
            
            with st.spinner("Creating Gantt chart..."):
                intensity_matrix, provider_site_customers, gantt_df = create_gantt_chart_data(filtered_df, gantt_date)
                
                if intensity_matrix is not None:
                    # Create and display the interactive Gantt chart
                    fig = create_interactive_gantt_chart(intensity_matrix, provider_site_customers, gantt_df)
                    
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Display statistics
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            active_hours = np.sum(intensity_matrix.sum(axis=0) > 0)
                            st.metric("Active Hours", f"{active_hours}/24")
                        
                        with col2:
                            peak_hour = np.argmax(intensity_matrix.sum(axis=0))
                            st.metric("Peak Hour", f"{peak_hour:02d}:00")
                        
                        with col3:
                            total_combinations = len(provider_site_customers)
                            st.metric("Provider|Site|Customer Combinations", f"{total_combinations:,}")
                        
                        # Show busiest combinations
                        st.subheader("Top 10 Busiest Provider|Site|Customer Combinations")
                        provider_totals = intensity_matrix.sum(axis=1)
                        top_indices = np.argsort(provider_totals)[-10:][::-1]
                        
                        top_combinations = []
                        for idx in top_indices:
                            if provider_totals[idx] > 0:
                                top_combinations.append({
                                    'Provider|Site|Customer': provider_site_customers[idx],
                                    'Total Records': int(provider_totals[idx])
                                })
                        
                        if top_combinations:
                            top_combinations_df = pd.DataFrame(top_combinations)
                            top_combinations_df['Total Records'] = top_combinations_df['Total Records'].apply(lambda x: "{:,}".format(x))
                            st.dataframe(top_combinations_df, use_container_width=True, hide_index=True)
                    else:
                        st.error("Could not create Gantt chart")
                else:
                    st.warning("No time data available for Gantt chart visualization")
        else:
            st.warning("No data matches the selected filters.")
    
    with tab4:
        st.header("Data Explorer")
        
        if len(filtered_df) > 0:
            # Column selection for display
            st.subheader("Select Columns to Display")
            
            # Default columns to show
            default_cols = [
                'collection_frequency', 'provider', 'site', 'customerCollection_customer',
                'timeBox_startTime_time', 'timeBox_endTime_time', 'row_count'
            ]
            
            available_cols = filtered_df.columns.tolist()
            selected_cols = st.multiselect(
                "Choose columns:",
                available_cols,
                default=[col for col in default_cols if col in available_cols]
            )
            
            if selected_cols:
                # Display options
                col1, col2 = st.columns(2)
                with col1:
                    show_rows = st.number_input("Number of rows to display", min_value=10, max_value=len(filtered_df), value=min(50, len(filtered_df)))
                with col2:
                    sort_by = st.selectbox("Sort by column", selected_cols, index=selected_cols.index('row_count') if 'row_count' in selected_cols else 0)
                
                # Sort and display data
                display_df = filtered_df[selected_cols].sort_values(sort_by, ascending=False).head(int(show_rows))
                
                # Format time columns for better readability
                for col in selected_cols:
                    if 'time' in col.lower() and col.endswith('_time'):
                        display_df[col] = display_df[col].apply(convert_time_to_hour_minute)
                
                # Format numeric columns with commas
                for col in selected_cols:
                    if pd.api.types.is_numeric_dtype(display_df[col]):
                        display_df[col] = display_df[col].apply(lambda x: "{:,}".format(x) if pd.notna(x) else x)
                
                st.dataframe(display_df, use_container_width=True, hide_index=True)
                
                # Download option
                csv = display_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download filtered data as CSV",
                    data=csv,
                    file_name=f"filtered_autoscheduler_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.warning("Please select at least one column to display.")
        else:
            st.warning("No data matches the selected filters.")
    
    # Footer
    st.markdown("---")
    st.markdown("**Autoscheduler Data Explorer** | Built with Streamlit 🚀")

if __name__ == "__main__":
    main() 