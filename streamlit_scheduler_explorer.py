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

def create_gantt_chart_data(df):
    """Create intensity matrix and labels for multi-day Gantt chart.

    Parameters
    ----------
    df : pd.DataFrame
        Filtered dataframe containing at least the following columns:
        - provider, site, customerCollection_customer
        - timeBox_startTime_date, timeBox_startTime_time, timeBox_endTime_time
        - hourly_collection_plan_id, row_count

    Returns
    -------
    intensity_matrix : np.ndarray | None
        Matrix of shape (n_provider_site_customer, n_dates*24) with record counts.
    provider_site_customers : list[str] | None
        Ordered list of provider|site|customer identifiers.
    x_date_labels : list[str] | None
        Repeated date labels for multi-category x-axis.
    x_hour_labels : list[str] | None
        Repeated hour labels ("HH:00") for multi-category x-axis.
    """

    # Ensure required columns are present
    required_cols = {
        'provider', 'site', 'customerCollection_customer',
        'timeBox_startTime_date', 'timeBox_startTime_time', 'timeBox_endTime_time',
        'hourly_collection_plan_id', 'row_count'
    }
    if not required_cols.issubset(set(df.columns)):
        return None, None, None, None

    df_filtered = df.copy()

    # Combine provider, site, customer for Y-axis
    df_filtered['provider_site_customer'] = (
        df_filtered['provider'].astype(str) + '|' +
        df_filtered['site'].fillna('N/A').astype(str) + '|' +
        df_filtered['customerCollection_customer'].fillna('N/A').astype(str)
    )

    # Helper conversion
    df_filtered['start_decimal'] = df_filtered['timeBox_startTime_time'].apply(time_to_decimal_hour)
    df_filtered['end_decimal'] = df_filtered['timeBox_endTime_time'].apply(time_to_decimal_hour)

    # If time conversion failed, drop those rows
    df_filtered = df_filtered[pd.notna(df_filtered['start_decimal']) & pd.notna(df_filtered['end_decimal'])]
    if df_filtered.empty:
        return None, None, None, None

    # Adjust for windows that roll over midnight
    df_filtered['end_decimal_adjusted'] = df_filtered.apply(
        lambda r: r['end_decimal'] + 24 if r['end_decimal'] < r['start_decimal'] else r['end_decimal'], axis=1
    )

    # Unique dates sorted
    dates = sorted(df_filtered['timeBox_startTime_date'].dropna().unique().tolist())
    n_dates = len(dates)
    if n_dates == 0:
        return None, None, None, None
    date_to_idx = {d: i for i, d in enumerate(dates)}

    # Provider ordering for stable Y-axis
    provider_ordering = df_filtered.groupby('provider_site_customer')['hourly_collection_plan_id'].first().sort_values()
    provider_site_customers = provider_ordering.index.tolist()

    # Initialize intensity matrix
    intensity_matrix = np.zeros((len(provider_site_customers), n_dates * 24))

    # Populate matrix
    for _, row in df_filtered.iterrows():
        provider_idx = provider_site_customers.index(row['provider_site_customer'])
        date_idx = date_to_idx.get(row['timeBox_startTime_date'])
        if date_idx is None:
            continue

        start_hr = int(row['start_decimal'])
        end_hr = int(row['end_decimal_adjusted'])

        for hr in range(start_hr, end_hr + 1):
            target_date_idx = date_idx
            hour_within_day = hr
            if hr >= 24:
                target_date_idx += hr // 24
                hour_within_day = hr % 24
            if target_date_idx >= n_dates:
                break  # Skip overflow beyond available dates
            col_idx = target_date_idx * 24 + hour_within_day
            intensity_matrix[provider_idx, col_idx] += row['row_count']

    # Build multi-category labels
    x_date_labels = []
    x_hour_labels = []
    for d in dates:
        for hr in range(24):
            x_date_labels.append(str(d))
            x_hour_labels.append(f"{hr:02d}:00")

    return intensity_matrix, provider_site_customers, x_date_labels, x_hour_labels

def create_interactive_gantt_chart(intensity_matrix, provider_site_customers, x_date_labels, x_hour_labels):
    """Create interactive multi-day Gantt chart with intensity using Plotly"""
    if intensity_matrix is None or len(provider_site_customers) == 0:
        st.warning("No data available for Gantt chart visualization")
        return None

    # Multi-category X-axis (date, hour)
    x_multi = [x_date_labels, x_hour_labels]

    # Log scale for intensity
    log_intensity = np.log1p(intensity_matrix)

    hover_text = [[f"{val:,.0f}" for val in row] for row in intensity_matrix]

    fig = go.Figure(data=go.Heatmap(
        z=log_intensity,
        x=x_multi,
        y=provider_site_customers[::-1],  # Reverse Y-axis order for readability
        colorscale='Reds',
        hoverongaps=False,
        hovertemplate='<b>%{y}</b><br>' +
                      'Date / Hour: %{x}<br>' +
                      'Log(Records + 1): %{z:.2f}<br>' +
                      'Raw Records: %{customdata}<extra></extra>',
        customdata=hover_text,
        showscale=True
    ))

    fig.update_layout(
        title={
            'text': f'Scheduling Intensity Heatmap - {len(provider_site_customers)} Provider|Site|Customer Combinations',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='Date / Hour',
        yaxis_title='Provider | Site | Customer',
        height=max(400, len(provider_site_customers) * 25),
        font=dict(size=12),
        margin=dict(l=300, r=50, t=80, b=50)
    )

    fig.update_xaxes(type='multicategory', tickangle=45)
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
    
    # Collection Name filter
    if 'customerCollection_name' in df.columns:
        collection_names = ['All'] + sorted(df['customerCollection_name'].fillna('N/A').unique().tolist())
        selected_collection_name = st.sidebar.selectbox(
            "Collection Name",
            collection_names,
            index=0
        )
    else:
        selected_collection_name = 'All'
    
    # SiteHierarchy Priority filter
    if 'siteHierarchy_priority' in df.columns:
        priorities = ['All'] + sorted(df['siteHierarchy_priority'].fillna('N/A').unique().tolist())
        selected_priority = st.sidebar.selectbox(
            "SiteHierarchy Priority",
            priorities,
            index=0
        )
    else:
        selected_priority = 'All'
    
    # Customer Site Code filter
    if 'siteHierarchy_customerSiteCode' in df.columns:
        customer_site_codes = ['All'] + sorted(df['siteHierarchy_customerSiteCode'].fillna('N/A').unique().tolist())
        selected_customer_site_code = st.sidebar.selectbox(
            "Customer Site Code",
            customer_site_codes,
            index=0
        )
    else:
        selected_customer_site_code = 'All'
    
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
    
    if selected_collection_name != 'All' and 'customerCollection_name' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['customerCollection_name'].fillna('N/A') == selected_collection_name]
    
    if selected_priority != 'All' and 'siteHierarchy_priority' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['siteHierarchy_priority'].fillna('N/A') == selected_priority]
    
    if selected_customer_site_code != 'All' and 'siteHierarchy_customerSiteCode' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['siteHierarchy_customerSiteCode'].fillna('N/A') == selected_customer_site_code]
    
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
    tab1, tab2, tab3 = st.tabs(["📊 Data Overview", "📈 Summary Charts", "📅 Gantt Chart"])
    
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
            # Date selection for Gantt chart (removed to allow multi-day view)
            gantt_date = None

            with st.spinner("Creating Gantt chart..."):
                intensity_matrix, provider_site_customers, x_date_labels, x_hour_labels = create_gantt_chart_data(filtered_df)
                
                if intensity_matrix is not None:
                    # Create and display the interactive Gantt chart
                    fig = create_interactive_gantt_chart(intensity_matrix, provider_site_customers, x_date_labels, x_hour_labels)
                    
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Display statistics
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            active_hours = np.sum(intensity_matrix.sum(axis=0) > 0)
                            total_hours = intensity_matrix.shape[1]
                            st.metric("Active Hours", f"{active_hours}/{total_hours}")
                        
                        with col2:
                            peak_hour_idx = np.argmax(intensity_matrix.sum(axis=0))
                            day_of_peak = peak_hour_idx // 24
                            hour_of_peak = peak_hour_idx % 24
                            peak_label = f"{x_date_labels[peak_hour_idx]} {hour_of_peak:02d}:00"
                            st.metric("Peak Hour", peak_label)
                        
                        with col3:
                            total_combinations = len(provider_site_customers)
                            st.metric("Provider|Site|Customer Combinations", f"{total_combinations:,}")
                        
        else:
            st.warning("No data matches the selected filters.")
    
    # Footer
    st.markdown("---")
    st.markdown("**Autoscheduler Data Explorer** | Built with Streamlit 🚀")

if __name__ == "__main__":
    main() 