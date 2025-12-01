"""
Page 4: Extreme Weather Events
Days with extreme weather events (high precipitation + high wind gusts)
"""

import dash
from dash import html, dcc, callback
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connection import execute_query

# Layout
layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("⚠️ Extreme Weather Events Analysis", className="text-primary mb-3"),
            html.P("Days with extreme weather conditions (high precipitation AND high wind gusts)",
                   className="lead text-muted")
        ])
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("Event Definition Thresholds")),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Precipitation Threshold (mm/day):", className="fw-bold"),
                            dcc.Slider(
                                id='precip-threshold',
                                min=20,
                                max=100,
                                value=30,
                                marks={i: f"{i}mm" for i in range(20, 101, 10)},
                                step=5,
                                tooltip={"placement": "bottom", "always_visible": True}
                            )
                        ], md=6),
                        dbc.Col([
                            html.Label("Wind Gust Threshold (km/h):", className="fw-bold"),
                            dcc.Slider(
                                id='wind-threshold',
                                min=40,
                                max=100,
                                value=50,
                                marks={i: f"{i}" for i in range(40, 101, 10)},
                                step=5,
                                tooltip={"placement": "bottom", "always_visible": True}
                            )
                        ], md=6)
                    ])
                ])
            ], className="mb-4")
        ], md=12)
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Alert([
                html.I(className="fas fa-info-circle me-2"),
                html.Strong("Data Availability: "),
                "This analysis requires raw daily weather data. ",
                html.Span(id='data-status')
            ], color="info", id='data-alert')
        ])
    ], className="mb-3"),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-events-bar",
                children=[dcc.Graph(id='extreme-events-bar')],
                type="default"
            )
        ], md=6),
        dbc.Col([
            dcc.Loading(
                id="loading-events-timeline",
                children=[dcc.Graph(id='extreme-events-timeline')],
                type="default"
            )
        ], md=6)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-scatter",
                children=[dcc.Graph(id='precip-wind-scatter')],
                type="default"
            )
        ], md=12)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-monthly-dist",
                children=[dcc.Graph(id='monthly-distribution')],
                type="default"
            )
        ], md=6),
        dbc.Col([
            dcc.Loading(
                id="loading-severity-pie",
                children=[dcc.Graph(id='severity-pie')],
                type="default"
            )
        ], md=6)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            html.H4("⚡ Event Summary", className="text-primary"),
            html.Div(id='extreme-weather-summary')
        ], md=12)
    ])
], fluid=True)

# Callback to check data availability and update all visualizations
@callback(
    [Output('extreme-events-bar', 'figure'),
     Output('extreme-events-timeline', 'figure'),
     Output('precip-wind-scatter', 'figure'),
     Output('monthly-distribution', 'figure'),
     Output('severity-pie', 'figure'),
     Output('extreme-weather-summary', 'children'),
     Output('data-status', 'children'),
     Output('data-alert', 'color')],
    [Input('precip-threshold', 'value'),
     Input('wind-threshold', 'value')]
)
def update_extreme_weather_charts(precip_threshold, wind_threshold):
    """Update extreme weather analysis visualizations"""

    try:
        # First, check if raw_weather_data has data
        check_query = "SELECT count() as cnt FROM raw_weather_data"
        df_check = execute_query(check_query)
        data_count = df_check['cnt'].iloc[0]

        if data_count == 0:
            # No data available
            empty_fig = go.Figure()
            empty_fig.add_annotation(
                text="⚠️ No raw weather data available<br>Please run the data loading script first",
                showarrow=False,
                font=dict(size=16, color="orange")
            )
            empty_fig.update_layout(height=400)

            alert_msg = "No raw weather data found in database. Please load data first using the load_raw_data.py script."
            alert_color = "warning"

            summary = dbc.Alert([
                html.H5("⚠️ Data Loading Required"),
                html.P("The raw_weather_data table is empty. To enable extreme weather analysis:"),
                html.Ol([
                    html.Li("Navigate to the dashboard container"),
                    html.Li("Run: python utils/load_raw_data.py"),
                    html.Li("Refresh this page")
                ]),
                html.P("Alternatively, check the README.md for detailed instructions.", className="mb-0")
            ], color="warning")

            return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, summary, alert_msg, alert_color

        # Data is available, proceed with queries
        alert_msg = f"✓ {data_count:,} daily records available for analysis"
        alert_color = "success"

        # Query 1: Extreme events by district
        query_by_district = f"""
        SELECT
            l.city_name as district,
            COUNT(*) as extreme_days,
            ROUND(AVG(w.precipitation_sum), 2) as avg_precip,
            ROUND(AVG(w.wind_gusts_10m_max), 2) as avg_wind,
            ROUND(MAX(w.precipitation_sum), 2) as max_precip,
            ROUND(MAX(w.wind_gusts_10m_max), 2) as max_wind
        FROM raw_weather_data w
        LEFT JOIN locations l ON w.location_id = l.location_id
        WHERE w.precipitation_sum > {precip_threshold}
          AND w.wind_gusts_10m_max > {wind_threshold}
        GROUP BY l.city_name
        ORDER BY extreme_days DESC
        """
        df_by_district = execute_query(query_by_district)

        # Query 2: Extreme events by year
        query_by_year = f"""
        SELECT
            toYear(w.date) as year,
            COUNT(*) as extreme_days,
            COUNT(DISTINCT l.city_name) as affected_districts
        FROM raw_weather_data w
        LEFT JOIN locations l ON w.location_id = l.location_id
        WHERE w.precipitation_sum > {precip_threshold}
          AND w.wind_gusts_10m_max > {wind_threshold}
        GROUP BY year
        ORDER BY year
        """
        df_by_year = execute_query(query_by_year)

        # Query 3: Scatter plot data (all significant events)
        query_scatter = f"""
        SELECT
            l.city_name as district,
            w.date,
            w.precipitation_sum,
            w.wind_gusts_10m_max,
            w.temperature_2m_max,
            CASE
                WHEN w.precipitation_sum > {precip_threshold * 1.5} AND w.wind_gusts_10m_max > {wind_threshold * 1.3} THEN 'Severe'
                WHEN w.precipitation_sum > {precip_threshold} AND w.wind_gusts_10m_max > {wind_threshold} THEN 'Moderate'
                ELSE 'Normal'
            END as severity
        FROM raw_weather_data w
        LEFT JOIN locations l ON w.location_id = l.location_id
        WHERE w.precipitation_sum > {precip_threshold * 0.7}
           OR w.wind_gusts_10m_max > {wind_threshold * 0.8}
        ORDER BY w.date DESC
        LIMIT 1000
        """
        df_scatter = execute_query(query_scatter)

        # Query 4: Monthly distribution
        query_monthly = f"""
        SELECT
            toMonth(w.date) as month,
            COUNT(*) as extreme_days
        FROM raw_weather_data w
        WHERE w.precipitation_sum > {precip_threshold}
          AND w.wind_gusts_10m_max > {wind_threshold}
        GROUP BY month
        ORDER BY month
        """
        df_monthly = execute_query(query_monthly)

        # Create visualizations

        # 1. Bar chart - Extreme days by district
        if len(df_by_district) > 0:
            fig_bar = px.bar(
                df_by_district,
                x='extreme_days',
                y='district',
                orientation='h',
                title=f"Extreme Weather Days by District (Precip>{precip_threshold}mm & Wind>{wind_threshold}km/h)",
                labels={'extreme_days': 'Number of Extreme Days', 'district': 'District'},
                color='extreme_days',
                color_continuous_scale='Reds',
                text='extreme_days'
            )
            fig_bar.update_traces(textposition='outside')
            fig_bar.update_layout(height=400, showlegend=False)
        else:
            fig_bar = go.Figure()
            fig_bar.add_annotation(text="No extreme events found with current thresholds", showarrow=False)

        # 2. Timeline - Extreme days by year
        if len(df_by_year) > 0:
            fig_timeline = go.Figure()
            fig_timeline.add_trace(go.Bar(
                x=df_by_year['year'],
                y=df_by_year['extreme_days'],
                name='Extreme Days',
                marker_color='red'
            ))
            fig_timeline.update_layout(
                title="Extreme Weather Events Timeline (by Year)",
                xaxis_title="Year",
                yaxis_title="Number of Extreme Days",
                height=400
            )
        else:
            fig_timeline = go.Figure()
            fig_timeline.add_annotation(text="No extreme events found", showarrow=False)

        # 3. Scatter plot - Precipitation vs Wind
        if len(df_scatter) > 0:
            fig_scatter = px.scatter(
                df_scatter,
                x='precipitation_sum',
                y='wind_gusts_10m_max',
                color='severity',
                size='temperature_2m_max',
                hover_data=['district', 'date'],
                title="Precipitation vs Wind Gusts (colored by severity)",
                labels={
                    'precipitation_sum': 'Precipitation (mm/day)',
                    'wind_gusts_10m_max': 'Wind Gusts (km/h)',
                    'temperature_2m_max': 'Max Temperature (°C)'
                },
                color_discrete_map={'Normal': 'blue', 'Moderate': 'orange', 'Severe': 'red'}
            )
            # Add threshold lines
            fig_scatter.add_hline(y=wind_threshold, line_dash="dash", line_color="red",
                                annotation_text=f"Wind Threshold: {wind_threshold} km/h")
            fig_scatter.add_vline(x=precip_threshold, line_dash="dash", line_color="blue",
                                annotation_text=f"Precip Threshold: {precip_threshold} mm")
            fig_scatter.update_layout(height=500)
        else:
            fig_scatter = go.Figure()
            fig_scatter.add_annotation(text="No data available", showarrow=False)

        # 4. Monthly distribution
        if len(df_monthly) > 0:
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            df_monthly['month_name'] = df_monthly['month'].apply(lambda x: month_names[int(x)-1])

            fig_monthly_bar = px.bar(
                df_monthly,
                x='month_name',
                y='extreme_days',
                title="Seasonal Pattern of Extreme Weather Events",
                labels={'extreme_days': 'Number of Events', 'month_name': 'Month'},
                color='extreme_days',
                color_continuous_scale='Reds'
            )
            fig_monthly_bar.update_layout(height=400, showlegend=False)
        else:
            fig_monthly_bar = go.Figure()
            fig_monthly_bar.add_annotation(text="No events to display", showarrow=False)

        # 5. Severity pie chart
        if len(df_scatter) > 0:
            severity_counts = df_scatter['severity'].value_counts().reset_index()
            severity_counts.columns = ['severity', 'count']

            fig_pie = px.pie(
                severity_counts,
                values='count',
                names='severity',
                title="Event Severity Distribution",
                color='severity',
                color_discrete_map={'Normal': 'blue', 'Moderate': 'orange', 'Severe': 'red'}
            )
            fig_pie.update_layout(height=400)
        else:
            fig_pie = go.Figure()
            fig_pie.add_annotation(text="No data", showarrow=False)

        # Generate summary
        if len(df_by_district) > 0:
            total_events = df_by_district['extreme_days'].sum()
            most_affected = df_by_district.iloc[0]
            least_affected = df_by_district.iloc[-1] if len(df_by_district) > 1 else most_affected

            # Calculate severity distribution
            severe_count = len(df_scatter[df_scatter['severity'] == 'Severe'])
            moderate_count = len(df_scatter[df_scatter['severity'] == 'Moderate'])

            summary = dbc.Card([
                dbc.CardBody([
                    html.H5("⚡ Event Statistics", className="card-title"),
                    html.Ul([
                        html.Li([
                            html.Strong("Total Extreme Weather Days: "),
                            f"{total_events:,} events recorded"
                        ]),
                        html.Li([
                            html.Strong("Most Affected District: "),
                            f"{most_affected['district']} with {most_affected['extreme_days']} extreme days"
                        ]),
                        html.Li([
                            html.Strong("Least Affected District: "),
                            f"{least_affected['district']} with {least_affected['extreme_days']} extreme days"
                        ]),
                        html.Li([
                            html.Strong("Severity Breakdown: "),
                            f"{severe_count} severe, {moderate_count} moderate events"
                        ]),
                        html.Li([
                            html.Strong("Current Thresholds: "),
                            f"Precipitation >{precip_threshold}mm/day AND Wind >{wind_threshold}km/h"
                        ])
                    ])
                ])
            ], color="light")
        else:
            summary = dbc.Alert(
                f"No extreme weather events found with current thresholds "
                f"(Precip>{precip_threshold}mm, Wind>{wind_threshold}km/h). "
                f"Try lowering the thresholds to see more events.",
                color="info"
            )

        return fig_bar, fig_timeline, fig_scatter, fig_monthly_bar, fig_pie, summary, alert_msg, alert_color

    except Exception as e:
        print(f"Error updating extreme weather charts: {e}")
        import traceback
        traceback.print_exc()

        empty_fig = go.Figure()
        empty_fig.add_annotation(text=f"Error: {str(e)}", showarrow=False, font=dict(color="red"))

        error_msg = f"Error loading data: {str(e)}"
        alert_color = "danger"

        error_summary = dbc.Alert(f"Error: {str(e)}", color="danger")

        return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, error_summary, error_msg, alert_color
