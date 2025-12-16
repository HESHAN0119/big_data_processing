

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
            html.H1(" Temperature Analysis (>30°C)", className="text-primary mb-3"),
            html.P("Percentage of months with mean temperature above 30°C across districts and years",
                   className="lead text-muted")
        ])
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("Filters")),
                dbc.CardBody([
                    html.Label("Select District(s):", className="fw-bold"),
                    dcc.Dropdown(
                        id='district-dropdown-p3',
                        multi=True,
                        placeholder="Select districts (leave empty for all)"
                    ),
                    html.Br(),
                    html.Label("Temperature Threshold (°C):", className="fw-bold"),
                    dcc.Slider(
                        id='temp-threshold',
                        min=25,
                        max=35,
                        value=30,
                        marks={i: f"{i}°C" for i in range(25, 36, 1)},
                        step=0.5,
                        tooltip={"placement": "bottom", "always_visible": True}
                    )
                ])
            ], className="mb-4")
        ], md=12)
    ]),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-heatmap-temp",
                children=[dcc.Graph(id='temp-heatmap')],
                type="default"
            )
        ], md=12)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-area-chart",
                children=[dcc.Graph(id='temp-area-chart')],
                type="default"
            )
        ], md=6),
        dbc.Col([
            dcc.Loading(
                id="loading-bar-districts",
                children=[dcc.Graph(id='temp-bar-districts')],
                type="default"
            )
        ], md=6)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-yearly-trend",
                children=[dcc.Graph(id='temp-yearly-trend')],
                type="default"
            )
        ], md=12)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            html.H4("🔥 Climate Insights", className="text-primary"),
            html.Div(id='temperature-insights')
        ], md=6),
        dbc.Col([
            html.H4("📊 Statistics", className="text-primary"),
            html.Div(id='temperature-statistics')
        ], md=6)
    ])
], fluid=True)

# Callback to populate district dropdown
@callback(
    Output('district-dropdown-p3', 'options'),
    Input('temp-threshold', 'value')
)
def update_district_options_p3(threshold):
    """Get list of districts"""
    try:
        query = "SELECT DISTINCT district FROM district_monthly_weather ORDER BY district"
        df = execute_query(query)
        options = [{'label': d, 'value': d} for d in df['district'].tolist()]
        return options
    except Exception as e:
        print(f"Error loading districts: {e}")
        return []

# Callback to update all visualizations
@callback(
    [Output('temp-heatmap', 'figure'),
     Output('temp-area-chart', 'figure'),
     Output('temp-bar-districts', 'figure'),
     Output('temp-yearly-trend', 'figure'),
     Output('temperature-insights', 'children'),
     Output('temperature-statistics', 'children')],
    [Input('district-dropdown-p3', 'value'),
     Input('temp-threshold', 'value')]
)
def update_temperature_charts(selected_districts, threshold):
    """Update all temperature analysis visualizations"""

    try:
        # Build WHERE clause
        where_clauses = []

        if selected_districts and len(selected_districts) > 0:
            districts_str = "', '".join(selected_districts)
            where_clauses.append(f"district IN ('{districts_str}')")

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Query 1: Heatmap data (district x year)
        query_heatmap = f"""
        SELECT
            district,
            year,
            ROUND(SUM(CASE WHEN mean_temperature > {threshold} THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as percentage
        FROM district_monthly_weather
        WHERE {where_clause}
        GROUP BY district, year
        ORDER BY district, year
        """
        df_heatmap = execute_query(query_heatmap)

        # Query 2: By district and year (for area chart and bar)
        query_by_year = f"""
        SELECT
            district,
            year,
            COUNT(*) as total_months,
            SUM(CASE WHEN mean_temperature > {threshold} THEN 1 ELSE 0 END) as hot_months,
            ROUND(SUM(CASE WHEN mean_temperature > {threshold} THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as percentage,
            ROUND(AVG(mean_temperature), 2) as avg_temp,
            ROUND(MAX(mean_temperature), 2) as max_temp
        FROM district_monthly_weather
        WHERE {where_clause}
        GROUP BY district, year
        ORDER BY district, year
        """
        df_by_year = execute_query(query_by_year)

        # Query 3: Overall by district
        query_by_district = f"""
        SELECT
            district,
            COUNT(*) as total_months,
            SUM(CASE WHEN mean_temperature > {threshold} THEN 1 ELSE 0 END) as hot_months,
            ROUND(SUM(CASE WHEN mean_temperature > {threshold} THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as percentage,
            ROUND(AVG(mean_temperature), 2) as avg_temp,
            ROUND(MIN(mean_temperature), 2) as min_temp,
            ROUND(MAX(mean_temperature), 2) as max_temp
        FROM district_monthly_weather
        WHERE {where_clause}
        GROUP BY district
        ORDER BY percentage DESC
        """
        df_by_district = execute_query(query_by_district)

        # Query 4: Year-over-year trend
        query_yearly_trend = f"""
        SELECT
            year,
            COUNT(DISTINCT district) as districts_count,
            ROUND(AVG(CASE WHEN mean_temperature > {threshold} THEN 100.0 ELSE 0.0 END), 2) as avg_percentage,
            SUM(CASE WHEN mean_temperature > {threshold} THEN 1 ELSE 0 END) as total_hot_months,
            COUNT(*) as total_months,
            ROUND(AVG(mean_temperature), 2) as avg_temp,
            ROUND(MAX(mean_temperature), 2) as max_temp_recorded
        FROM district_monthly_weather
        WHERE {where_clause}
        GROUP BY year
        ORDER BY year
        """
        df_yearly_trend = execute_query(query_yearly_trend)

        # Create heatmap
        df_heatmap_pivot = df_heatmap.pivot(index='district', columns='year', values='percentage')

        fig_heatmap = px.imshow(
            df_heatmap_pivot,
            labels=dict(x="Year", y="District", color=f"% Months >{threshold}°C"),
            title=f"Percentage of Months with Temperature >{threshold}°C (by District and Year)",
            color_continuous_scale="YlOrRd",
            aspect="auto"
        )
        fig_heatmap.update_layout(height=600)

        # Create area chart (year trend by district)
        fig_area = px.area(
            df_by_year,
            x='year',
            y='percentage',
            color='district',
            title=f"Trend of Hot Months (>{threshold}°C) Over Time",
            labels={'percentage': '% of Months', 'year': 'Year'}
        )
        fig_area.update_layout(height=400, hovermode='x unified')

        # Create bar chart (by district overall)
        fig_bar = px.bar(
            df_by_district.head(10),
            x='percentage',
            y='district',
            orientation='h',
            title=f"Top Districts by % of Months >{threshold}°C",
            labels={'percentage': '% of Months', 'district': 'District'},
            color='percentage',
            color_continuous_scale='YlOrRd',
            text='percentage'
        )
        fig_bar.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
        fig_bar.update_layout(height=400, showlegend=False)

        # Create yearly trend line chart
        fig_yearly = go.Figure()
        fig_yearly.add_trace(go.Scatter(
            x=df_yearly_trend['year'],
            y=df_yearly_trend['avg_percentage'],
            mode='lines+markers',
            name=f'% Months >{threshold}°C',
            line=dict(color='red', width=3),
            marker=dict(size=8)
        ))
        fig_yearly.add_trace(go.Scatter(
            x=df_yearly_trend['year'],
            y=df_yearly_trend['avg_temp'],
            mode='lines+markers',
            name='Avg Temperature (°C)',
            line=dict(color='orange', width=2, dash='dash'),
            marker=dict(size=6),
            yaxis='y2'
        ))
        fig_yearly.update_layout(
            title="Climate Change Trend: Hot Months vs Average Temperature",
            xaxis_title="Year",
            yaxis_title=f"% of Months >{threshold}°C",
            yaxis2=dict(
                title="Average Temperature (°C)",
                overlaying='y',
                side='right'
            ),
            height=500,
            hovermode='x unified'
        )

        # Generate insights
        if len(df_by_district) > 0:
            # Hottest district
            hottest = df_by_district.iloc[0]
            coolest = df_by_district.iloc[-1]

            # Year with most hot months
            hottest_year = df_yearly_trend.nlargest(1, 'avg_percentage').iloc[0]
            coolest_year = df_yearly_trend.nsmallest(1, 'avg_percentage').iloc[0]

            # Trend calculation (first 3 years vs last 3 years)
            first_3_avg = df_yearly_trend.head(3)['avg_percentage'].mean()
            last_3_avg = df_yearly_trend.tail(3)['avg_percentage'].mean()
            trend_change = last_3_avg - first_3_avg

            insights = dbc.Card([
                dbc.CardBody([
                    html.Ul([
                        html.Li([
                            html.Strong("Hottest District: "),
                            f"{hottest['district']} with {hottest['percentage']:.1f}% of months above {threshold}°C"
                        ]),
                        html.Li([
                            html.Strong("Coolest District: "),
                            f"{coolest['district']} with {coolest['percentage']:.1f}% of months above {threshold}°C"
                        ]),
                        html.Li([
                            html.Strong("Hottest Year: "),
                            f"{int(hottest_year['year'])} with {hottest_year['avg_percentage']:.1f}% hot months"
                        ]),
                        html.Li([
                            html.Strong("Coolest Year: "),
                            f"{int(coolest_year['year'])} with {coolest_year['avg_percentage']:.1f}% hot months"
                        ]),
                        html.Li([
                            html.Strong("Climate Trend: "),
                            f"{'Warming' if trend_change > 0 else 'Cooling'} trend of {abs(trend_change):.1f}% points from 2010-2012 to 2022-2024"
                        ])
                    ])
                ])
            ], color="light")

            # Statistics card
            total_months_analyzed = df_by_district['total_months'].sum()
            total_hot_months = df_by_district['hot_months'].sum()
            overall_percentage = (total_hot_months / total_months_analyzed) * 100

            statistics = dbc.Card([
                dbc.CardBody([
                    html.Ul([
                        html.Li([
                            html.Strong("Temperature Threshold: "),
                            f"{threshold}°C"
                        ]),
                        html.Li([
                            html.Strong("Districts Analyzed: "),
                            f"{len(df_by_district)}"
                        ]),
                        html.Li([
                            html.Strong("Total Months: "),
                            f"{total_months_analyzed:,}"
                        ]),
                        html.Li([
                            html.Strong("Hot Months: "),
                            f"{int(total_hot_months):,} ({overall_percentage:.1f}%)"
                        ]),
                        html.Li([
                            html.Strong("Highest Max Temp: "),
                            f"{df_by_district['max_temp'].max():.1f}°C"
                        ])
                    ])
                ])
            ], color="info", outline=True)
        else:
            insights = dbc.Alert("No data available.", color="warning")
            statistics = dbc.Alert("No statistics available.", color="warning")

        return fig_heatmap, fig_area, fig_bar, fig_yearly, insights, statistics

    except Exception as e:
        print(f"Error updating temperature charts: {e}")
        import traceback
        traceback.print_exc()

        empty_fig = go.Figure()
        empty_fig.add_annotation(text="Error loading data", showarrow=False)
        error_msg = dbc.Alert(f"Error: {str(e)}", color="danger")

        return empty_fig, empty_fig, empty_fig, empty_fig, error_msg, error_msg
