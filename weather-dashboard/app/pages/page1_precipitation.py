"""
Page 1: Precipitation Analysis
Most precipitous month/season for each district across different periods
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

from utils.db_connection import execute_query, get_month_name

# Layout
layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("📊 Precipitation Analysis", className="text-primary mb-3"),
            html.P("Most precipitous month/season for each district across different periods",
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
                        id='district-dropdown-p1',
                        multi=True,
                        placeholder="Select districts (leave empty for all)"
                    ),
                    html.Br(),
                    html.Label("Select Year Range:", className="fw-bold"),
                    dcc.RangeSlider(
                        id='year-slider-p1',
                        min=2010,
                        max=2024,
                        value=[2010, 2024],
                        marks={i: str(i) for i in range(2010, 2025, 2)},
                        step=1
                    )
                ])
            ], className="mb-4")
        ], md=12)
    ]),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-heatmap",
                children=[dcc.Graph(id='precipitation-heatmap')],
                type="default"
            )
        ], md=12)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-seasonal",
                children=[dcc.Graph(id='seasonal-precipitation')],
                type="default"
            )
        ], md=6),
        dbc.Col([
            dcc.Loading(
                id="loading-monthly",
                children=[dcc.Graph(id='monthly-bar-chart')],
                type="default"
            )
        ], md=6)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-trends",
                children=[dcc.Graph(id='precipitation-trends')],
                type="default"
            )
        ], md=12)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            html.H4("📈 Key Insights", className="text-primary"),
            html.Div(id='precipitation-insights')
        ])
    ])
], fluid=True)

# Callback to populate district dropdown
@callback(
    Output('district-dropdown-p1', 'options'),
    Input('year-slider-p1', 'value')
)
def update_district_options(year_range):
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
    [Output('precipitation-heatmap', 'figure'),
     Output('seasonal-precipitation', 'figure'),
     Output('monthly-bar-chart', 'figure'),
     Output('precipitation-trends', 'figure'),
     Output('precipitation-insights', 'children')],
    [Input('district-dropdown-p1', 'value'),
     Input('year-slider-p1', 'value')]
)
def update_precipitation_charts(selected_districts, year_range):
    """Update all precipitation visualizations"""

    try:
        # Build WHERE clause for filters
        where_clauses = [f"year BETWEEN {year_range[0]} AND {year_range[1]}"]

        if selected_districts and len(selected_districts) > 0:
            districts_str = "', '".join(selected_districts)
            where_clauses.append(f"district IN ('{districts_str}')")

        where_clause = " AND ".join(where_clauses)

        # Query 1: Monthly precipitation data for heatmap
        query1 = f"""
        SELECT
            district,
            month,
            ROUND(AVG(total_precipitation_hours), 2) as avg_precip
        FROM district_monthly_weather
        WHERE {where_clause}
        GROUP BY district, month
        ORDER BY district, month
        """
        df_monthly = execute_query(query1)

        # Query 2: Seasonal precipitation
        query2 = f"""
        SELECT
            district,
            CASE
                WHEN month IN (9,10,11,12,1,2,3) THEN 'Maha (Sep-Mar)'
                WHEN month IN (4,5,6,7,8) THEN 'Yala (Apr-Aug)'
            END as season,
            year,
            ROUND(SUM(total_precipitation_hours), 2) as total_precip
        FROM district_monthly_weather
        WHERE {where_clause}
        GROUP BY district, season, year
        ORDER BY district, year, season
        """
        df_seasonal = execute_query(query2)

        # Query 3: Monthly trends for top 5 districts
        # First, get top 5 districts by total precipitation
        query3_top5 = f"""
        SELECT
            district,
            SUM(total_precipitation_hours) as total_precip
        FROM district_monthly_weather
        WHERE {where_clause}
        GROUP BY district
        ORDER BY total_precip DESC
        LIMIT 5
        """
        df_top5_districts = execute_query(query3_top5)

        # Get monthly data for top 5 districts
        if len(df_top5_districts) > 0:
            top5_districts = df_top5_districts['district'].tolist()
            districts_str = "', '".join(top5_districts)

            query3 = f"""
            SELECT
                district,
                year,
                month,
                total_precipitation_hours
            FROM district_monthly_weather
            WHERE {where_clause}
              AND district IN ('{districts_str}')
            ORDER BY district, year, month
            """
            df_trends = execute_query(query3)

            # Create date column for better x-axis (YYYY-MM format)
            df_trends['date'] = pd.to_datetime(
                df_trends['year'].astype(str) + '-' + df_trends['month'].astype(str).str.zfill(2) + '-01'
            )
        else:
            df_trends = pd.DataFrame()

        # Create heatmap
        df_heatmap_pivot = df_monthly.pivot(index='district', columns='month', values='avg_precip')
        df_heatmap_pivot.columns = [get_month_name(m) for m in df_heatmap_pivot.columns]

        fig_heatmap = px.imshow(
            df_heatmap_pivot,
            labels=dict(x="Month", y="District", color="Precipitation (hours)"),
            title="Average Monthly Precipitation by District",
            color_continuous_scale="Blues",
            aspect="auto"
        )
        fig_heatmap.update_layout(height=600)

        # Create seasonal chart
        fig_seasonal = px.bar(
            df_seasonal,
            x='district',
            y='total_precip',
            color='season',
            barmode='group',
            title="Seasonal Precipitation Comparison (Maha vs Yala)",
            labels={'total_precip': 'Total Precipitation (hours)', 'district': 'District'}
        )
        fig_seasonal.update_layout(height=400)

        # Create monthly bar chart (top precipitous months)
        df_top_months = df_monthly.nlargest(10, 'avg_precip')
        df_top_months['month_name'] = df_top_months['month'].apply(get_month_name)

        fig_monthly = px.bar(
            df_top_months,
            x='avg_precip',
            y='district',
            color='month_name',
            orientation='h',
            title="Top 10 Most Precipitous District-Month Combinations",
            labels={'avg_precip': 'Avg Precipitation (hours)', 'district': 'District'}
        )
        fig_monthly.update_layout(height=400, showlegend=True)

        # Create monthly trends line chart for top 5 districts
        if len(df_trends) > 0:
            fig_trends = px.line(
                df_trends,
                x='date',
                y='total_precipitation_hours',
                color='district',
                title="Monthly Precipitation Trends for Top 5 Districts by Total Precipitation",
                labels={
                    'total_precipitation_hours': 'Precipitation (hours)',
                    'date': 'Month-Year',
                    'district': 'District'
                }
            )
            fig_trends.update_layout(
                height=500,
                xaxis=dict(
                    tickformat='%b %Y',
                    dtick='M12'
                )
            )
        else:
            fig_trends = go.Figure()
            fig_trends.add_annotation(text="No data available", showarrow=False)
            fig_trends.update_layout(height=500)

        # Generate insights
        if len(df_monthly) > 0:
            # Most precipitous district-month
            top_row = df_monthly.nlargest(1, 'avg_precip').iloc[0]
            most_precip_district = top_row['district']
            most_precip_month = get_month_name(top_row['month'])
            most_precip_value = top_row['avg_precip']

            # Least precipitous
            least_row = df_monthly.nsmallest(1, 'avg_precip').iloc[0]
            least_precip_district = least_row['district']
            least_precip_month = get_month_name(least_row['month'])
            least_precip_value = least_row['avg_precip']

            # Seasonal comparison
            maha_avg = df_seasonal[df_seasonal['season'] == 'Maha (Sep-Mar)']['total_precip'].mean()
            yala_avg = df_seasonal[df_seasonal['season'] == 'Yala (Apr-Aug)']['total_precip'].mean()

            insights = dbc.Card([
                dbc.CardBody([
                    html.Ul([
                        html.Li([
                            html.Strong("Most Precipitous: "),
                            f"{most_precip_district} in {most_precip_month} with {most_precip_value:.2f} hours on average"
                        ]),
                        html.Li([
                            html.Strong("Least Precipitous: "),
                            f"{least_precip_district} in {least_precip_month} with {least_precip_value:.2f} hours on average"
                        ]),
                        html.Li([
                            html.Strong("Seasonal Comparison: "),
                            f"Maha season (monsoon) averages {maha_avg:.2f} hours vs Yala season (inter-monsoon) {yala_avg:.2f} hours"
                        ]),
                        html.Li([
                            html.Strong("Data Range: "),
                            f"{year_range[0]} - {year_range[1]} ({year_range[1] - year_range[0] + 1} years)"
                        ])
                    ])
                ])
            ], color="light")
        else:
            insights = dbc.Alert("No data available for the selected filters.", color="warning")

        return fig_heatmap, fig_seasonal, fig_monthly, fig_trends, insights

    except Exception as e:
        print(f"Error updating precipitation charts: {e}")
        import traceback
        traceback.print_exc()

        # Return empty figures
        empty_fig = go.Figure()
        empty_fig.add_annotation(text="Error loading data", showarrow=False)

        error_msg = dbc.Alert(f"Error: {str(e)}", color="danger")

        return empty_fig, empty_fig, empty_fig, empty_fig, error_msg
