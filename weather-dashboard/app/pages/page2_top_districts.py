"""
Page 2: Top 5 Districts by Precipitation
Analysis of districts with highest total precipitation
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
            html.H1("🏆 Top 5 Districts by Precipitation", className="text-primary mb-3"),
            html.P("Districts with the highest total precipitation amounts",
                   className="lead text-muted")
        ])
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("Filters")),
                dbc.CardBody([
                    html.Label("Select Year Range:", className="fw-bold"),
                    dcc.RangeSlider(
                        id='year-slider-p2',
                        min=2010,
                        max=2024,
                        value=[2010, 2024],
                        marks={i: str(i) for i in range(2010, 2025, 2)},
                        step=1
                    ),
                    html.Br(),
                    html.Label("Ranking Metric:", className="fw-bold"),
                    dcc.RadioItems(
                        id='metric-selector',
                        options=[
                            {'label': ' Total Precipitation', 'value': 'total'},
                            {'label': ' Average Monthly Precipitation', 'value': 'average'}
                        ],
                        value='total',
                        inline=True
                    )
                ])
            ], className="mb-4")
        ], md=12)
    ]),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-bar-chart",
                children=[dcc.Graph(id='top5-bar-chart')],
                type="default"
            )
        ], md=6),
        dbc.Col([
            dcc.Loading(
                id="loading-pie-chart",
                children=[dcc.Graph(id='top5-pie-chart')],
                type="default"
            )
        ], md=6)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-yearly-trends",
                children=[dcc.Graph(id='yearly-trends-top5')],
                type="default"
            )
        ], md=12)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-comparison-table",
                children=[html.Div(id='comparison-table')],
                type="default"
            )
        ], md=6),
        dbc.Col([
            html.H4("📊 Statistics Summary", className="text-primary"),
            html.Div(id='top5-statistics')
        ], md=6)
    ])
], fluid=True)

# Callback to update all visualizations
@callback(
    [Output('top5-bar-chart', 'figure'),
     Output('top5-pie-chart', 'figure'),
     Output('yearly-trends-top5', 'figure'),
     Output('comparison-table', 'children'),
     Output('top5-statistics', 'children')],
    [Input('year-slider-p2', 'value'),
     Input('metric-selector', 'value')]
)
def update_top5_charts(year_range, metric):
    """Update all Top 5 districts visualizations"""

    try:
        # Query for top 5 districts
        if metric == 'total':
            order_by = 'total_precip_hours DESC'
            metric_label = 'Total Precipitation (hours)'
            metric_col = 'total_precip_hours'
        else:
            order_by = 'avg_monthly_precip DESC'
            metric_label = 'Avg Monthly Precipitation (hours)'
            metric_col = 'avg_monthly_precip'

        query_top5 = f"""
        SELECT
            district,
            ROUND(SUM(total_precipitation_hours), 2) as total_precip_hours,
            ROUND(AVG(total_precipitation_hours), 2) as avg_monthly_precip,
            ROUND(MIN(total_precipitation_hours), 2) as min_monthly_precip,
            ROUND(MAX(total_precipitation_hours), 2) as max_monthly_precip,
            COUNT(*) as month_count,
            MIN(year) as first_year,
            MAX(year) as last_year
        FROM district_monthly_weather
        WHERE year BETWEEN {year_range[0]} AND {year_range[1]}
        GROUP BY district
        ORDER BY {order_by}
        LIMIT 5
        """
        df_top5 = execute_query(query_top5)

        if len(df_top5) == 0:
            raise Exception("No data available for selected years")

        # Get top 5 district names
        top5_districts = df_top5['district'].tolist()

        # Query yearly breakdown for top 5
        districts_str = "', '".join(top5_districts)
        query_yearly = f"""
        SELECT
            district,
            year,
            ROUND(SUM(total_precipitation_hours), 2) as yearly_precip,
            ROUND(AVG(total_precipitation_hours), 2) as avg_monthly_precip
        FROM district_monthly_weather
        WHERE district IN ('{districts_str}')
          AND year BETWEEN {year_range[0]} AND {year_range[1]}
        GROUP BY district, year
        ORDER BY district, year
        """
        df_yearly = execute_query(query_yearly)

        # Create horizontal bar chart
        fig_bar = px.bar(
            df_top5,
            x=metric_col,
            y='district',
            orientation='h',
            title=f"Top 5 Districts by {metric_label}",
            labels={metric_col: metric_label, 'district': 'District'},
            color=metric_col,
            color_continuous_scale='Blues',
            text=metric_col
        )
        fig_bar.update_traces(texttemplate='%{text:.2f}', textposition='outside')
        fig_bar.update_layout(height=400, showlegend=False)

        # Create pie chart
        fig_pie = px.pie(
            df_top5,
            values=metric_col,
            names='district',
            title=f"Distribution of {metric_label} Among Top 5",
            hole=0.3
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie.update_layout(height=400)

        # Create yearly trends line chart
        fig_trends = px.line(
            df_yearly,
            x='year',
            y='yearly_precip',
            color='district',
            title="Yearly Precipitation Trends for Top 5 Districts",
            labels={'yearly_precip': 'Yearly Precipitation (hours)', 'year': 'Year'},
            markers=True
        )
        fig_trends.update_layout(height=500, hovermode='x unified')

        # Create comparison table
        table_data = []
        for idx, row in df_top5.iterrows():
            table_data.append(html.Tr([
                html.Td(idx + 1, className="text-center fw-bold"),
                html.Td(row['district'], className="fw-bold"),
                html.Td(f"{row['total_precip_hours']:,.2f}"),
                html.Td(f"{row['avg_monthly_precip']:.2f}"),
                html.Td(f"{row['min_monthly_precip']:.2f}"),
                html.Td(f"{row['max_monthly_precip']:.2f}"),
                html.Td(row['month_count'])
            ]))

        comparison_table = dbc.Card([
            dbc.CardHeader(html.H5("📋 Detailed Comparison")),
            dbc.CardBody([
                dbc.Table([
                    html.Thead(html.Tr([
                        html.Th("Rank", className="text-center"),
                        html.Th("District"),
                        html.Th("Total (hrs)"),
                        html.Th("Avg (hrs)"),
                        html.Th("Min (hrs)"),
                        html.Th("Max (hrs)"),
                        html.Th("Months")
                    ])),
                    html.Tbody(table_data)
                ], bordered=True, hover=True, responsive=True, striped=True)
            ])
        ])

        # Generate statistics
        top_district = df_top5.iloc[0]
        total_all_top5 = df_top5[metric_col].sum()
        avg_all_top5 = df_top5[metric_col].mean()

        # Calculate percentage difference between #1 and #5
        pct_diff = ((df_top5.iloc[0][metric_col] - df_top5.iloc[4][metric_col]) / df_top5.iloc[4][metric_col]) * 100

        statistics = dbc.Card([
            dbc.CardBody([
                html.Ul([
                    html.Li([
                        html.Strong("Top District: "),
                        f"{top_district['district']} with {top_district[metric_col]:,.2f} hours"
                    ]),
                    html.Li([
                        html.Strong("Top 5 Combined: "),
                        f"{total_all_top5:,.2f} hours total"
                    ]),
                    html.Li([
                        html.Strong("Top 5 Average: "),
                        f"{avg_all_top5:,.2f} hours"
                    ]),
                    html.Li([
                        html.Strong("#1 vs #5 Difference: "),
                        f"{pct_diff:.1f}% higher"
                    ]),
                    html.Li([
                        html.Strong("Analysis Period: "),
                        f"{year_range[0]} - {year_range[1]} ({year_range[1] - year_range[0] + 1} years)"
                    ])
                ])
            ])
        ], color="light")

        return fig_bar, fig_pie, fig_trends, comparison_table, statistics

    except Exception as e:
        print(f"Error updating top 5 charts: {e}")
        import traceback
        traceback.print_exc()

        # Return empty figures
        empty_fig = go.Figure()
        empty_fig.add_annotation(text="Error loading data", showarrow=False)

        error_msg = dbc.Alert(f"Error: {str(e)}", color="danger")

        return empty_fig, empty_fig, empty_fig, error_msg, error_msg
