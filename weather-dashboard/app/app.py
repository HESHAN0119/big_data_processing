"""
Weather Analytics Dashboard - Main Application
Multi-page Dash application for visualizing Sri Lankan weather data
"""

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

# Initialize Dash app with Bootstrap theme
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
    suppress_callback_exceptions=True,
    title="Weather Analytics Dashboard"
)

server = app.server

# Import all pages at startup to register their callbacks
from pages import page1_precipitation, page2_top_districts, page3_temperature, page4_extreme_weather

# Navigation bar
navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Precipitation Analysis", href="/", active="exact")),
        dbc.NavItem(dbc.NavLink("Top 5 Districts", href="/top-districts", active="exact")),
        dbc.NavItem(dbc.NavLink("Temperature Analysis", href="/temperature", active="exact")),
        dbc.NavItem(dbc.NavLink("Extreme Weather", href="/extreme-weather", active="exact")),
    ],
    brand="🌦️ Sri Lanka Weather Analytics Dashboard",
    brand_href="/",
    color="primary",
    dark=True,
    className="mb-4"
)

# App layout with URL routing
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    navbar,
    dbc.Container(id='page-content', fluid=True)
])

# Callback to handle page routing
@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    """Route to appropriate page based on URL"""

    if pathname == '/':
        return page1_precipitation.layout

    elif pathname == '/top-districts':
        return page2_top_districts.layout

    elif pathname == '/temperature':
        return page3_temperature.layout

    elif pathname == '/extreme-weather':
        return page4_extreme_weather.layout

    else:
        # 404 page
        return dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("404: Page Not Found", className="text-danger"),
                    html.P("The page you're looking for doesn't exist."),
                    dbc.Button("Go Home", href="/", color="primary")
                ])
            ])
        ])

if __name__ == '__main__':
    print("=" * 60)
    print("🌦️  Weather Analytics Dashboard")
    print("=" * 60)
    print("Starting Dash server...")
    print("Access the dashboard at: http://localhost:8050")
    print("=" * 60)

    # Run the app
    app.run_server(
        host='0.0.0.0',
        port=8050,
        debug=True
    )
