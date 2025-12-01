# 🌦️ Sri Lanka Weather Analytics Dashboard

Interactive web dashboard for visualizing Sri Lankan weather data analysis results.

## 🚀 Quick Start

```bash
cd weather-dashboard
docker-compose up -d
```

Access at: **http://localhost:8050**

## 📊 Features

### Page 1: Precipitation Analysis
- Heatmap (District × Month)
- Seasonal comparison (Maha vs Yala)
- Time series trends

### Page 2: Top 5 Districts
- Bar chart rankings
- Pie chart distribution
- Yearly trends

### Page 3: Temperature Analysis
- Heatmap (District × Year)
- % of months >30°C
- Climate trend analysis

### Page 4: Extreme Weather Events
- Event count by district
- Scatter plot (Precipitation vs Wind)
- Severity classification

## 🔧 Setup

The dashboard connects to your existing ClickHouse database via the `hadoop` network.

### Load Raw Data for Page 4

```bash
docker exec -it weather-dashboard python utils/load_raw_data.py
```

## 🛠️ Commands

```bash
# Start
docker-compose up -d

# Stop
docker-compose down

# View logs
docker-compose logs -f dashboard

# Restart
docker-compose restart
```

## 📊 Data Requirements

- **district_monthly_weather**: 4,698 records (Pages 1-3)
- **raw_weather_data**: 142K+ records (Page 4)
- **locations**: 27 records (Page 4)

## 🐛 Troubleshooting

```bash
# Check container
docker ps | grep weather-dashboard

# Test ClickHouse connection
docker exec weather-dashboard ping clickhouse

# Check data
docker exec clickhouse clickhouse-client --query "SELECT count() FROM weather_analytics.district_monthly_weather"
```

---

**Tech Stack:** Python, Plotly Dash, ClickHouse, Docker
