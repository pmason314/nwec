from datetime import datetime

import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, html


# Create sample data for utility companies
def create_sample_data():
    # Companies
    companies = ["PowerGrid Corp", "EnergyPlus", "GreenUtility", "MetroElectric"]
    # Customer types
    customer_types = ["Residential", "Commercial", "Industrial", "Government"]
    # Dates from 2021-2023
    dates = []
    usage = []
    costs = []
    co2_emissions = []
    outages = []

    company_list = []
    customer_type_list = []

    # Generate 3 years of monthly data
    for year in range(2021, 2024):
        for month in range(1, 13):
            for company in companies:
                for cust_type in customer_types:
                    # Add date
                    dates.append(datetime(year, month, 1))

                    # Add company and customer type
                    company_list.append(company)
                    customer_type_list.append(cust_type)

                    # Generate somewhat realistic data with seasonal patterns
                    # Usage varies by season and customer type
                    base_usage = (
                        100
                        if cust_type == "Residential"
                        else 500
                        if cust_type == "Commercial"
                        else 2000
                        if cust_type == "Industrial"
                        else 300
                    )
                    season_factor = 1.3 if month in [12, 1, 2, 6, 7, 8] else 1.0  # Higher in winter and summer
                    company_factor = 0.9 if company == "GreenUtility" else 1.0  # Green utility has slightly lower usage
                    random_factor = 0.8 + (hash(f"{company}{cust_type}{year}{month}") % 40) / 100

                    monthly_usage = base_usage * season_factor * company_factor * random_factor
                    usage.append(round(monthly_usage, 2))

                    # Cost calculations
                    base_rate = (
                        0.12
                        if company == "PowerGrid Corp"
                        else 0.14
                        if company == "EnergyPlus"
                        else 0.15
                        if company == "GreenUtility"
                        else 0.13
                    )
                    volume_discount = (
                        1.0
                        if cust_type == "Residential"
                        else 0.95
                        if cust_type == "Commercial"
                        else 0.85
                        if cust_type == "Industrial"
                        else 0.90
                    )
                    costs.append(round(monthly_usage * base_rate * volume_discount, 2))

                    # CO2 emissions
                    emission_factor = 0.5 if company == "GreenUtility" else 0.8 if company == "EnergyPlus" else 1.0
                    co2_emissions.append(round(monthly_usage * emission_factor, 2))

                    # Outages (number of incidents)
                    base_outages = (
                        2
                        if company == "MetroElectric"
                        else 1.5
                        if company == "PowerGrid Corp"
                        else 1
                        if company == "EnergyPlus"
                        else 0.5
                    )
                    weather_factor = 1.5 if month in [1, 2, 7, 8, 12] else 1.0  # More outages in extreme weather
                    outages.append(round(base_outages * weather_factor * random_factor))

    # Create the Polars DataFrame
    df = pl.DataFrame(
        {
            "date": dates,
            "company": company_list,
            "customer_type": customer_type_list,
            "usage_kwh": usage,
            "cost_usd": costs,
            "co2_kg": co2_emissions,
            "outages": outages,
        }
    )

    # Add year and month columns for easier filtering
    df = df.with_columns(
        [
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.month().alias("month"),
            pl.col("date").dt.month().map_elements(lambda m: datetime(2000, m, 1).strftime("%B")).alias("month_name"),
        ]
    )

    return df


# Initialize the Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Create the layout
app.layout = html.Div(
    [
        html.H1("Utility Company Dashboard", style={"textAlign": "center", "marginBottom": "30px"}),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Select Utility Company:"),
                        dcc.Dropdown(
                            id="company-filter",
                            multi=True,
                            placeholder="Select companies...",
                        ),
                    ],
                    style={"width": "23%", "display": "inline-block", "marginRight": "2%"},
                ),
                html.Div(
                    [
                        html.Label("Select Customer Type:"),
                        dcc.Dropdown(
                            id="customer-type-filter",
                            multi=True,
                            placeholder="Select customer types...",
                        ),
                    ],
                    style={"width": "23%", "display": "inline-block", "marginRight": "2%"},
                ),
                html.Div(
                    [
                        html.Label("Select Year:"),
                        dcc.Dropdown(
                            id="year-filter",
                            multi=True,
                            placeholder="Select years...",
                        ),
                    ],
                    style={"width": "23%", "display": "inline-block", "marginRight": "2%"},
                ),
                html.Div(
                    [
                        html.Label("Select Month:"),
                        dcc.Dropdown(
                            id="month-filter",
                            multi=True,
                            placeholder="Select months...",
                        ),
                    ],
                    style={"width": "23%", "display": "inline-block"},
                ),
            ],
            style={"marginBottom": "30px"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Key Metrics", style={"textAlign": "center"}),
                        html.Div(id="key-metrics", style={"display": "flex", "justifyContent": "space-around"}),
                    ],
                    style={"marginBottom": "30px"},
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.H3("Usage Over Time", style={"textAlign": "center"}),
                                dcc.Graph(id="usage-time-graph"),
                            ],
                            style={"width": "48%", "display": "inline-block"},
                        ),
                        html.Div(
                            [
                                html.H3("Cost vs. Usage by Customer Type", style={"textAlign": "center"}),
                                dcc.Graph(id="cost-usage-graph"),
                            ],
                            style={"width": "48%", "display": "inline-block", "float": "right"},
                        ),
                    ],
                    style={"marginBottom": "30px"},
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.H3("CO2 Emissions by Company", style={"textAlign": "center"}),
                                dcc.Graph(id="emissions-graph"),
                            ],
                            style={"width": "48%", "display": "inline-block"},
                        ),
                        html.Div(
                            [html.H3("Outages by Month", style={"textAlign": "center"}), dcc.Graph(id="outages-graph")],
                            style={"width": "48%", "display": "inline-block", "float": "right"},
                        ),
                    ],
                    style={"marginBottom": "30px"},
                ),
                html.H3("Detailed Data", style={"textAlign": "center"}),
                dash_table.DataTable(
                    id="detailed-table",
                    page_size=10,
                    style_table={"overflowX": "auto"},
                    style_cell={
                        "textAlign": "left",
                        "minWidth": "100px",
                        "width": "150px",
                        "maxWidth": "200px",
                        "overflow": "hidden",
                        "textOverflow": "ellipsis",
                    },
                    style_header={"backgroundColor": "rgb(230, 230, 230)", "fontWeight": "bold"},
                    sort_action="native",
                    filter_action="native",
                    page_action="native",
                ),
            ]
        ),
    ]
)

# Generate the Polars DataFrame
polars_df = create_sample_data()


# Callback to initialize filter options
@callback(
    [
        Output("company-filter", "options"),
        Output("company-filter", "value"),
        Output("customer-type-filter", "options"),
        Output("customer-type-filter", "value"),
        Output("year-filter", "options"),
        Output("year-filter", "value"),
        Output("month-filter", "options"),
        Output("month-filter", "value"),
    ],
    Input("detailed-table", "page_size"),  # Using a dummy input that exists in the layout
)
def initialize_filters(_):
    # Get unique values for each filter
    companies = polars_df.select("company").unique().to_pandas()
    company_options = [{"label": company, "value": company} for company in companies["company"]]

    customer_types = polars_df.select("customer_type").unique().to_pandas()
    customer_type_options = [{"label": ct, "value": ct} for ct in customer_types["customer_type"]]

    years = polars_df.select("year").unique().sort(by="year").to_pandas()
    year_options = [{"label": str(year), "value": year} for year in years["year"]]

    # For months, we want to keep them in order
    month_data = polars_df.select(["month", "month_name"]).unique().sort("month").to_pandas()
    month_options = [{"label": row["month_name"], "value": row["month"]} for _, row in month_data.iterrows()]

    # Set default values (all selected)
    company_values = [company for company in companies["company"]]
    customer_type_values = [ct for ct in customer_types["customer_type"]]
    year_values = [year for year in years["year"]]
    month_values = [month for month in month_data["month"]]

    return (
        company_options,
        company_values,
        customer_type_options,
        customer_type_values,
        year_options,
        year_values,
        month_options,
        month_values,
    )


# Callback to update all visualizations based on filters
@callback(
    [
        Output("key-metrics", "children"),
        Output("usage-time-graph", "figure"),
        Output("cost-usage-graph", "figure"),
        Output("emissions-graph", "figure"),
        Output("outages-graph", "figure"),
        Output("detailed-table", "columns"),
        Output("detailed-table", "data"),
    ],
    [
        Input("company-filter", "value"),
        Input("customer-type-filter", "value"),
        Input("year-filter", "value"),
        Input("month-filter", "value"),
    ],
)
def update_dashboard(companies, customer_types, years, months):
    # Apply filters to the DataFrame
    filtered_df = polars_df

    if companies and len(companies) > 0:
        filtered_df = filtered_df.filter(pl.col("company").is_in(companies))

    if customer_types and len(customer_types) > 0:
        filtered_df = filtered_df.filter(pl.col("customer_type").is_in(customer_types))

    if years and len(years) > 0:
        filtered_df = filtered_df.filter(pl.col("year").is_in(years))

    if months and len(months) > 0:
        filtered_df = filtered_df.filter(pl.col("month").is_in(months))

    # Convert to pandas for Plotly compatibility
    pandas_df = filtered_df.to_pandas()

    # If the DataFrame is empty after filtering, return empty visualizations
    if pandas_df.empty:
        empty_figure = {
            "data": [],
            "layout": {
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "annotations": [
                    {
                        "text": "No data available for the selected filters",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {"size": 18},
                    }
                ],
            },
        }

        return ([html.Div("No data available")], empty_figure, empty_figure, empty_figure, empty_figure, [], [])

    # ----- Key Metrics -----
    total_usage = filtered_df.select(pl.sum("usage_kwh")).item()
    total_cost = filtered_df.select(pl.sum("cost_usd")).item()
    total_co2 = filtered_df.select(pl.sum("co2_kg")).item()
    total_outages = filtered_df.select(pl.sum("outages")).item()

    metrics = [
        html.Div(
            [html.H4("Total Usage"), html.H2(f"{total_usage:,.0f} kWh")],
            style={
                "textAlign": "center",
                "border": "1px solid #ddd",
                "borderRadius": "5px",
                "padding": "10px",
                "width": "22%",
            },
        ),
        html.Div(
            [html.H4("Total Cost"), html.H2(f"${total_cost:,.2f}")],
            style={
                "textAlign": "center",
                "border": "1px solid #ddd",
                "borderRadius": "5px",
                "padding": "10px",
                "width": "22%",
            },
        ),
        html.Div(
            [html.H4("CO2 Emissions"), html.H2(f"{total_co2:,.0f} kg")],
            style={
                "textAlign": "center",
                "border": "1px solid #ddd",
                "borderRadius": "5px",
                "padding": "10px",
                "width": "22%",
            },
        ),
        html.Div(
            [html.H4("Total Outages"), html.H2(f"{total_outages:,.0f}")],
            style={
                "textAlign": "center",
                "border": "1px solid #ddd",
                "borderRadius": "5px",
                "padding": "10px",
                "width": "22%",
            },
        ),
    ]

    # ----- Usage Over Time Graph -----
    # Aggregate data by date and company
    time_data = pandas_df.groupby(["date", "company"])["usage_kwh"].sum().reset_index()
    time_data["date"] = pd.to_datetime(time_data["date"])
    time_data = time_data.sort_values("date")

    usage_fig = px.line(
        time_data,
        x="date",
        y="usage_kwh",
        color="company",
        title="Energy Usage Over Time",
        labels={"usage_kwh": "Usage (kWh)", "date": "Date", "company": "Company"},
    )
    usage_fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=40, b=40),
    )

    # ----- Cost vs Usage by Customer Type Graph -----
    cost_usage_data = pandas_df.groupby("customer_type").agg({"usage_kwh": "sum", "cost_usd": "sum"}).reset_index()

    cost_usage_fig = px.scatter(
        cost_usage_data,
        x="usage_kwh",
        y="cost_usd",
        size="usage_kwh",
        color="customer_type",
        text="customer_type",
        labels={"usage_kwh": "Usage (kWh)", "cost_usd": "Cost (USD)", "customer_type": "Customer Type"},
    )
    cost_usage_fig.update_traces(textposition="top center")
    cost_usage_fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # ----- CO2 Emissions by Company Graph -----
    emissions_data = pandas_df.groupby("company")["co2_kg"].sum().reset_index()

    emissions_fig = px.bar(
        emissions_data,
        x="company",
        y="co2_kg",
        color="company",
        labels={"co2_kg": "CO2 Emissions (kg)", "company": "Company"},
    )
    emissions_fig.update_layout(margin=dict(l=40, r=40, t=40, b=40), showlegend=False)

    # ----- Outages by Month Graph -----
    # First create a complete month-year grid with all combinations
    outages_data = pandas_df.groupby(["year", "month", "month_name"])["outages"].sum().reset_index()
    outages_data["year_month"] = outages_data["year"].astype(str) + "-" + outages_data["month_name"]

    outages_fig = px.bar(
        outages_data.sort_values(["year", "month"]),
        x="year_month",
        y="outages",
        color="year",
        labels={"outages": "Number of Outages", "year_month": "Period", "year": "Year"},
    )
    outages_fig.update_layout(
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis={"categoryorder": "array", "categoryarray": outages_data.sort_values(["year", "month"])["year_month"]},
    )

    # ----- Detailed Data Table -----
    # Format the DataFrame for display
    display_df = filtered_df.with_columns(
        [
            pl.col("date").cast(pl.Utf8),
            pl.col("usage_kwh").round(2),
            pl.col("cost_usd").round(2),
            pl.col("co2_kg").round(2),
        ]
    )

    # Only include relevant columns
    display_df = display_df.select(["date", "company", "customer_type", "usage_kwh", "cost_usd", "co2_kg", "outages"])

    columns = [{"name": col, "id": col} for col in display_df.columns]
    data = display_df.to_pandas().to_dict("records")

    return metrics, usage_fig, cost_usage_fig, emissions_fig, outages_fig, columns, data


if __name__ == "__main__":
    app.run_server(debug=True)
