"""Reusable layout component builders for dashboard tabs."""

from typing import Literal, cast

from dash import dash_table, dcc, html

from dashboard.styles import (
    CARD_STYLE,
    DOWNLOAD_BUTTON_STYLE,
    LOADING_CONFIG,
    SECTION_HEADER_STYLE,
    SUBTITLE_STYLE,
    TAB_SELECTED_STYLE,
    TAB_STYLE,
    TABLE_CONTAINER_STYLE,
    TABLE_STYLE_CELL,
    TABLE_STYLE_DATA_CONDITIONAL,
    TABLE_STYLE_HEADER,
    TITLE_STYLE,
)


def create_chart_card(
    title: str,
    chart_id: str,
    subtitle_id: str | None = None,
    loading_color: str = "#156570",
) -> html.Div:
    """Create standardized chart card component.

    Args:
        title: Chart title
        chart_id: ID for the dcc.Graph component
        subtitle_id: ID for the subtitle paragraph (optional)
        loading_color: Color for loading spinner

    Returns:
        Dash html.Div component
    """
    children = [
        html.H3(title, style=TITLE_STYLE),
    ]

    if subtitle_id:
        children.append(html.P(id=subtitle_id, style=SUBTITLE_STYLE))

    children.append(
        dcc.Loading(
            type=cast(
                "Literal['graph', 'cube', 'circle', 'dot', 'default'] | None",
                LOADING_CONFIG.get("type"),
            ),
            color=loading_color,
            children=dcc.Graph(
                id=chart_id,
                config={
                    "displayModeBar": True,
                    "toImageButtonOptions": {
                        "format": "png",
                        "filename": chart_id,
                        "height": 800,
                        "width": 1200,
                        "scale": 2,
                    },
                },
            ),
        )
    )

    return html.Div(children, style=CARD_STYLE)


def create_data_table_card(
    title: str,
    table_id: str,
    download_btn_id: str,
    download_id: str,
    columns: list[dict],
    page_size: int = 25,
) -> html.Div:
    """Create standardized data table card component.

    Args:
        title: Table title
        table_id: ID for the DataTable component
        download_btn_id: ID for the download button
        download_id: ID for the dcc.Download component
        columns: List of column definitions for DataTable
        page_size: Number of rows per page

    Returns:
        Dash html.Div component
    """
    return html.Div(
        [
            # Header with title and download button
            html.Div(
                [
                    html.H3(
                        title,
                        style={**TITLE_STYLE, "display": "inline-block"},
                    ),
                    html.Button(
                        "⬇ Download CSV",
                        id=download_btn_id,
                        style=DOWNLOAD_BUTTON_STYLE,
                    ),
                    dcc.Download(id=download_id),
                ],
                style=TABLE_CONTAINER_STYLE,
            ),
            # Data table
            dcc.Loading(
                type=cast(
                    "Literal['graph', 'cube', 'circle', 'dot', 'default'] | None",
                    LOADING_CONFIG.get("type"),
                ),
                color=LOADING_CONFIG["color"],
                children=dash_table.DataTable(
                    id=table_id,
                    columns=columns,
                    data=[],
                    style_table={"overflowX": "auto"},
                    style_cell=TABLE_STYLE_CELL,
                    style_header=TABLE_STYLE_HEADER,
                    style_data_conditional=TABLE_STYLE_DATA_CONDITIONAL,
                    page_size=page_size,
                    sort_action="native",
                    filter_action="native",
                ),
            ),
        ],
        style=CARD_STYLE,
    )


def create_section_header(
    title: str,
    border_color: str = "#156570",
) -> html.H2:
    """Create standardized section header.

    Args:
        title: Section title text
        border_color: Color for bottom border

    Returns:
        Dash html.H2 component
    """
    style = SECTION_HEADER_STYLE.copy()
    style["borderBottom"] = f"3px solid {border_color}"

    return html.H2(title, style=style)


def create_tab(
    label: str,
    value: str,
    emoji: str = "📊",
    children: list | None = None,
) -> dcc.Tab:
    """Create standardized tab component.

    Args:
        label: Tab label text
        value: Tab value identifier
        emoji: Emoji prefix for tab label
        children: Tab content (list of components)

    Returns:
        Dash dcc.Tab component
    """
    return dcc.Tab(
        label=f"{emoji} {label}",
        value=value,
        style=TAB_STYLE,
        selected_style=TAB_SELECTED_STYLE,
        children=children or [],
    )


def create_chart_subtitle(
    subtitle_text: str,
) -> html.P:
    """Create standardized chart subtitle.

    Args:
        subtitle_text: Subtitle text

    Returns:
        Dash html.P component
    """
    return html.P(subtitle_text, style=SUBTITLE_STYLE)


def create_visualization_section(
    section_title: str | None,
    charts: list[dict],
    border_color: str = "#156570",
) -> list:
    """Create a visualization section with multiple charts.

    Args:
        section_title: Optional section header title
        charts: List of chart configurations, each containing:
                - title: Chart title
                - chart_id: Graph component ID
                - subtitle_id: Optional subtitle component ID
        border_color: Color for section header border

    Returns:
        List of Dash components
    """
    components = []

    # Add section header if provided
    if section_title:
        components.append(create_section_header(section_title, border_color))

    # Add chart cards
    components.extend(
        [
            create_chart_card(
                title=chart_config["title"],
                chart_id=chart_config["chart_id"],
                subtitle_id=chart_config.get("subtitle_id"),
            )
            for chart_config in charts
        ]
    )

    return components


def create_tab_container(
    sections: list[dict],
) -> html.Div:
    """Create a complete tab container with multiple sections.

    Args:
        sections: List of section configurations, each containing:
                 - title: Optional section title
                 - charts: List of chart configs
                 - table: Optional table config with keys:
                          - title, table_id, download_btn_id, download_id, columns

    Returns:
        Dash html.Div component containing all sections
    """
    children = []

    for section in sections:
        # Add visualization section if charts are provided
        if "charts" in section:
            children.extend(
                create_visualization_section(
                    section_title=section.get("title"),
                    charts=section["charts"],
                )
            )

        # Add data table section if provided
        if "table" in section:
            table_config = section["table"]
            children.append(
                create_data_table_card(
                    title=table_config["title"],
                    table_id=table_config["table_id"],
                    download_btn_id=table_config["download_btn_id"],
                    download_id=table_config["download_id"],
                    columns=table_config["columns"],
                    page_size=table_config.get("page_size", 25),
                )
            )

    return html.Div(
        children,
        style={
            "padding": "30px 40px",
            "backgroundColor": "#f8f9fa",
            "minHeight": "100vh",
        },
    )
