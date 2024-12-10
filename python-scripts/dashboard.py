import time
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from config import GCP_CONFIG, STOCK_CONFIGS
from google.cloud import bigquery


# Update the time window configurations in the fetch_stock_data function
@st.cache_data(ttl=300)  # Cache data for 5 minutes
def fetch_stock_data(symbol, time_window, table_type, dataset):
    """
    Get stock data from BigQuery with focus on real-time data
    time_window: 'recent', '1w', '1m'
    """
    window_configs = {
        "recent": "LIMIT 100",  # Most recent 100 records
        "1w": "AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)",
        "1m": "AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)",
    }

    time_filter = window_configs.get(time_window, window_configs["recent"])
    table_suffix = "_processed" if table_type == "processed" else "_raw"

    query = f"""
    SELECT *
    FROM {dataset}.{STOCK_CONFIGS[symbol]['table_name']}{table_suffix}
    WHERE 1=1 
    {time_filter if time_window != 'recent' else ''}
    ORDER BY timestamp DESC
    {' ' + time_filter if time_window == 'recent' else ''}
    """

    try:
        data = pandas_gbq.read_gbq(
            query, project_id=GCP_CONFIG["PROJECT_ID"], progress_bar_type=None
        )

        if not data.empty:
            data["timestamp"] = pd.to_datetime(data["timestamp"])
            data = data.sort_values("timestamp")

        return data

    except Exception as e:
        st.error(f"Error fetching data for {symbol}: {str(e)}")
        return pd.DataFrame()


class StockDashboard:
    def __init__(self):
        self.client = bigquery.Client()
        self.dataset = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"

    def get_stock_data(self, symbol, time_window="recent", table_type="processed"):
        return fetch_stock_data(symbol, time_window, table_type, self.dataset)

    def calculate_metrics(self, data):
        if data.empty:
            return None

        try:
            latest = data.iloc[-1]
            prev_close = (
                data.iloc[-2]["close"] if len(data) > 1 else data.iloc[0]["open"]
            )

            return {
                "current_price": latest["close"],
                "day_change": (latest["close"] - prev_close) / prev_close * 100,
                "day_volume": latest["volume"],
                "ma7": latest.get("ma7", latest["close"]),
                "ma20": latest.get("ma20", latest["close"]),
                "volatility": latest.get("volatility", 0),
                "momentum": latest.get("momentum", 0),
                "last_update": latest["timestamp"],
            }
        except Exception as e:
            st.warning(f"Error calculating metrics: {str(e)}")
            return None


def create_price_chart(data):
    """Create candlestick chart with moving averages"""
    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=data["timestamp"],
            open=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
            name="OHLC",
        )
    )

    if "ma7" in data.columns:
        fig.add_trace(
            go.Scatter(
                x=data["timestamp"],
                y=data["ma7"],
                name="MA7",
                line=dict(color="blue", width=1),
            )
        )

    if "ma20" in data.columns:
        fig.add_trace(
            go.Scatter(
                x=data["timestamp"],
                y=data["ma20"],
                name="MA20",
                line=dict(color="orange", width=1),
            )
        )

    fig.update_layout(
        title="Real-time Price Movement with Moving Averages",
        yaxis_title="Price (USD)",
        xaxis_title="Time",
        height=600,
        template="plotly_white",
    )

    return fig


def create_volume_chart(data):
    """Create volume chart with moving average"""
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=data["timestamp"],
            y=data["volume"],
            name="Volume",
            marker_color="lightblue",
        )
    )

    if "volume_ma5" in data.columns:
        fig.add_trace(
            go.Scatter(
                x=data["timestamp"],
                y=data["volume_ma5"],
                name="Volume MA5",
                line=dict(color="red", width=2),
            )
        )

    fig.update_layout(
        title="Real-time Trading Volume Analysis",
        yaxis_title="Volume",
        xaxis_title="Time",
        height=400,
        template="plotly_white",
    )

    return fig


def main():
    st.set_page_config(
        page_title="Real-time Stock Market Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Custom CSS
    st.markdown(
        """
        <style>
        .stAlert {
            font-size: 16px;
            margin: 20px 0;
        }
        .metric-card {
            background-color: #f0f2f6;
            padding: 10px;
            border-radius: 5px;
        }
        </style>
    """,
        unsafe_allow_html=True,
    )

    dashboard = StockDashboard()

    # Sidebar Controls
    st.sidebar.title("📊 Dashboard Controls")

    # Update time window options
    time_windows = {
        "Real-time (Latest 100 records)": "recent",
        "Last Week": "1w",
        "Last Month": "1m",
    }
    selected_window = st.sidebar.selectbox(
        "Select Time Window", list(time_windows.keys())
    )
    time_window = time_windows[selected_window]
    selected_stock = st.sidebar.selectbox("Select Stock", list(STOCK_CONFIGS.keys()))

    view_type = st.sidebar.radio(
        "Select View Type",
        ["Technical Analysis", "Volume Analysis", "Comparative Analysis"],
    )

    auto_refresh = st.sidebar.checkbox("Enable Auto-refresh", value=False)
    if auto_refresh:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)", min_value=5, max_value=300, value=60
        )
        st.sidebar.info(f"Data will refresh every {refresh_interval} seconds")

    st.title(f"📈 Real-time Stock Analysis - {selected_stock}")

    def load_and_display_data():
        with st.spinner("Loading real-time data..."):
            processed_data = dashboard.get_stock_data(
                selected_stock, time_window, "processed"
            )

            if processed_data.empty:
                st.info(
                    f"No recent data available for {selected_stock}. This might be due to market hours or data loading."
                )
                return

            # Display metrics
            metrics = dashboard.calculate_metrics(processed_data)
            if metrics:
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric(
                        "Current Price",
                        f"${metrics['current_price']:.2f}",
                        f"{metrics['day_change']:.2f}%",
                    )
                with col2:
                    st.metric("Volume", f"{metrics['day_volume']:,}")
                with col3:
                    st.metric("MA7", f"${metrics['ma7']:.2f}")
                with col4:
                    st.metric("Volatility", f"{metrics['volatility']:.2f}%")
                with col5:
                    st.metric(
                        "Last Update", metrics["last_update"].strftime("%H:%M:%S")
                    )

            # Technical Analysis View
            if view_type == "Technical Analysis":
                st.plotly_chart(
                    create_price_chart(processed_data), use_container_width=True
                )

                col1, col2 = st.columns(2)
                with col1:
                    if "daily_return" in processed_data.columns:
                        fig_returns = px.line(
                            processed_data,
                            x="timestamp",
                            y="daily_return",
                            title="Returns (%)",
                        )
                        st.plotly_chart(fig_returns, use_container_width=True)

                with col2:
                    if "momentum" in processed_data.columns:
                        fig_momentum = px.line(
                            processed_data,
                            x="timestamp",
                            y="momentum",
                            title="Momentum",
                        )
                        st.plotly_chart(fig_momentum, use_container_width=True)

            # Volume Analysis View
            elif view_type == "Volume Analysis":
                st.plotly_chart(
                    create_volume_chart(processed_data), use_container_width=True
                )

                fig_vol_dist = px.histogram(
                    processed_data, x="volume", nbins=50, title="Volume Distribution"
                )
                st.plotly_chart(fig_vol_dist, use_container_width=True)

            # Comparative Analysis View
            else:
                comparison_df = pd.DataFrame()

                for symbol in STOCK_CONFIGS.keys():
                    stock_data = dashboard.get_stock_data(
                        symbol, time_window, "processed"
                    )
                    if not stock_data.empty:
                        first_price = stock_data["close"].iloc[0]
                        comparison_df[symbol] = (
                            stock_data["close"] / first_price - 1
                        ) * 100
                        comparison_df["timestamp"] = stock_data["timestamp"]

                if not comparison_df.empty:
                    fig_comparison = px.line(
                        comparison_df,
                        x="timestamp",
                        y=[col for col in comparison_df.columns if col != "timestamp"],
                        title="Relative Performance Comparison (%)",
                        height=500,
                    )
                    st.plotly_chart(fig_comparison, use_container_width=True)

                    correlation = comparison_df.drop("timestamp", axis=1).corr()
                    fig_corr = px.imshow(
                        correlation,
                        title="Price Correlation Matrix",
                        color_continuous_scale="RdBu",
                    )
                    st.plotly_chart(fig_corr, use_container_width=True)

            # Statistical Summary
            with st.expander("Statistical Summary"):
                st.dataframe(processed_data.describe())

    # Initial load
    load_and_display_data()

    # Auto-refresh loop
    if auto_refresh:
        placeholder = st.empty()
        while True:
            time.sleep(refresh_interval)
            with placeholder.container():
                load_and_display_data()

    # Footer
    st.markdown("---")
    now = datetime.now()
    st.markdown(
        f"""
        Last dashboard update: {now.strftime("%Y-%m-%d %H:%M:%S")}  
        Data refreshes every {refresh_interval if auto_refresh else 5} seconds
    """
    )


if __name__ == "__main__":
    main()
