from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from dags.landing.class_types import WeatherStationId
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from dags.utils.hdfs_utils import HDFSManager
from pyspark.sql import SparkSession
from scipy.stats import gaussian_kde
from xgboost import XGBRegressor
import plotly.express as px
import numpy as np
import logging
import pickle
import os
import io

log = setup_logging(__name__)


def data_analysis_2(hdfs_manager: HDFSManager, postgres_manager: PostgresManager, input_view: str = 'experiment2'):
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("WeatherQualityElectricity") \
        .getOrCreate()

    hdfs_path = '/data/data_analysis/exp2/'
    hdfs_manager.mkdirs(hdfs_path)
    log.info(f'Making directories for plot storing: {hdfs_path}')

    df = postgres_manager.read_table(spark, input_view).toPandas()
    figs = task_2_analysis(df)

    plots = ['fig_importance', 'fig_real_vs_pred', 'fig_errors']
    for station in WeatherStationId:
        station_name = station.name.lower()
        station_figs = figs[station_name]
        for i, fig in enumerate(station_figs):
            buffer = io.BytesIO()
            pickle.dump(fig, buffer)
            buffer.seek(0)
            hdfs_file_path = hdfs_path + f"{station_name}_{plots[i]}.pkl"

            with hdfs_manager._client.write(hdfs_file_path, overwrite=True) as writer:
                writer.write(buffer.read())


def task_2_analysis(df):
    features = ['station', 'TMAX', 'TMIN', 'PRCP', 'cenergy', 'co', 'no2', 'o3', 'so2', 'pm25']
    df = df[features]

    df_downtown = df[df['station'] == 'downtown']
    df_reseda = df[df['station'] == 'reseda']
    df_long_beach = df[df['station'] == 'long_beach']

    df_reseda = df_reseda.drop(columns=['so2'])
    df_long_beach = df_long_beach.drop(columns=['co'])

    # Handle missing values for Long Beach's pm25
    df_long_beach['pm25'] = df_long_beach['pm25'].fillna(df_long_beach['pm25'].mean())

    # Impute missing values
    for col in df_downtown.columns:
        if df_downtown[col].isnull().any():
            df_downtown[col] = df_downtown[col].fillna(df_downtown[col].mean())

    for col in df_reseda.columns:
        if col != 'so2' and df_reseda[col].isnull().any():
            df_reseda[col] = df_reseda[col].fillna(df_reseda[col].mean())

    for col in df_long_beach.columns:
        if col not in ['co', 'pm25'] and df_long_beach[col].isnull().any():
            df_long_beach[col] = df_long_beach[col].fillna(df_long_beach[col].mean())

    target_reseda_downtown = 'pm25'
    target_long_beach = 'o3'

    # Feature/target splits
    X_downtown = df_downtown.drop(columns=['station', target_reseda_downtown])
    y_downtown = df_downtown[target_reseda_downtown]

    X_reseda = df_reseda.drop(columns=['station', target_reseda_downtown])
    y_reseda = df_reseda[target_reseda_downtown]

    X_long_beach = df_long_beach.drop(columns=['station', target_long_beach])
    y_long_beach = df_long_beach[target_long_beach]

    # Train/test splits
    X_train_d, X_test_d, y_train_d, y_test_d = train_test_split(X_downtown, y_downtown, test_size=0.2, random_state=42)
    X_train_r, X_test_r, y_train_r, y_test_r = train_test_split(X_reseda, y_reseda, test_size=0.2, random_state=42)
    X_train_lb, X_test_lb, y_train_lb, y_test_lb = train_test_split(X_long_beach, y_long_beach, test_size=0.2,
                                                                    random_state=42)

    # Initialize models with fixed parameters
    xgb_d = XGBRegressor(objective='reg:squarederror', n_estimators=200, max_depth=5, learning_rate=0.1,
                         random_state=42)
    xgb_r = XGBRegressor(objective='reg:squarederror', n_estimators=200, max_depth=5, learning_rate=0.1,
                         random_state=42)
    xgb_lb = XGBRegressor(objective='reg:squarederror', n_estimators=200, max_depth=5, learning_rate=0.1,
                          random_state=42)

    def fit_model(model, X_train, y_train, X_test, y_test, station_name):
        model.fit(X_train, y_train)  # Direct training without grid search
        y_pred = model.predict(X_test)

        # Calculate metrics
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        # Plotting code remains the same as original
        importances = model.feature_importances_
        features = X_train.columns
        sorted_idx = np.argsort(importances)[::-1]

        fig_importance = px.bar(
            x=importances[sorted_idx],
            y=features[sorted_idx],
            orientation='h',
            labels={'x': 'Importance', 'y': 'Features'},
            title=f"Feature Importance ({station_name})",
            height=400
        )

        fig_importance.update_layout(template='plotly_white', showlegend=False)

        # Real vs Predicted
        fig_real_vs_pred = px.scatter(
            x=y_test, y=y_pred, 
            labels={'x': 'Real Values', 'y': 'Predictions'},
            title=f"Real vs Predicted ({station_name})",
            opacity=0.6
        )
        min_val = min(y_test.min(), y_pred.min())
        max_val = max(y_test.max(), y_pred.max())
        fig_real_vs_pred.add_shape(
            type='line',
            line=dict(dash='dash', color='red'),
            x0=min_val, x1=max_val,
            y0=min_val, y1=max_val
        )
        fig_real_vs_pred.update_layout(
            template='plotly_white',
            xaxis_range=[min_val, max_val],
            yaxis_range=[min_val, max_val]
        )

        # Error distribution
        errors = y_test - y_pred
        fig_errors = px.histogram(
            errors, 
            nbins=30,
            labels={'value': 'Error (real - predicted)', 'count': 'Frequency'},
            title=f"Error Distribution ({station_name})",
            color_discrete_sequence=['coral']
        )
        kde = gaussian_kde(errors)
        x_range = np.linspace(errors.min(), errors.max(), 100)
        fig_errors.add_scatter(
            x=x_range, 
            y=kde(x_range), 
            mode='lines',
            line=dict(color='black'),
            name='KDE'
        )
        fig_errors.update_layout(template='plotly_white')

        return fig_importance, fig_real_vs_pred, fig_errors

    # Train models and generate plots
    plots_downtown = fit_model(xgb_d, X_train_d, y_train_d, X_test_d, y_test_d, 'Downtown')
    plots_reseda = fit_model(xgb_r, X_train_r, y_train_r, X_test_r, y_test_r, 'Reseda')
    plots_long_beach = fit_model(xgb_lb, X_train_lb, y_train_lb, X_test_lb, y_test_lb, 'Long_Beach')

    return {
        'downtown': plots_downtown,
        'reseda': plots_reseda,
        'long_beach': plots_long_beach
    }