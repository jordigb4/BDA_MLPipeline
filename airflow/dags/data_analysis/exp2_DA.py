from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split, GridSearchCV
from dags.landing.class_types import WeatherStationId
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from dags.utils.hdfs_utils import HDFSManager
from pyspark.sql import SparkSession
from scipy.stats import gaussian_kde
from xgboost import XGBRegressor # type:ignore
import plotly.express as px
import numpy as np
import logging
import pickle
import os
import io

# Configure logging
log = setup_logging(__name__)

def data_analysis_2(hdfs_manager: HDFSManager, postgres_manager: PostgresManager, input_view: str = 'experiment2'):
    # Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("WeatherQualityElectricity") \
        .getOrCreate()

    # Base path for experiments
    hdfs_path = '/data/data_analysis/exp2/'
    hdfs_manager.mkdirs(hdfs_path)
    log.info(f'Making directories for plot storing: {hdfs_path}')

    # Read view
    df = postgres_manager.read_table(spark, input_view)

    # Generate the necessary figures to plot
    figs = task_2_analysis(df)

    # Store plots in its corresponding directory:
    plots = ['fig_importance', 'fig_real_vs_pred', 'fig_errors']
    for station in WeatherStationId:
        station_figs = figs[station.name.lower()]
        for i, fig in enumerate(station_figs):
            # Serialize figure to an in-memory bytes buffer
            buffer = io.BytesIO()
            pickle.dump(fig, buffer)
            buffer.seek(0)

            # Define HDFS file path
            hdfs_file_path = hdfs_path + f"{station.name.lower()}_{plots[i]}.pkl"

        # Upload buffer content directly to HDFS
        with hdfs_manager._client.write(hdfs_file_path, overwrite=True) as writer:
            writer.write(buffer.read())


def task_2_analysis(df):
    """Perform the task 2 experiment, which works with air quality data, electricity and weather's"""

    # 1. Select columns
    features = ['station', 'TMAX', 'TMIN', 'PRCP', 'cenergy', 'co', 'no2', 'o3', 'so2', 'pm25']
    df = df[features]

    # 2. Split by station
    df_downtown = df[df['station'] == 'downtown']
    df_reseda = df[df['station'] == 'reseda']
    df_long_beach = df[df['station'] == 'long_beach']

    df_reseda = df_reseda.drop(columns=['so2'])
    df_long_beach = df_long_beach.drop(columns=['co'])

    # Impute with the mean for the Downtown station
    for col in df_downtown.columns:
        if df_downtown[col].isnull().any():
            df_downtown.loc[:, col] = df_downtown[col].fillna(df_downtown[col].mean())

    # For the Reseda station, drop 'so2' and then impute with the mean in other columns
    for col in df_reseda.columns:
        if col != 'so2' and df_reseda[col].isnull().any():
            df_reseda.loc[:, col] = df_reseda[col].fillna(df_reseda[col].mean())

    # For the Long Beach station, drop 'co' and 'pm25' and then impute with the mean in other columns
    for col in df_long_beach.columns:
        if col not in ['co', 'pm25'] and df_long_beach[col].isnull().any():
            df_long_beach.loc[:, col] = df_long_beach[col].fillna(df_long_beach[col].mean())

    # Target variable for each station
    target_reseda_downtown = 'pm25'  # For Reseda and Downtown
    target_long_beach = 'o3'  # For Long Beach

    # Split into X (features) and y (target) for each station
    X_downtown = df_downtown.drop(columns=['station', target_reseda_downtown])
    y_downtown = df_downtown[target_reseda_downtown]

    X_reseda = df_reseda.drop(columns=['station', target_reseda_downtown])
    y_reseda = df_reseda[target_reseda_downtown]

    X_long_beach = df_long_beach.drop(columns=['station', target_long_beach])
    y_long_beach = df_long_beach[target_long_beach]

    # Split into train and test sets (80% training, 20% test)
    X_train_downtown, X_test_downtown, y_train_downtown, y_test_downtown = train_test_split(X_downtown, y_downtown, test_size=0.2, random_state=42)
    X_train_reseda, X_test_reseda, y_train_reseda, y_test_reseda = train_test_split(X_reseda, y_reseda, test_size=0.2, random_state=42)
    X_train_long_beach, X_test_long_beach, y_train_long_beach, y_test_long_beach = train_test_split(X_long_beach, y_long_beach, test_size=0.2, random_state=42)

    # Define the base model
    xgb_downtown = XGBRegressor(objective='reg:squarederror', random_state=42)
    xgb_reseda = XGBRegressor(objective='reg:squarederror', random_state=42)
    xgb_long_beach = XGBRegressor(objective='reg:squarederror', random_state=42)

    # Define the hyperparameter grid
    param_grid = {
        'n_estimators': [100, 200],
        'max_depth': [3, 5, 7],
        'learning_rate': [0.01, 0.1],
    }

    def fit_model(grid_search, X_train, y_train, X_test, y_test, station_name):
        grid_search.fit(X_train, y_train)
        best_model = grid_search.best_estimator_
        y_pred = best_model.predict(X_test)
        
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        # Plot importance
        importances = best_model.feature_importances_
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

    # Fit and plot for each station
    grid_search_downtown = GridSearchCV(estimator=xgb_downtown, param_grid=param_grid, cv=5, scoring='neg_mean_squared_error', verbose=1, n_jobs=-1)
    plots_downtown = fit_model(grid_search_downtown, X_train_downtown, y_train_downtown, X_test_downtown, y_test_downtown, 'Downtown')

    grid_search_reseda = GridSearchCV(estimator=xgb_reseda, param_grid=param_grid, cv=5, scoring='neg_mean_squared_error', verbose=1, n_jobs=-1)
    plots_reseda = fit_model(grid_search_reseda, X_train_reseda, y_train_reseda, X_test_reseda, y_test_reseda, 'Reseda')

    grid_search_long_beach = GridSearchCV(estimator=xgb_long_beach, param_grid=param_grid, cv=5, scoring='neg_mean_squared_error', verbose=1, n_jobs=-1)
    plots_long_beach = fit_model(grid_search_long_beach, X_train_long_beach, y_train_long_beach, X_test_long_beach, y_test_long_beach, 'Long_Beach')

    return {
        'downtown': plots_downtown,
        'reseda': plots_reseda,
        'long_beach': plots_long_beach
    }