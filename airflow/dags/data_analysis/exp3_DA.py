#from dags.utils.other_utils import setup_logging
from dags.data_analysis.utils.trafficAcc_mocodes import create_mocodes_features
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from dags.utils.postgres_utils import PostgresManager
from sklearn.ensemble import RandomForestClassifier
from dags.utils.other_utils import setup_logging
from sklearn.compose import ColumnTransformer
from dags.utils.hdfs_utils import HDFSManager
from plotly.subplots import make_subplots
from sklearn.pipeline import Pipeline
from pyspark.sql import SparkSession
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import pickle
import os
import io

# Configure logging
log = setup_logging(__name__)

# Custom color scheme
COLOR_SCALE = ['#2A4C7D', '#4ECDC4', '#FF6B6B', '#6C5B7B']
TEMPLATE = 'plotly_white'


def data_analysis_3(hdfs_manager: HDFSManager, postgres_manager: PostgresManager, input_view: str = 'experiment3'):
    """Function to upload to hdfs the resulting figures of the analysis, for further streamlit plotting"""

    # Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("DataAnalysis3") \
        .getOrCreate()

    # Base path for experiments
    hdfs_path = '/data/data_analysis/exp3/'
    hdfs_manager.mkdirs(hdfs_path)
    log.info(f'Making directories for plot storing: {hdfs_path}')

    # Read view
    df = postgres_manager.read_table(spark, input_view).toPandas()

    # Generate the necessary figures to plot
    figs = task_3_analysis(df)
  
    # Serialize figures to an in-memory bytes buffer
    for name, fig in figs.items():
        buffer = io.BytesIO()
        pickle.dump(fig, buffer)
        buffer.seek(0)

        # Define HDFS file path
        hdfs_file_path = hdfs_path + f"{name}.pkl"
        log.info(f"Saving {name} to {hdfs_file_path}")

        # Upload buffer content directly to HDFS
        with hdfs_manager._client.write(hdfs_file_path, overwrite=True) as writer:
            writer.write(buffer.read())

def task_3_analysis(df):
    """Perform the task 3 experiment, which works with traffic accident data and weather's"""

    # 1. Feature engineering
    # ===================

    # Date & Time Features
    # --------------------
    df['date_occ'] = pd.to_datetime(df['date_occ'])
    df['date_rptd'] = pd.to_datetime(df['date_rptd'])

    # Combine date and time of occurance
    df['datetime_occ'] = pd.to_datetime(df['date_occ'].astype(str) + ' ' + df['time_occ'])
    df = df.sort_values(by='datetime_occ'); df = df.dropna()

    # Extract components of date_occ
    df = df.assign(**{f'occ_{attr}': getattr(df['datetime_occ'].dt, attr) 
                  for attr in ['year', 'month', 'day', 'dayofweek', 'hour', 'minute']})
    
    # Exmple: df['occ_dayofweek'] = df['datetime_occ'].dt.dayofweek -> Monday=0, Sunday=6

    # Create a 'part_of_day' feature
    def get_part_of_day(hour):
        if 6 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 17:
            return 'Afternoon'
        elif 17 <= hour < 20:
            return 'Evening'
        elif 20 <= hour <= 24:
            return 'Late Evening'
        else:
            return 'Night'
        
    df['occ_part_of_day'] = df['occ_hour'].apply(get_part_of_day)

        # Calculate reporting delay
    df['report_delay_days'] = (df['date_rptd'] - df['date_occ']).dt.days

    # Create Weekend flag
    df['is_weekend'] = df['occ_dayofweek'].isin([5, 6]).astype(int) # 1 if Saturday/Sunday, 0 otherwise

    # Numerical features
    # ------------------
    # Calculate temperature range
    df['temp_range'] = df['TMAX'] - df['TMIN']

    # Report if it was raining
    df['is_raining'] = df['PRCP'] > 0

    # Discretize vict_age
    age_bins = [0, 17, 25, 40, 60, np.inf]
    age_labels = ['Minor', 'Young Adult', 'Adult', 'Middle-Aged', 'Senior']
    df['vict_age_group'] = pd.cut(df['vict_age'], bins=age_bins, labels=age_labels, right=False)

    # Categorical features
    # --------------------
    # Convert mocodes to qualitative features
    df = create_mocodes_features(df)
    
    # Identify columns with > 50% 'unknown' (highly unbalanced)
    cols_to_drop = df.columns[(df == 'unknown').mean() > 0.5]
    df = df.drop(cols_to_drop, axis = 1)

    # Reduce levels in location (currently 1532) --> 44
    value_counts = df['location'].value_counts()
    frequent_streets = value_counts[value_counts > 100].index.tolist() # get popular streets
    df['location'] = np.where(df['location'].isin(frequent_streets), df['location'], 'secondary_st')

    # Remove redundant variables
    drop_cols = ['dr_no', 'date_occ', 'date_rptd', 'time_occ', 'rpt_dist_no', 'mocodes', 'cross_street']
    df = df.drop(drop_cols, axis = 1)

    # Reorder columns
    new_order = ['datetime_occ', 'occ_year', 'occ_month', 'occ_day', 'occ_dayofweek',
    'occ_hour', 'occ_minute', 'occ_part_of_day', 'is_weekend', 'report_delay_days',
    'area_name', 'location', 'Location_Type', 'latitude', 'longitude',
    'vict_age', 'vict_age_group', 'vict_sex', 'vict_descent',
    'Collision_Type', 'Injury_Level', 'Hit_Run_Property', 'Collision_Factor',
    'premis_cd', 'TMAX', 'TMIN', 'PRCP', 'temp_range', 'is_raining']
    df = df[new_order]

    # 2. Key Visualizations
    # =====================
    # Visualization 1: Time Series Analysis
    # -------------------------------------
    weekly_accidents = (df.set_index('datetime_occ').groupby([pd.Grouper(freq='W-MON'), 'area_name']).size().unstack()
                        .fillna(0).reset_index().melt(id_vars='datetime_occ', var_name='area_name', value_name='accident_count'))

    fig1 = px.line(weekly_accidents, x='datetime_occ', y='accident_count', color='area_name',
        title='<b>Weekly Accident Trends by Police District</b><br><sup>Normalized Weekly Counts | 2010-2020</sup>',
        labels={'accident_count': 'Accidents per Week', 'datetime_occ': 'Reporting Week'},
        color_discrete_sequence=COLOR_SCALE, height=500, template=TEMPLATE)

    fig1.update_layout(hovermode='x unified',
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=3, label="3mo", step="month", stepmode="backward"),
                    dict(count=6, label="6mo", step="month", stepmode="backward"),
                    dict(step="all")
                ]),
                bgcolor='rgba(42,76,125,0.1)'
            ),
            rangeslider=dict(visible=True)),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1),
        margin=dict(t=100))

    # Visualization 2: Temporal Patterns
    # ----------------------------------
    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                'July', 'August', 'September', 'October', 'November', 'December']
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    fig2 = (px.line(df.assign(
            month=df['datetime_occ'].dt.month_name(),
            day=df['datetime_occ'].dt.day_name(),
            year=df['datetime_occ'].dt.year)
        .groupby(['month', 'day', 'year'], as_index=False)
        .size()
        .assign(month=lambda x: pd.Categorical(x['month'], categories=month_order, ordered=True),
                day=lambda x: pd.Categorical(x['day'], categories=day_order, ordered=True))
        .groupby(['month', 'day'], as_index=False)
        .agg(avg_accidents=('size', 'mean')),  # Average across years
        x='month', y='avg_accidents', color='day',
        title='<b>Monthly Accident Patterns</b><br><sup>Average Daily Accidents by Weekday</sup>',
        labels={'avg_accidents': 'Avg Daily Accidents', 'month': ''},
        color_discrete_sequence=COLOR_SCALE, markers=True)
    .update_layout(
        hovermode='x unified',
        legend=dict(title='Weekday', orientation='h', yanchor='bottom', y=1.02, x=1),
        yaxis=dict(showgrid=False), xaxis=dict(tickangle=45), margin=dict(t=100), height=500)
    .update_traces(line=dict(width=2), hovertemplate='<b>%{color}</b><br>Month: %{x}<br>Avg Accidents: %{y:.1f}<extra></extra>'))


    # Visualization 3: Weather Impact Analysis
    # ----------------------------------------
    # Calculate normalized rain impact
    rain_analysis = (df.groupby('is_raining').agg(
                total_accidents=('is_raining', 'size'),
                total_days=('datetime_occ', lambda x: x.dt.date.nunique())).reset_index())
    rain_analysis['accidents_per_day'] = rain_analysis['total_accidents'] / rain_analysis['total_days']
    rain_analysis['condition'] = np.where(rain_analysis['is_raining'], 'Rainy Days', 'Dry Days')

    # Create subplot figure
    fig3 = make_subplots(rows=3, cols=1,
        subplot_titles=(
            "Temperature Extremes Analysis (TMAX Distribution)",
            "Temperature Extremes Analysis (TMIN Distribution)", 
            "Accident Rate by Precipitation Status"
        ),
        vertical_spacing=0.15,
        specs=[[{"type": "histogram"}], [{"type": "histogram"}], [{"type": "bar"}]]
    )

    # Temperature distributions
    fig3.add_trace(go.Histogram(x=df['TMAX'], name='TMAX', marker_color=COLOR_SCALE[0],
        nbinsx=20, hovertemplate="Temp: %{x}°F<br>Count: %{y}<extra></extra>"), row=1, col=1)

    fig3.add_trace(go.Histogram(x=df['TMIN'], name='TMIN', marker_color=COLOR_SCALE[1],
        nbinsx=20, hovertemplate="Temp: %{x}°F<br>Count: %{y}<extra></extra>"), row=2, col=1)

    # Normalized rain impact
    fig3.add_trace(go.Bar(x=rain_analysis['condition'], y=rain_analysis['accidents_per_day'],
        marker_color=[COLOR_SCALE[2], COLOR_SCALE[3]], name='Accident Rate',
        text=np.round(rain_analysis['accidents_per_day'], 2), textposition='auto',
        hovertemplate="<b>%{x}</b><br>Rate: %{y:.2f} accidents/day<extra></extra>"), row=3, col=1)

    fig3.update_layout(height=900, showlegend=False, template=TEMPLATE,
        title_text="<b>Meteorological Impact Analysis</b><br><sup>Normalized Weather Effects on Accident Frequency</sup>",
        title_x=0.5, margin=dict(t=120),
        annotations=[
            dict(text="Temperature distributions show concentration of accidents at different temperature ranges",
                xref="paper", yref="paper",
                x=0.5, y=0.25, showarrow=False,
                font=dict(size=12, color="#666666"))])

    # Axis labels
    fig3.update_xaxes(title_text="Maximum Temperature (°F)", row=1, col=1)
    fig3.update_xaxes(title_text="Minimum Temperature (°F)", row=2, col=1)
    fig3.update_yaxes(title_text="Accident Count", row=1, col=1)
    fig3.update_yaxes(title_text="Accident Count", row=2, col=1)
    fig3.update_yaxes(title_text="Accidents per Day", row=3, col=1)
    fig3.update_yaxes(showgrid=False, row=3, col=1)


    # 3. Data Analysis: predict seriousness of accident -> Non Injury, Visible Injury, Severe Injury
    # =================
    # Discard unknown values for Level Injury
    df = df[df['Injury_Level'] != 'unknown']

    # Group groups for better classification
    # Step 2: Group values for Severe Injury
    df['Injury_Level'] = df['Injury_Level'].replace({
        'Complaint of Injury': 'Visible Injury',
        'Fatal Injury': 'Severe Injury'
    })

    # Make the partitions
    n = len(df)
    train_end = int(n * 0.85)
    val_end = train_end + int(n * 0.15)

    # Split data chronologically
    train_df = df.iloc[:train_end]
    val_df = df.iloc[train_end:val_end]

    print(f"Training set: {len(train_df)} records from {train_df['datetime_occ'].min()} to {train_df['datetime_occ'].max()}")
    print(f"Validation set: {len(val_df)} records from {val_df['datetime_occ'].min()} to {val_df['datetime_occ'].max()}")

    # 4. Identify relevant features for modeling
    # ==========================================
    time_cols = ['occ_day', 'occ_month', 'occ_hour']
    numerical_cols = ['vict_age', 'is_weekend', 'temp_range', 'TMAX', 'TMIN', 'PRCP']
    categorical_cols = ['Location_Type', 'Collision_Type', 'Collision_Factor', 'premis_cd']

    # Define the target variable
    target = 'Injury_Level'

    # Create preprocessing pipelines
    numeric_transformer = Pipeline(steps=[
        ('scaler', StandardScaler())
    ])

    categorical_transformer = Pipeline(steps=[
        ('onehot', OneHotEncoder(handle_unknown='ignore'))
    ])

    # Combine all preprocessing steps - time features are passed through without scaling
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numerical_cols),
            ('time', 'passthrough', time_cols),
            ('cat', categorical_transformer, categorical_cols)
        ])
    
    # 5. Create the model: Random Forest Classifier
    # ==========================================

    rf_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', RandomForestClassifier(
            n_estimators=200,        
            max_depth=25,           
            min_samples_split=8,
            min_samples_leaf=3,
            max_features='sqrt',
            class_weight='balanced',
            criterion='entropy',
            random_state=42,
            n_jobs=-1
        ))
    ])

    # Split features and target
    all_features = time_cols + numerical_cols + categorical_cols
    X_train = train_df[all_features]
    y_train = train_df[target]
    X_val = val_df[all_features]
    y_val = val_df[target]

    # Train the model
    print("Training Random Forest model...")
    rf_pipeline.fit(X_train, y_train)

    # Make predictions
    print("Making predictions on validation set...")
    y_pred = rf_pipeline.predict(X_val)

    # Evaluate the model
    print("\nModel Evaluation:")
    print(f"Accuracy: {accuracy_score(y_val, y_pred):.4f}")
    print("\nClassification Report:")
    print(classification_report(y_val, y_pred))

    # Extract feature importance
    if hasattr(rf_pipeline.named_steps['classifier'], 'feature_importances_'):
        # Get feature names after preprocessing
        ohe = preprocessor.named_transformers_['cat'].named_steps['onehot']
        cat_feature_names = list(ohe.get_feature_names_out(categorical_cols))
        feature_names = numerical_cols + time_cols + cat_feature_names
        
        # Get feature importances
        importances = rf_pipeline.named_steps['classifier'].feature_importances_
        
        # Sort feature importances in descending order
        indices = np.argsort(importances)[::-1]
        
        # Create a DataFrame for the feature importances
        feature_importance_df = pd.DataFrame({
            'Feature': [feature_names[i] for i in indices],
            'Importance': importances[indices]
        })
        
        top_features_df = feature_importance_df.head(20)
        fig_importance = px.bar(top_features_df, y='Feature', x='Importance', orientation='h',
            title='<b>Top 20 Feature Importances</b><br><sup>Factors that most influence injury severity prediction</sup>',
            color='Importance', color_continuous_scale='Blues', template='plotly_white')
        
        # Customize the layout
        fig_importance.update_layout(yaxis=dict(title='', categoryorder='total ascending'), xaxis=dict(title='Relative Importance'),
                                    coloraxis_showscale=False, height=600, margin=dict(t=100, l=20, r=20, b=20))
        
    # Create a confusion matrix visualization with Plotly
    cm = confusion_matrix(y_val, y_pred)
    classes = rf_pipeline.classes_

    # Create a heatmap with annotations
    fig_cm = px.imshow(cm, labels=dict(x="Predicted Label", y="True Label", color="Count"),
                x=classes, y=classes, color_continuous_scale='Blues',
                title='<b>Confusion Matrix</b><br><sup>Assessment of classification accuracy across injury categories</sup>')

    # Add text annotations to each cell
    annotations = []
    for i, row in enumerate(cm):
        for j, value in enumerate(row):
            annotations.append(
                dict(x=j, y=i, text=str(value), showarrow=False, font=dict(color='white' if value > cm.max() / 2 else 'black')))

    fig_cm.update_layout(annotations=annotations,height=500, margin=dict(t=100, l=20, r=20, b=20), template='plotly_white')


    # Create a feature correlation heatmap
    corr_features = numerical_cols + time_cols
    corr_df = X_train[corr_features].copy()
    corr_matrix = corr_df.corr()

    # Create correlation heatmap
    fig_corr = px.imshow(corr_matrix, text_auto='.2f', color_continuous_scale='RdBu_r',
        title='<b>Feature Correlation Matrix</b><br><sup>Relationship between numerical and time-based features</sup>',
        template='plotly_white', aspect="equal")

    fig_corr.update_layout(height=600, margin=dict(t=100, l=20, r=20, b=20))

    return {
        "time_series_fig": fig1,
        "date_acc_patterns": fig2,
        "weather_impact_fig": fig3,
        "feature_importance_fig": fig_importance,
        "results_cm_fig": fig_cm,
        "results_corr_fig": fig_corr
    }