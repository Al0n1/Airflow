import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import os
import joblib

def run_data_preprocessing(base_path: str):
    data_processed_dir = os.path.join(base_path, 'data', 'processed')
    data_raw_dir = os.path.join(base_path, 'data', 'raw')

    os.makedirs(data_processed_dir, exist_ok=True)
    
    input_file = os.path.join(data_raw_dir, 'iris_dataset.csv')
    df = pd.read_csv(input_file)
    
    X = df.drop(['target', 'target_names'], axis=1)
    y = df['target_names']

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    np.save(os.path.join(data_processed_dir, 'X_train.npy'), X_train_scaled)
    np.save(os.path.join(data_processed_dir, 'X_test.npy'), X_test_scaled)
    np.save(os.path.join(data_processed_dir, 'y_train.npy'), y_train)
    np.save(os.path.join(data_processed_dir, 'y_test.npy'), y_test)

    joblib.dump(scaler, os.path.join(data_processed_dir, 'scaler.joblib'))
    
    print(f"Preprocessing completed. Files saved in {data_processed_dir}")

if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    airflow_project_dir = os.path.abspath(os.path.join(current_script_dir, '..'))
    run_data_preprocessing(base_path=airflow_project_dir)
