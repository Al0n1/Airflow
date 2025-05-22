import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib
import os

def run_model_training(base_path: str):
    models_dir = os.path.join(base_path, 'models')
    data_processed_dir = os.path.join(base_path, 'data', 'processed')

    os.makedirs(models_dir, exist_ok=True)
    
    X_train = np.load(os.path.join(data_processed_dir, 'X_train.npy'), allow_pickle=True)
    X_test = np.load(os.path.join(data_processed_dir, 'X_test.npy'), allow_pickle=True)
    y_train = np.load(os.path.join(data_processed_dir, 'y_train.npy'), allow_pickle=True)
    y_test = np.load(os.path.join(data_processed_dir, 'y_test.npy'), allow_pickle=True)
    
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=None,
        min_samples_split=2,
        min_samples_leaf=1,
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    print("\nModel Performance Report:")
    print(classification_report(y_test, y_pred))

    model_path = os.path.join(models_dir, 'rf_model.joblib')
    joblib.dump(model, model_path)
    print(f"\nModel saved to {model_path}")

if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    airflow_project_dir = os.path.abspath(os.path.join(current_script_dir, '..'))
    run_model_training(base_path=airflow_project_dir)
