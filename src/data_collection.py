import pandas as pd
from sklearn.datasets import load_iris
import os

def run_data_collection(base_path: str):
    data_raw_dir = os.path.join(base_path, 'data', 'raw')
    os.makedirs(data_raw_dir, exist_ok=True)
    
    iris = load_iris()
    
    df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
    df['target'] = iris.target
    df['target_names'] = df['target'].map({
        0: 'setosa',
        1: 'versicolor',
        2: 'virginica'
    })
    
    output_file = os.path.join(data_raw_dir, 'iris_dataset.csv')
    df.to_csv(output_file, index=False)
    print(f"Dataset saved to {output_file}")

if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    airflow_project_dir = os.path.abspath(os.path.join(current_script_dir, '..')) 
    run_data_collection(base_path=airflow_project_dir)
