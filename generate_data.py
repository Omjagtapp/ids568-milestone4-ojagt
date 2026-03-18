import argparse
import pandas as pd
import numpy as np
import os
import hashlib

def compute_data_hash(df: pd.DataFrame) -> str:
    """Compute hash for verification to prove reproducibility."""
    content = pd.util.hash_pandas_object(df).values
    return hashlib.sha256(content.tobytes()).hexdigest()[:16]

def generate_data(rows: int, output_dir: str, seed: int):
    print(f"Generating {rows} rows of synthetic data with seed {seed}...")
    np.random.seed(seed)
    os.makedirs(output_dir, exist_ok=True)
    
    chunk_size = 1000000 # 1 million rows per chunk to save memory
    total_hash = ""
    
    for i in range(0, rows, chunk_size):
        current_rows = min(chunk_size, rows - i)
        
        data = {
            "user_id": np.random.randint(1, 50000, size=current_rows).astype(str),
            "amount": np.random.exponential(scale=50, size=current_rows),
            "timestamp": pd.date_range(start='1/1/2026', periods=current_rows, freq='s'),
            "category": np.random.choice(["Electronics", "Groceries", "Clothing", "Entertainment"], size=current_rows)
        }
        
        df = pd.DataFrame(data)
        file_path = os.path.join(output_dir, f"data_chunk_{i}.parquet")
        
        # FIXED: Coerce timestamps to microseconds (us) so PySpark doesn't crash
        df.to_parquet(file_path, engine='pyarrow', coerce_timestamps='us', allow_truncated_timestamps=True)
        
        if i == 0:
            total_hash = compute_data_hash(df)
            
        print(f"Wrote {current_rows} rows to {file_path}")
        
    print(f"Data generation complete! Verification hash (first chunk): {total_hash}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data.")
    parser.add_argument("--rows", type=int, default=10000000, help="Number of rows to generate")
    parser.add_argument("--output", type=str, required=True, help="Output directory path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    generate_data(args.rows, args.output, args.seed)