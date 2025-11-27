import pandas as pd
import os
import argparse
import numpy as np
import random


def inject_errors(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    safe_index = lambda: random.randint(0, len(df) - 1)

    # 1) Missing values
    i = safe_index()
    col = random.choice(df.columns)
    df.loc[i, col] = None

    # 2) Invalid country
    if "country" in df.columns:
        df.loc[safe_index(), "country"] = "France"

    # 3) Invalid gender
    if "gender" in df.columns:
        df.loc[safe_index(), "gender"] = "child"

    # 4) Negative age
    if "age" in df.columns:
        df.loc[safe_index(), "age"] = -5

    # 5) String in income
    if "income" in df.columns:
        df.loc[safe_index(), "income"] = "not_a_number"

    # 6) Drop required column
    required_cols = ["age", "gender", "country", "income"]
    drop_col = random.choice(required_cols)
    if drop_col in df.columns:
        df = df.drop(columns=[drop_col])

    # 7) Duplicate row
    dup_idx = safe_index()
    df = pd.concat([df, df.iloc[[dup_idx]].copy()], ignore_index=True)

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_path', type=str, required=True)
    parser.add_argument('--raw_data_dir', type=str, required=True)
    parser.add_argument('--num_files', type=int, default=1000)
    args = parser.parse_args()

    df = pd.read_csv(args.dataset_path)
    rows = len(df)

    os.makedirs(args.raw_data_dir, exist_ok=True)

    rows_per_file = int(np.ceil(rows / args.num_files))

    i = 0
    while i * rows_per_file < rows:
        start = i * rows_per_file
        end = min(start + rows_per_file, rows)

        split_df = df.iloc[start:end].copy()

        if split_df.empty:
            print(f"[SKIP] Split {i+1} is empty → not saved")
            i += 1
            continue

        split_df = inject_errors(split_df)

        out_path = os.path.join(args.raw_data_dir, f"raw_split_{i+1}.csv")
        split_df.to_csv(out_path, index=False)

        print(f"[OK] Created {out_path} with {len(split_df)} rows")
        i += 1

    print("\nAll splits created successfully!")


if __name__ == "__main__":
    main()
