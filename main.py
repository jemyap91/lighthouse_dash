import pandas as pd
from pathlib import Path
import yaml

BASE_DIR = Path(__file__).parent

def load_config(path="config.yaml"):
    cfg_path = BASE_DIR / path
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)

def main(cfg):
    repo_root = BASE_DIR
    print(repo_root)


    ref_path  = repo_root / cfg["column_reference_file_name"]
    data_path = repo_root / cfg["assessment_guide_file_name"]

    ref_sheets  = pd.read_excel(ref_path,  sheet_name=None)

    ref_tabs = []     

    for sheet_name, df in ref_sheets.items():
        if {"Table", "Column"}.issubset(df.columns):
            table_cols = (
                df.groupby("Table")["Column"]
                .apply(list)
                .to_dict()
            )
            ref_tabs.append({ sheet_name: table_cols })
        else:
            print(f"Skipping sheet {sheet_name!r}: missing \"Table\" and \"Columm\" columns")

    pieces = []

    for tab_dict in ref_tabs:
        sheet_key = next(iter(tab_dict.keys()))
        sheet_map = next(iter(tab_dict.values()))
        
        for table_name, cols in sheet_map.items(): 
            df = pd.read_excel(data_path, sheet_name=table_name, header=4) 
            df.columns = df.columns.str.strip()
            present = [c for c in cols if c in df.columns]
            missing = set(cols) - set(present)
            if missing:
                print(f"⚠️ Tab {sheet_key}, in table {table_name!r}, these columns were missing: {missing}")
            
            df_sub = df[present].copy()
            for c in missing:
                df_sub[c] = pd.NA
        
            pieces.append(df_sub)

    master_df = pd.concat(pieces, axis=1).reset_index(drop=True)

    master_df.head(10)

    output_path = repo_root/ cfg["compiled_master_sheet"]
    master_df.to_excel(output_path, index=False)

    print(f"✅ Saved master sheet to {output_path!r}")

if __name__ == "__main__":
    cfg = load_config()
    main(cfg)