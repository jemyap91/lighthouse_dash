import pandas as pd
from pathlib import Path
from collections import defaultdict, Counter
from collections import OrderedDict
import warnings
import yaml
warnings.simplefilter("ignore", UserWarning)

BASE_DIR = Path(__file__).parent

def load_config(path="config.yaml"):
    cfg_path = BASE_DIR / path
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)

def main(cfg):
    repo_root = BASE_DIR
    print(repo_root)


    ref_path  = repo_root / cfg["column_reference_file_name"]
    ag_folder = repo_root / cfg["assessment_guide_folder_name"]
    ag_files  = sorted(ag_folder.glob("*.xls*"))

    print("Found workbooks:")
    for p in ag_files:
        print("•", p.name)

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

    
    table_cols = defaultdict(list)

    for tab_dict in ref_tabs:
        sheet_map = next(iter(tab_dict.values()))
        for table_name, cols in sheet_map.items():
            table_cols[table_name].extend(cols)
            
    for table_name, all_cols in table_cols.items():
        counts = Counter(all_cols)
        dupes = [col for col, n in counts.items() if n > 1]
        if dupes:
            print(f"⚠️ Table “{table_name}” has duplicate columns: {dupes}")
        else:
            print(f"✅ Table “{table_name}” has no repeated columns.")

    for table_name, cols in table_cols.items():
        table_cols[table_name] = list(OrderedDict.fromkeys(cols))
    
    # # Collect duplicated column names across tables
    # all_cols = []
    # for cols in table_cols.values():
    #     all_cols.extend(cols)
    # dupe_cols = {c for c, cnt in Counter(all_cols).items() if cnt > 1}

    file_frames = []   # to collect df for each file

    for data_path in ag_files:
        # derive suffix
        parts = data_path.stem.split("assessment_guide_")
        suffix = parts[1]

        print(f"Reading {data_path.name!r} (suffix = {suffix})")

        pieces = []
        for table_name, cols in table_cols.items():
            if table_name == 'Glossary&Definitions':
                glossary_df = pd.read_excel(data_path, sheet_name=table_name, header=4)
            else:
                df = pd.read_excel(data_path, sheet_name=table_name, header=4)  # pull in AG data. Each table name from column reference sheet is a sheet name in the AG file.
                df.columns = (
                    df.columns
                    .str.replace(r'\s*\n\s*', ' ', regex=True)  
                    .str.strip()                                
                )

                present = [c for c in cols if c in df.columns] # cols = columns from reference, present = columns in AG
                missing = set(cols) - set(present)
                if missing:
                    print(f"⚠️ In table {table_name!r}, missing columns: {missing}")

                df_sub = df[present].copy()
                for c in missing:
                    df_sub[c] = pd.NA

                # rename_map = {
                # col: f"{col}_{table_name}"
                # for col in df_sub.columns
                # if col in dupe_cols
                # }
                # if rename_map:
                #     df_sub = df_sub.rename(columns=rename_map)

                pieces.append(df_sub)

        file_df = pd.concat(pieces, axis=1) # master df for each file

        file_df.insert(0, "DevCo", [suffix] * len(file_df)) # append DevCo column

        file_frames.append(file_df) # append master df for each file to a list

    master_df = pd.concat(file_frames, axis=0, ignore_index=True) # stack master df for each file row-wise

    print("Final master shape:", master_df.shape)

    output_path = repo_root/ cfg["compiled_master_sheet"]

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        master_df.to_excel(writer, sheet_name='Sheet1', index=False)
        glossary_df.to_excel(writer, sheet_name='Glossary', index=False)
        print(f"✅ Saved master sheet to {output_path!r}")

if __name__ == "__main__":
    cfg = load_config()
    main(cfg)