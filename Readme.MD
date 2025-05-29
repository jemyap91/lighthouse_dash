# Assessment Guide Compiler

A python script that consolidates multiple *Assessment Guide* workbooks into a single master sheet that serves as the primary data source for the powerbi report.

---

## ✨ Features

* Ingests every workbook in an `assessment_guide` folder.
* **Column sanity‑checks** against a *Column Reference* file, with warnings for duplicates or missing fields.
* **Glossary tab** kept on a separate tab. Definitions used to explain terminology on dashboard.
* **Config file** – point the script to filenames or folders necessary for consolidation by editing the config.yaml file.

---

## 🗂️ Folder Structure

```
repo_root/
├─ main.py   ← the script used to compile the assessment guide workbooks
├─ config.yaml                    ← run‑time settings (see below)
├─ requirements.txt               ← Python dependencies file
├─ README.md                      ← this file
├─ master_sheet_reference.xlsx    ← authoritative list of tables & columns
├─ assessment_guides/             ← **folder** containing Assessment Guide excel workbooks, e.g.
│  ├─ assessment_guide_qiddiya.xlsx
│  ├─ assessment_guide_diriyah.xlsm
│  └─ ...
└─ master_sheet.xlsx/             ← **output** fle
```

---

## ⚙️ Configuration

`config.yaml` drives everything. A minimal example:

```yaml
# config.yaml
column_reference_file_name: "master_sheet_reference.xlsx"
assessment_guide_folder_name: "assessment_guide"   # folder
compiled_master_sheet: "master_sheet.xlsx"
```

* **`column_reference_file_name`** – excel file where each sheet refers to a dash in the report and contains the *Table* and *Column* lists for data extraction from the assessment guide files.
* **`assessment_guide_folder_name`** – directory containing one workbook per DevCo (must start with `assessment_guide_`).
* **`compiled_master_sheet`** – output file name.
---

## 🤖 How It Works (High‑level Flow)

```text
load_config() → read YAML
   ↓
scan master_sheet_reference.xlsx
   ↓
for each Assessment Guide workbook
       • validate & rename columns
       • collect subset of columns per reference table
   ↓
concat columns for each DevCo horizontally (column-wise) and data for each DevCo vertically (row-wise)
   ↓
write master sheet + Glossary tab
```

---