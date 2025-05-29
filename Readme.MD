# Assessment Guide Compiler

A lightweight Python utility that consolidates multiple *Assessment Guide* workbooks into a single, analytics‑ready master sheet.

---

## ✨ Features

* **Automated ingest** of every workbook in an `assessment_guide` folder.
* **Column sanity‑checks** against a central *Column Reference* file, with helpful warnings for duplicates or missing fields.
* **Glossary extraction** kept on a separate tab so analysts always have the latest definitions at hand.
* **Zero‑code config** – point the script to new filenames or folders by editing one small YAML file.

---

## 🗂️ Folder Structure

```
repo_root/
├─ assessment_guide_compiler.py   ← the script shown in this repo
├─ config.yaml                    ← run‑time settings (see below)
├─ README.md                      ← this file
├─ column_reference.xlsx          ← authoritative list of tables & columns
├─ assessment_guides/             ← **folder** full of Excel workbooks, e.g.
│  ├─ assessment_guide_ACME.xlsx
│  ├─ assessment_guide_FooCorp.xlsm
│  └─ ...
└─ compiled/                      ← **output** folder (created automatically)
   └─ master_compiled.xlsx
```

Feel free to rename paths – just mirror the changes in `config.yaml`.

---

## ⚙️ Configuration

`config.yaml` drives everything. A minimal example:

```yaml
# config.yaml
column_reference_file_name: "column_reference.xlsx"
assessment_guide_folder_name: "assessment_guides"   # folder, _not_ a file
compiled_master_sheet: "compiled/master_compiled.xlsx"
```

* \`\` – Excel file where each sheet maps a *Table* → *Column* list.
* \`\` – directory containing one workbook per DevCo (must start with `assessment_guide_`).
* \`\` – destination for the merged output.

---

## 🚀 Quick Start

1. **Clone** the repository (or create a new one and drop the script in).
2. `pip install -r requirements.txt`
   *(at minimum: **`**, **`**, **\`\`**)*
3. Make sure your input Excel files match the structure above.
4. Run:

```bash
python assessment_guide_compiler.py
```

The console will print status messages (✔ and ⚠). When finished, look in the `compiled/` folder for `master_compiled.xlsx`.

---

## 🤖 How It Works (High‑level Flow)

```text
load_config() → read YAML
   ↓
scan column_reference.xlsx
   ↓
for each Assessment Guide workbook
       • validate & rename columns
       • collect subset per reference table
   ↓
concat all DevCos vertically
   ↓
write master sheet + Glossary tab
```

---

## 📝 Saving & Committing to Your Repo

1. Place `and` in the root of the repo (or a `/src` folder if you prefer – just update instructions).
2. Add a \`\` entry for temporary artefacts:

```
# compiled outputs
compiled/
*.py[cod]
__pycache__/
```

3. Commit and push:

```bash
git add assessment_guide_compiler.py README.md config.yaml .gitignore
git commit -m "Add Assessment Guide compiler utility"
git push origin main  # or your chosen branch
```

---

## 🛠️ Extending

* **Extra validation** – plug in Great Expectations or pandera.
* **Different outputs** – swap `pd.ExcelWriter` for CSV or a database connector.
* **Logging** – replace `print` with the `logging` module for configurable log levels.

---

## 🤝 Contributing

Pull requests are welcome! Please open an issue first to discuss major changes.

---

## 📄 License

MIT – see `LICENSE` file for details.

---

> Built with ☕ and 🐼s (pandas) to save analysts from repetitive copy‑paste.
