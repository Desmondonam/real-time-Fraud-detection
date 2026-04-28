"""
Generates the project blog as a Word document.
Run: python scripts/generate_blog.py
Output: Real-Time_Fraud_Detection_Blog.docx (project root)
"""

import os
from docx import Document
from docx.shared import Pt, RGBColor, Inches, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.style import WD_STYLE_TYPE
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "Real-Time_Fraud_Detection_Blog.docx")

# ── colour palette ─────────────────────────────────────────────────────────────
DARK_BLUE   = RGBColor(0x1A, 0x23, 0x3A)   # headings
ACCENT_BLUE = RGBColor(0x23, 0x6F, 0xB4)   # sub-headings / links
CODE_BG     = RGBColor(0xF4, 0xF4, 0xF4)   # code block shading
CODE_FG     = RGBColor(0x1E, 0x1E, 0x1E)   # code text
GREY        = RGBColor(0x55, 0x55, 0x55)   # body text
GREEN       = RGBColor(0x1E, 0x7E, 0x34)   # callout / result
RED         = RGBColor(0xC0, 0x39, 0x2B)   # warning


def shade_cell(cell, hex_color="F4F4F4"):
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color)
    tcPr.append(shd)


def add_horizontal_rule(doc):
    p = doc.add_paragraph()
    pPr = p._p.get_or_add_pPr()
    pBdr = OxmlElement("w:pBdr")
    bottom = OxmlElement("w:bottom")
    bottom.set(qn("w:val"), "single")
    bottom.set(qn("w:sz"), "6")
    bottom.set(qn("w:space"), "1")
    bottom.set(qn("w:color"), "AAAAAA")
    pBdr.append(bottom)
    pPr.append(pBdr)
    return p


def h1(doc, text):
    p = doc.add_heading(text, level=1)
    for run in p.runs:
        run.font.color.rgb = DARK_BLUE
        run.font.size = Pt(22)
        run.font.bold = True
    p.paragraph_format.space_before = Pt(18)
    p.paragraph_format.space_after = Pt(6)
    return p


def h2(doc, text):
    p = doc.add_heading(text, level=2)
    for run in p.runs:
        run.font.color.rgb = ACCENT_BLUE
        run.font.size = Pt(16)
        run.font.bold = True
    p.paragraph_format.space_before = Pt(14)
    p.paragraph_format.space_after = Pt(4)
    return p


def h3(doc, text):
    p = doc.add_heading(text, level=3)
    for run in p.runs:
        run.font.color.rgb = DARK_BLUE
        run.font.size = Pt(13)
        run.font.bold = True
    p.paragraph_format.space_before = Pt(10)
    p.paragraph_format.space_after = Pt(2)
    return p


def body(doc, text):
    p = doc.add_paragraph(text)
    p.paragraph_format.space_after = Pt(6)
    for run in p.runs:
        run.font.color.rgb = GREY
        run.font.size = Pt(11)
    return p


def bullet(doc, text, level=0):
    p = doc.add_paragraph(text, style="List Bullet")
    p.paragraph_format.left_indent = Inches(0.25 * (level + 1))
    p.paragraph_format.space_after = Pt(3)
    for run in p.runs:
        run.font.size = Pt(11)
        run.font.color.rgb = GREY
    return p


def code_block(doc, text):
    """Renders a monospaced code block in a shaded single-cell table."""
    table = doc.add_table(rows=1, cols=1)
    table.style = "Table Grid"
    cell = table.cell(0, 0)
    shade_cell(cell, "F4F4F4")
    cell.paragraphs[0].clear()
    for line in text.split("\n"):
        p = cell.add_paragraph(line)
        run = p.runs[0] if p.runs else p.add_run(line)
        run.font.name = "Courier New"
        run.font.size = Pt(9)
        run.font.color.rgb = CODE_FG
        p.paragraph_format.space_after = Pt(0)
        p.paragraph_format.space_before = Pt(0)
    # remove the auto-created blank first paragraph
    first = cell.paragraphs[0]
    if not first.text.strip():
        first._element.getparent().remove(first._element)
    doc.add_paragraph()  # spacer after block
    return table


def callout(doc, text, color=GREEN, label="Result"):
    p = doc.add_paragraph()
    run = p.add_run(f"  {label}:  ")
    run.bold = True
    run.font.color.rgb = color
    run.font.size = Pt(10)
    run2 = p.add_run(text)
    run2.font.color.rgb = color
    run2.font.size = Pt(10)
    p.paragraph_format.space_after = Pt(6)
    return p


def info_table(doc, headers, rows):
    table = doc.add_table(rows=1 + len(rows), cols=len(headers))
    table.style = "Table Grid"
    # header row
    hdr = table.rows[0]
    for i, h in enumerate(headers):
        c = hdr.cells[i]
        shade_cell(c, "1A233A")
        run = c.paragraphs[0].add_run(h)
        run.bold = True
        run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        run.font.size = Pt(10)
    # data rows
    for r_idx, row in enumerate(rows):
        tr = table.rows[r_idx + 1]
        fill = "FFFFFF" if r_idx % 2 == 0 else "EEF4FB"
        for c_idx, val in enumerate(row):
            cell = tr.cells[c_idx]
            shade_cell(cell, fill)
            run = cell.paragraphs[0].add_run(str(val))
            run.font.size = Pt(10)
            run.font.color.rgb = GREY
    doc.add_paragraph()
    return table


# ══════════════════════════════════════════════════════════════════════════════
#  DOCUMENT
# ══════════════════════════════════════════════════════════════════════════════

def build():
    doc = Document()

    # ── page margins ──────────────────────────────────────────────────────────
    for section in doc.sections:
        section.top_margin    = Cm(2.5)
        section.bottom_margin = Cm(2.5)
        section.left_margin   = Cm(2.8)
        section.right_margin  = Cm(2.8)

    # ── default body font ─────────────────────────────────────────────────────
    style = doc.styles["Normal"]
    style.font.name = "Calibri"
    style.font.size = Pt(11)

    # ══════════════════════════════════════════════════════════════════════════
    #  COVER
    # ══════════════════════════════════════════════════════════════════════════
    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    tr = title.add_run("Building a Real-Time Fraud Detection System")
    tr.bold = True
    tr.font.size = Pt(28)
    tr.font.color.rgb = DARK_BLUE

    sub = doc.add_paragraph()
    sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sr = sub.add_run(
        "From Raw CSV to Live Dashboard — A Complete End-to-End Technical Walkthrough"
    )
    sr.font.size = Pt(14)
    sr.font.color.rgb = ACCENT_BLUE
    sr.italic = True

    meta = doc.add_paragraph()
    meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
    mr = meta.add_run("Author: Desmond Onam   |   Date: April 2026   |   Stack: Python · PostgreSQL · Airflow · Grafana · Docker · DVC")
    mr.font.size = Pt(10)
    mr.font.color.rgb = GREY

    add_horizontal_rule(doc)
    doc.add_paragraph()

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 1 — INTRODUCTION
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "1. Introduction")
    body(doc,
         "Credit card fraud costs the global financial industry over $30 billion every year. "
         "Traditional rule-based systems catch only a fraction of fraudulent transactions while "
         "generating high volumes of false positives that frustrate legitimate customers and "
         "consume analyst time. The modern answer is a hybrid detection system that combines "
         "the interpretability of domain-knowledge rules with the pattern-recognition power of "
         "unsupervised machine learning — all orchestrated in a real-time pipeline.")
    body(doc,
         "This article walks through the complete engineering process of building such a system "
         "from scratch: ingesting and cleaning raw transaction data, training an Isolation Forest "
         "anomaly detector, wiring everything together with an Apache Airflow hourly batch pipeline, "
         "and delivering live insights through a Grafana dashboard. Every design decision is explained, "
         "every piece of code is shown, and the full deployment is demonstrated step by step.")

    h2(doc, "1.1 The Dataset")
    body(doc,
         "The project uses the publicly available Kaggle Credit Card Fraud Detection dataset, "
         "which contains 284,807 transactions made by European cardholders over 48 hours in September 2013. "
         "The dataset is highly imbalanced — only 492 transactions (0.173%) are fraudulent.")

    info_table(doc,
        ["Column", "Type", "Description"],
        [
            ["Time",        "Float", "Seconds elapsed since the first transaction (0 – 172,792)"],
            ["V1 – V28",    "Float", "PCA-transformed features (original features are masked for privacy)"],
            ["Amount",      "Float", "Raw transaction amount in EUR"],
            ["Class",       "Int",   "Ground truth label: 0 = legitimate, 1 = fraud"],
        ]
    )

    body(doc,
         "Because V1–V28 are the result of a PCA transformation applied to protect cardholder privacy, "
         "they cannot be interpreted individually. However, published analysis of this dataset shows "
         "that V14, V17, V12, V10, V3, and V4 carry the strongest individual correlation with fraud — "
         "a fact exploited by the rule engine described in Section 4.")

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 2 — ARCHITECTURE
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "2. System Architecture")
    body(doc,
         "The system is composed of four sequential stages that together form an end-to-end "
         "machine learning pipeline. Figure 1 shows the high-level data flow.")

    code_block(doc, """\
Raw CSV (144 MB, 284,807 rows)
        │
        │  DVC tracks file → local remote storage
        ▼
┌──────────────────────────────────┐
│  Stage 1 — Ingest & Clean        │
│  src/ingest_and_clean.py         │
│  • Schema validation             │
│  • Null imputation               │
│  • Amount Z-score normalisation  │
│  → 284,807 rows into PostgreSQL  │
└────────────────┬─────────────────┘
                 │
                 ▼
     PostgreSQL 17 (fraud_detection)
     ┌──────────────┐  ┌───────────────────┐
     │ transactions │  │   fraud_alerts    │
     │ 284,807 rows │  │   1,900+ rows     │
     └──────┬───────┘  └─────────┬─────────┘
            │                    │
            ▼                    ▼
┌──────────────────────┐  ┌─────────────────────┐
│  Stage 2 — Detect    │  │  Stage 4 — Dashboard │
│  src/detection/      │  │  Grafana OSS 11.4    │
│  • RuleEngine        │  │  Docker container    │
│  • IsolationForest   │  │  localhost:3000      │
│  • CombinedScorer    │  └─────────────────────┘
└──────────┬───────────┘
           │  scoring logic used by
           ▼
┌──────────────────────┐
│  Stage 3 — ETL       │
│  Airflow DAG         │
│  @hourly schedule    │
│  Docker container    │
│  localhost:8080      │
└──────────────────────┘""")

    body(doc,
         "Each stage is independently runnable: you can rerun the ingest script after schema changes, "
         "retrain the model without touching the ETL pipeline, or restart Grafana without affecting "
         "PostgreSQL. This loose coupling makes the system maintainable and testable in isolation.")

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 3 — TECH STACK
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "3. Tech Stack")
    info_table(doc,
        ["Layer", "Technology", "Version", "Purpose"],
        [
            ["Language",        "Python",               "3.12",     "All pipeline and ML code"],
            ["Database",        "PostgreSQL",           "17",       "Primary data store"],
            ["Data versioning", "DVC",                  "3.60",     "Track the 144 MB CSV outside git"],
            ["ML / Scoring",    "scikit-learn",         "1.5",      "IsolationForest, StandardScaler, MinMaxScaler"],
            ["Data wrangling",  "pandas",               "2.2",      "DataFrame operations throughout"],
            ["Orchestration",   "Apache Airflow",       "2.10.4",   "Hourly ETL DAG (Docker)"],
            ["Dashboard",       "Grafana OSS",          "11.4",     "Live fraud monitoring (Docker)"],
            ["DB access",       "SQLAlchemy + psycopg2","2.0",      "ORM-free SQL with connection pooling"],
            ["Model storage",   "joblib",               "1.4",      "Serialise/deserialise sklearn models"],
            ["Config",          "python-dotenv",        "1.2",      "Load .env credentials"],
            ["Containers",      "Docker / Compose",     "28.4",     "Run Airflow and Grafana on Windows"],
        ]
    )

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 4 — DVC
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "4. Stage 0 — Data Versioning with DVC")

    h2(doc, "4.1 The Problem with Large Files in Git")
    body(doc,
         "The raw creditcard.csv file is 144 MB. Git is designed for code, not data — pushing a "
         "144 MB binary file to GitHub would exceed GitHub's 100 MB file size limit and bloat the "
         "repository history permanently. Even if the file were split or compressed, every developer "
         "cloning the repository would download the full dataset regardless of whether they needed it.")
    body(doc,
         "DVC (Data Version Control) solves this by replacing the actual file with a tiny pointer "
         "file (.dvc) that is tracked by git. The real data is stored in a separate location — "
         "in this project, a local directory at C:/Users/Admin/dvc-storage.")

    h2(doc, "4.2 Removing the File from Git History")
    body(doc,
         "The CSV had already been committed to git before DVC was introduced. Git stores objects "
         "immutably in its object store, so simply deleting the file from the working tree does not "
         "remove it from history — the large blob persists in every previous commit. The git-filter-repo "
         "tool was used to rewrite every commit in the repository's history, surgically removing the "
         "CSV blob from all of them:")

    code_block(doc, """\
# Install git-filter-repo
pip install git-filter-repo

# Remove the file from ALL commits (rewrites history)
git filter-repo --path data/creditcard.csv --invert-paths --force

# git filter-repo removes the origin remote as a safety measure — re-add it
git remote add origin https://github.com/Desmondonam/real-time-Fraud-detection.git

# Force-push the rewritten history (safe here because this is a solo project
# and no other branches reference the old commits)
git push --force origin main""")

    body(doc,
         "After this operation the repository's pack file shrank from 47 MB to 25 MB. "
         "The CSV is now managed exclusively by DVC.")

    h2(doc, "4.3 Initialising DVC and Tracking the File")
    code_block(doc, """\
# Initialise DVC (creates .dvc/ directory)
dvc init

# Configure a local storage remote
dvc remote add -d local_storage C:/Users/Admin/dvc-storage

# Add the CSV to DVC tracking
# This creates data/creditcard.csv.dvc (the pointer file)
# and adds data/creditcard.csv to data/.gitignore
dvc add data/creditcard.csv

# Push the actual data to the local remote
dvc push

# Commit the pointer file and DVC config to git
git add data/creditcard.csv.dvc data/.gitignore .dvc/config
git commit -m "Track creditcard.csv with DVC"
git push origin main""")

    body(doc,
         "From this point forward, git tracks only the 1 KB pointer file. "
         "A developer cloning the repository and running dvc pull will download "
         "the CSV from the configured remote storage.")

    callout(doc,
            "data/creditcard.csv.dvc contains an md5 hash of the file. DVC uses this "
            "hash to fetch the correct version of the data from the remote cache, "
            "giving full reproducibility — if the source data changes, the hash changes "
            "and dvc pull will download the updated version.",
            color=ACCENT_BLUE, label="How it works")

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 5 — INGEST & CLEAN
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "5. Stage 1 — Data Ingestion and Cleaning")
    body(doc,
         "With the data version-controlled, the next step is loading it into a form that the "
         "detection pipeline can query efficiently. Raw CSVs are not queryable; they cannot be "
         "indexed, joined, or aggregated without loading the entire file into memory. "
         "PostgreSQL gives us indexed random access, typed columns, and SQL — "
         "all essential for the batch ETL in Stage 3.")

    h2(doc, "5.1 Schema Validation")
    body(doc,
         "Before touching the data, the script validates that the CSV matches the expected schema. "
         "This protects against silent data drift — if the upstream dataset ever adds or removes "
         "columns, the pipeline fails loudly rather than producing corrupt results.")

    code_block(doc, """\
EXPECTED_COLUMNS = (
    ["Time"]
    + [f"V{i}" for i in range(1, 29)]   # V1 through V28
    + ["Amount", "Class"]
)

def validate_schema(df: pd.DataFrame) -> None:
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    extra   = [c for c in df.columns if c not in EXPECTED_COLUMNS]

    if missing:
        raise ValueError(f"Missing columns: {missing}")
    if extra:
        log.warning("Unexpected columns (will be dropped): %s", extra)
        df.drop(columns=extra, inplace=True)

    # Verify every column can be cast to its expected type
    wrong_types = []
    for col, expected in EXPECTED_DTYPES.items():
        try:
            df[col].astype(expected)
        except (ValueError, TypeError):
            wrong_types.append(col)

    if wrong_types:
        raise ValueError(f"Type-incompatible columns: {wrong_types}")

    log.info("Schema validation passed — %d columns, %d rows",
             len(df.columns), len(df))""")

    h2(doc, "5.2 Null Handling")
    body(doc,
         "The creditcard dataset has no null values in its published form, but the null-handling "
         "logic is written defensively — the pipeline should be robust to real-world data quality "
         "issues if the dataset is extended or replaced. The strategy per column type is:")

    info_table(doc,
        ["Column Group", "Strategy", "Rationale"],
        [
            ["V1 – V28 (PCA features)", "Fill with column median", "Median is robust to skew; mean would be biased by fraud outliers"],
            ["Amount",                  "Fill with column median", "Same reasoning — Amount is right-skewed"],
            ["Class (label)",           "Drop the row",           "A transaction with an unknown label cannot be trained on or evaluated against"],
        ]
    )

    h2(doc, "5.3 Amount Normalisation")
    body(doc,
         "The raw Amount column ranges from €0 to €25,691 — a spread of over four orders of magnitude. "
         "The V1–V28 PCA features are already zero-centred with unit variance (a consequence of the "
         "PCA transformation). If Amount is left in its raw form, its scale dominates the Isolation "
         "Forest's distance calculations, effectively making Amount the only feature that matters.")
    body(doc,
         "StandardScaler (Z-score normalisation) transforms Amount to mean = 0, std = 1, "
         "making it directly comparable with the V features:")

    code_block(doc, """\
from sklearn.preprocessing import StandardScaler

def normalize_amount(df: pd.DataFrame) -> pd.DataFrame:
    scaler = StandardScaler()
    df["Amount"] = scaler.fit_transform(df[["Amount"]])
    log.info(
        "Amount normalized — mean=%.4f, std=%.4f (should be ~0 and ~1)",
        df["Amount"].mean(),
        df["Amount"].std(),
    )
    return df""")

    body(doc,
         "After normalisation, a Z-score of 2.0 means the transaction amount is two standard "
         "deviations above the mean — directly interpretable as 'unusually high spend', which "
         "the rule engine exploits in Section 6.")

    h2(doc, "5.4 Bulk Load into PostgreSQL")
    body(doc,
         "A naive row-by-row INSERT for 284,807 rows would take minutes. "
         "SQLAlchemy's to_sql with method='multi' and chunksize=5,000 performs bulk multi-row "
         "INSERTs that complete in approximately 6 minutes on a local machine:")

    code_block(doc, """\
df.to_sql(
    "transactions",
    engine,
    if_exists="replace",    # recreates the table on each run
    index=False,
    chunksize=5_000,        # 5,000 rows per INSERT statement
    method="multi",         # VALUES (row1), (row2), ... syntax
)""")

    body(doc, "Verification query after load:")
    code_block(doc, """\
SELECT COUNT(*) FROM transactions;
-- 284807

SELECT COUNT(*) FROM transactions WHERE "Class" = 1;
-- 492  (0.173% fraud rate — matches the published dataset)""")

    callout(doc,
            "Loaded 284,807 rows | 492 confirmed fraud cases (0.173%) | "
            "Amount: mean=0.0000, std=1.0000",
            color=GREEN, label="Output")

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 6 — DETECTION LOGIC
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "6. Stage 2 — Detection Logic")
    body(doc,
         "The detection logic is the intellectual core of the system. Rather than relying on a single "
         "model, the architecture uses two complementary methods — a rule engine and an Isolation Forest "
         "— and combines their outputs into a single risk score. This hybrid design gives the system "
         "both interpretability (rules) and generalization (ML) simultaneously.")

    h2(doc, "6.1 Why Isolation Forest for Fraud Detection?")
    body(doc,
         "Isolation Forest is a tree-based anomaly detection algorithm that works by randomly "
         "partitioning the feature space. The core insight is elegant: anomalies are 'few and different' "
         "— they require fewer random cuts to isolate than normal points. The algorithm builds an ensemble "
         "of isolation trees, and the anomaly score is the average path length across all trees. "
         "Short path length = easy to isolate = anomalous.")
    body(doc,
         "For fraud detection specifically, Isolation Forest has three important properties:")
    bullet(doc, "Unsupervised — it requires no labelled fraud examples to train, making it effective even when historical labels are unavailable or unreliable.")
    bullet(doc, "Contamination-aware — the contamination parameter tells the algorithm what fraction of the training data to expect as anomalies, guiding the decision threshold.")
    bullet(doc, "Scalable — the algorithm is O(n log n) in training and O(n) in inference, easily handling datasets with hundreds of thousands of rows.")

    h2(doc, "6.2 The Rule Engine")
    body(doc,
         "The rule engine is a stateless scorer that applies domain-knowledge thresholds to each "
         "transaction and returns a normalised score in [0, 1]. It serves three purposes:")
    bullet(doc, "Speed — it runs without loading any model from disk, making it useful as a first-pass fast filter.")
    bullet(doc, "Interpretability — every rule can be explained to a non-technical stakeholder.")
    bullet(doc, "Safety net — obvious violations (e.g. V14 < −5) are always captured in the final score even if the Isolation Forest disagrees.")

    body(doc, "Signal families and their point contributions:")
    info_table(doc,
        ["Signal", "Condition", "Points", "Rationale"],
        [
            ["Amount spike",  "|Amount| > 2σ",  "+1.0",  "Statistically unusual spend (2 std devs above mean)"],
            ["Extreme amount","|Amount| > 3σ",  "+0.5",  "Very unusual — top 0.13% of amounts"],
            ["V14 anomaly",   "V14 < −5",       "+1.5",  "Strongest single fraud predictor in this dataset"],
            ["V17 anomaly",   "V17 < −5",       "+1.0",  "Second strongest individual predictor"],
            ["V12 anomaly",   "V12 < −4",       "+0.75", "Strong negative correlation with fraud"],
            ["V10 anomaly",   "V10 < −4",       "+0.5",  "Moderate fraud signal"],
            ["V3 anomaly",    "V3 < −3",         "+0.5",  "Moderate fraud signal"],
            ["V4 anomaly",    "V4 > 4",          "+0.25", "Weak positive fraud signal"],
        ]
    )

    body(doc, "After summing points, a time-of-day multiplier is applied:")
    info_table(doc,
        ["Hour window", "Multiplier", "Rationale"],
        [
            ["00:00 – 05:59  (night)",    "× 1.5",  "Lower human oversight; unusual for most legitimate merchants"],
            ["09:00 – 17:59  (business)", "× 0.75", "Normal business hours; high-volume legitimate activity"],
            ["All other hours",           "× 1.0",  "Neutral"],
        ]
    )

    body(doc,
         "The total is divided by the theoretical maximum of 9.0 to normalise the output to [0, 1]. "
         "The time_hour is derived from the dataset's Time column by assuming t = 0 corresponds to midnight:")

    code_block(doc, """\
time_hour = int((transaction["Time"] % 86_400) / 3_600)  # 0 – 23""")

    h2(doc, "6.3 Training the Isolation Forest")
    body(doc,
         "The Isolation Forest is trained on ALL 284,807 transactions without using the Class label — "
         "making the training fully unsupervised. The Class label is used only during evaluation to "
         "measure detection quality after training completes.")
    body(doc, "Feature set: 30 features — V1 through V28, Amount (normalised), and time_hour.")
    body(doc, "Key hyperparameter choices:")

    info_table(doc,
        ["Parameter", "Value", "Reason"],
        [
            ["n_estimators",  "200",      "More trees produce more stable anomaly scores; diminishing returns beyond 200"],
            ["contamination", "0.00173",  "Set to the known fraud rate in the dataset (492 / 284,807 = 0.00173)"],
            ["max_samples",   "'auto'",   "Uses min(256, n_samples) sub-samples per tree — fast and sufficient"],
            ["random_state",  "42",       "Reproducibility"],
            ["n_jobs",        "-1",       "Use all CPU cores during training"],
        ]
    )

    code_block(doc, """\
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler

model = IsolationForest(
    n_estimators=200,
    contamination=0.00173,
    max_samples="auto",
    random_state=42,
    n_jobs=-1,
)
model.fit(X_train)   # X_train has no Class column

# decision_function returns positive for normal, negative for anomalous
# Invert so higher = more anomalous, then scale to [0, 1]
raw_scores = -model.decision_function(X_train)
scaler = MinMaxScaler()
scaler.fit(raw_scores.reshape(-1, 1))

# Save model + scaler together for inference
import joblib
joblib.dump({"model": model, "scaler": scaler}, "models/isolation_forest.pkl")""")

    h2(doc, "6.4 The Combined Scorer")
    body(doc,
         "The CombinedScorer merges both signals into a single score using a weighted average. "
         "The Isolation Forest receives 60% of the weight because it captures multivariate anomaly "
         "patterns across all 30 features simultaneously — patterns that no individual threshold "
         "rule can express. The rule engine receives 40% to ensure that clear-cut violations are "
         "always reflected in the final score:")

    code_block(doc, """\
combined_score = 0.4 × rule_score + 0.6 × if_score

# A transaction is flagged HIGH-RISK when:
is_high_risk = combined_score >= 0.40""")

    body(doc,
         "The 0.40 threshold was chosen by inspecting the score distributions for both classes. "
         "Legitimate transactions have a mean combined score of 0.083 (std 0.065), while fraudulent "
         "transactions have a mean of 0.467 (std 0.216). A threshold of 0.40 sits just below the "
         "fraud mean, capturing the majority of fraud cases while maintaining an acceptable false "
         "positive rate.")

    h2(doc, "6.5 Model Evaluation")
    body(doc,
         "Because the model is unsupervised, evaluation is performed post-hoc: scores are generated "
         "for all 284,807 transactions and then compared against the held-out Class column.")

    code_block(doc, """\
python src/train_detector.py

# Output:
============================================================
  DETECTION EVALUATION (threshold = 0.40)
============================================================
  ROC-AUC              : 0.9511
  Confusion matrix
    True Negatives     :  282,998   (legit correctly ignored)
    False Positives    :    1,317   (legit flagged — 0.46%)
    False Negatives    :      182   (fraud missed)
    True Positives     :      310   (fraud caught — 63.0%)

  Best F1 threshold    : 0.5503 → precision=0.5592
                                   recall=0.4126
                                   F1=0.4749
============================================================""")

    body(doc,
         "A ROC-AUC of 0.9511 for a fully unsupervised model on a 0.17%-fraud dataset is strong. "
         "The relatively modest precision (19%) at the 0.40 threshold is expected — the model "
         "is casting a wide net to maximise recall (63%). In a production system, high-risk flags "
         "would feed into a downstream analyst review queue or a secondary supervised model rather "
         "than being acted on directly.")

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 7 — AIRFLOW ETL
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "7. Stage 3 — ETL Pipeline with Apache Airflow")
    body(doc,
         "The Airflow DAG turns the one-off scoring logic into a continuously running pipeline "
         "that simulates a streaming workload. Rather than scoring all transactions at once, "
         "the DAG processes the 48-hour dataset in one-hour windows on an hourly schedule — "
         "exactly how a production pipeline would process a live transaction stream.")

    h2(doc, "7.1 Streaming Simulation Design")
    body(doc,
         "The dataset's Time column records seconds since the first transaction and spans "
         "approximately 48 hours (0 – 172,792 seconds). To simulate streaming, each hourly "
         "Airflow run is mapped to a 3,600-second dataset window:")

    code_block(doc, """\
DATASET_DURATION_SECS = 172_800   # 48 hours
BATCH_SIZE_SECS       = 3_600     # 1 hour per batch
DAG_START_DATE        = datetime(2024, 1, 1, tzinfo=timezone.utc)

# Inside the DAG task:
hours_since_start = int(
    (run_dt - DAG_START_DATE).total_seconds() // BATCH_SIZE_SECS
)
# Wrap around so the DAG can run indefinitely (48 windows cycle)
window_idx  = hours_since_start % (DATASET_DURATION_SECS // BATCH_SIZE_SECS)
batch_start = float(window_idx * BATCH_SIZE_SECS)
batch_end   = batch_start + BATCH_SIZE_SECS""")

    body(doc,
         "This mapping means Airflow run hour 0 processes dataset seconds [0, 3600), "
         "run hour 1 processes [3600, 7200), and so on. After hour 47 the window index "
         "wraps back to 0, allowing the DAG to cycle through the dataset indefinitely.")

    h2(doc, "7.2 Task Graph")
    body(doc, "The DAG contains four tasks wired in sequence:")
    code_block(doc, """\
ensure_fraud_alerts_table          # idempotent CREATE TABLE IF NOT EXISTS
         │
         ▼
fetch_and_score_batch              # load window → CombinedScorer → return high-risk rows
         │
         ▼
write_alerts                       # INSERT high-risk rows into fraud_alerts
         │
         ▼
log_summary                        # emit structured batch statistics""")

    h2(doc, "7.3 The fetch_and_score_batch Task")
    body(doc,
         "This is the most complex task. It selects all 30 feature columns for the current "
         "time window, loads the trained CombinedScorer, scores every transaction, and returns "
         "a serialisable dict (via Airflow XCom) containing only the high-risk rows:")

    code_block(doc, """\
@task()
def fetch_and_score_batch(logical_date: str) -> dict:
    from src.detection.scorer import CombinedScorer

    # ... (window calculation as above) ...

    feature_cols = ", ".join(f'"{c}"' for c in ALL_FEATURES)
    query = text(f\"""
        SELECT "Time", {feature_cols}, "Amount", "Class"
        FROM   transactions
        WHERE  "Time" >= :start AND "Time" < :end
    \""")
    df = pd.read_sql(query, engine, params={"start": batch_start, "end": batch_end})

    scorer  = CombinedScorer.load(model_path=MODEL_PATH)
    df      = scorer.score(df)   # adds rule_score, if_score, combined_score, is_high_risk

    # Flag high-risk AND any confirmed fraud (Class == 1) regardless of model score
    high_risk = df[df["is_high_risk"] | (df["Class"] == 1)].copy()

    return {
        "batch_start": batch_start,
        "batch_end":   batch_end,
        "total":       len(df),
        "flagged":     len(high_risk),
        "alerts":      high_risk.to_dict(orient="records"),
    }""")

    h2(doc, "7.4 Running Airflow on Windows — The Docker Solution")
    body(doc,
         "Airflow's scheduler imports python-daemon, which in turn imports the pwd module — "
         "a Unix-only standard library module that does not exist on Windows. Attempting to run "
         "airflow scheduler natively on Windows produces:")

    code_block(doc, "ModuleNotFoundError: No module named 'pwd'")

    body(doc,
         "The correct solution is to run Airflow inside a Docker container (Linux). "
         "The official apache/airflow:2.10.4-python3.12 image is used with the standalone command, "
         "which runs both the scheduler and webserver in a single process — ideal for development. "
         "Key configuration decisions in the docker-compose.yml:")

    bullet(doc, "extra_hosts: host.docker.internal:host-gateway — allows the container to reach the host's PostgreSQL on localhost:5432.")
    bullet(doc, "_PIP_ADDITIONAL_REQUIREMENTS — installs scikit-learn, joblib, psycopg2-binary, and python-dotenv at container startup.")
    bullet(doc, "Volume mounts — ../src is mounted at /opt/src and ../models at /opt/models.")

    body(doc,
         "The mount paths require careful alignment with the DAG's path resolution logic. "
         "The DAG computes the project root as two directory levels above the DAG file:")

    code_block(doc, """\
# DAG file location inside Docker:
#   /opt/airflow/dags/fraud_etl_dag.py

_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
# Result: /opt/airflow/dags/../../  =  /opt

# Therefore src must be mounted at /opt/src  (NOT /opt/airflow/src)
# and models must be at /opt/models          (NOT /opt/airflow/models)""")

    callout(doc,
            "This path alignment bug was discovered during deployment and fixed by updating "
            "the docker-compose.yml volume mount targets. It is a common pitfall when running "
            "DAGs inside Docker that use relative path resolution.",
            color=RGBColor(0xE6, 0x7E, 0x22), label="Lesson learned")

    h2(doc, "7.5 DAG Configuration")
    code_block(doc, """\
@dag(
    dag_id="fraud_etl_pipeline",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,          # do not backfill all missed runs since 2024-01-01
    max_active_runs=1,      # prevent concurrent runs overlapping
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fraud", "etl"],
)""")

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 8 — GRAFANA DASHBOARD
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "8. Stage 4 — Live Dashboard with Grafana")
    body(doc,
         "Grafana is an open-source observability platform that connects directly to PostgreSQL "
         "and renders live charts, tables, and KPI stats. It is the ideal tool for this use case "
         "because the fraud_alerts table uses real timestamps (flagged_at) that Grafana's "
         "time series panels understand natively.")

    h2(doc, "8.1 PostgreSQL Views for the Dashboard")
    body(doc,
         "Rather than embedding complex SQL inside Grafana panel definitions (which are hard to "
         "version-control and debug), two database views are created that the dashboard queries:")

    code_block(doc, """\
-- View 1: Enriches every alert with a human-readable merchant category
-- (derived from Amount Z-score since the dataset has no real merchant data)
CREATE OR REPLACE VIEW fraud_alerts_enriched AS
SELECT
  fa.*,
  CASE
    WHEN fa."Amount" < -1.0 THEN 'Grocery & Food'
    WHEN fa."Amount" < 0.0  THEN 'Retail & Shopping'
    WHEN fa."Amount" < 0.5  THEN 'Entertainment'
    WHEN fa."Amount" < 1.5  THEN 'Travel & Transport'
    WHEN fa."Amount" < 2.5  THEN 'Electronics'
    ELSE                         'Luxury & High-Value'
  END AS merchant_category,
  FLOOR(fa.batch_start_sec / 3600)::int AS dataset_hour
FROM fraud_alerts fa;

-- View 2: Per-hour fraud rate across the full transactions table
CREATE OR REPLACE VIEW hourly_transaction_summary AS
SELECT
  FLOOR(t."Time" / 3600)::int                             AS hour_bucket,
  COUNT(*)                                                 AS total_transactions,
  SUM(CASE WHEN t."Class" = 1 THEN 1 ELSE 0 END)          AS actual_fraud,
  ROUND(100.0 * SUM(CASE WHEN t."Class" = 1 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 4)                          AS fraud_rate_pct
FROM transactions t
GROUP BY 1
ORDER BY 1;""")

    h2(doc, "8.2 Provisioning — Zero-Click Configuration")
    body(doc,
         "Grafana supports provisioning: YAML files that define datasources and dashboards "
         "are mounted into the container and loaded automatically on startup. This means "
         "the dashboard appears immediately after docker compose up — no manual clicking "
         "in the UI required. The provisioning files live under grafana/provisioning/.")

    body(doc, "Datasource provisioning (grafana/provisioning/datasources/fraud_db.yaml):")
    code_block(doc, """\
apiVersion: 1
datasources:
  - name: FraudDB
    type: postgres
    uid: fraud_postgres
    url: host.docker.internal:5432   # reaches host PostgreSQL from Docker
    database: fraud_detection
    user: postgres
    secureJsonData:
      password: <your_password>
    jsonData:
      sslmode: disable
      postgresVersion: 1700""")

    h2(doc, "8.3 Dashboard Panels")
    info_table(doc,
        ["Panel", "Type", "Key Query / Data Source"],
        [
            ["Total Alerts",               "Stat",        "SELECT COUNT(*) FROM fraud_alerts"],
            ["Confirmed Fraud Cases",      "Stat",        "WHERE is_confirmed_fraud = true"],
            ["Fraud Confirmation Rate",    "Stat",        "% of alerts that are confirmed fraud"],
            ["Avg Combined Score",         "Stat",        "AVG(combined_score)"],
            ["Dataset Fraud Rate",         "Stat",        "AVG(fraud_rate_pct) from hourly_transaction_summary"],
            ["Hourly Alert Volume",        "Time series", "Group by flagged_at (real timestamp), auto-refreshes 30s"],
            ["Alert Volume by Hour",       "Time series", "fraud_alerts_enriched.dataset_hour (0–47)"],
            ["Top Flagged Categories",     "Pie chart",   "COUNT(*) GROUP BY merchant_category"],
            ["Recent High-Risk Transactions","Table",     "Sortable; combined_score colour-coded red/orange/green"],
            ["Live Fraud Rate by Hour",    "Dual-axis",   "total_transactions (left) + fraud_rate_pct (right)"],
        ]
    )

    h2(doc, "8.4 Populating Data for the Dashboard")
    body(doc,
         "The dashboard is only useful with data. The populate_alerts.py script backfills "
         "all 48 one-hour windows so the dashboard has a full picture immediately after deployment. "
         "It also staggers the flagged_at timestamps so Grafana's time axis spans a 48-hour window:")

    code_block(doc, """\
# Map each transaction's dataset Time to a real wall-clock timestamp
# starting from midnight today — makes the time series meaningful in Grafana
base_ts = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0)
high_risk["flagged_at"] = base_ts + pd.to_timedelta(high_risk["Time"], unit="s")

python scripts/populate_alerts.py
# Output:
# Window 00/48 → 18 alerts
# Window 01/48 → 9 alerts
# ...
# Done — 1809 total alerts across 48 windows | 492 confirmed fraud""")

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 9 — DEPLOYMENT
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "9. Deployment — Step by Step")
    body(doc,
         "This section documents the exact sequence of commands needed to deploy the full "
         "system on a fresh Windows 11 machine with PostgreSQL 17 and Docker Desktop installed.")

    h2(doc, "Step 1 — Clone and Configure")
    code_block(doc, """\
git clone https://github.com/Desmondonam/real-time-Fraud-detection.git
cd real-time-Fraud-detection

# Create .env with database credentials
# (copy the template or create manually)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fraud_detection
DB_USER=postgres
DB_PASSWORD=your_postgres_password""")

    h2(doc, "Step 2 — Install Python Dependencies")
    code_block(doc, """\
pip install pandas sqlalchemy psycopg2-binary scikit-learn \\
            joblib python-dotenv dvc""")

    h2(doc, "Step 3 — Create PostgreSQL Databases")
    code_block(doc, """\
psql -U postgres -c "CREATE DATABASE fraud_detection;"
psql -U postgres -c "CREATE DATABASE airflow_metadata;"
psql -U postgres -d fraud_detection -f scripts/create_views.sql""")

    h2(doc, "Step 4 — Pull Data with DVC")
    code_block(doc, """\
# Configure the DVC remote to point to your local storage location
dvc remote modify local_storage url /path/to/dvc-storage

# Download the CSV
dvc pull   # → restores data/creditcard.csv (144 MB)""")

    h2(doc, "Step 5 — Ingest and Clean Data")
    code_block(doc, """\
python src/ingest_and_clean.py
# Expected: 284,807 rows loaded | Amount normalised | fraud rate 0.173%""")

    h2(doc, "Step 6 — Train the Detection Model")
    code_block(doc, """\
python src/train_detector.py
# Expected: ROC-AUC 0.9511 | model saved to models/isolation_forest.pkl""")

    h2(doc, "Step 7 — Populate Fraud Alerts")
    code_block(doc, """\
python scripts/populate_alerts.py
# Expected: 1,809 alerts across 48 windows""")

    h2(doc, "Step 8 — Start Grafana")
    code_block(doc, """\
# Copy the datasource template and fill in your password
cp grafana/provisioning/datasources/fraud_db.yaml.example \\
   grafana/provisioning/datasources/fraud_db.yaml

cd grafana
docker compose up -d

# Verify
curl http://localhost:3000/api/health
# → {"database":"ok","version":"11.4.0"}

# Open in browser
# http://localhost:3000  (admin / admin)""")

    h2(doc, "Step 9 — Start Airflow")
    code_block(doc, """\
cd airflow
docker compose up -d

# Wait ~90 seconds for pip package installation, then verify
curl http://localhost:8080/health
# → {"scheduler":{"status":"healthy"},"metadatabase":{"status":"healthy"}}

# Open in browser
# http://localhost:8080  (admin / admin)""")

    h2(doc, "Step 10 — Enable and Trigger the DAG")
    code_block(doc, """\
# Inside the Airflow container:
docker exec fraud_airflow airflow dags unpause fraud_etl_pipeline
docker exec fraud_airflow airflow dags trigger fraud_etl_pipeline

# Monitor run status
docker exec fraud_airflow airflow dags list-runs -d fraud_etl_pipeline""")

    callout(doc,
            "After triggering, the DAG run transitions: queued → running → success. "
            "Each task (ensure_fraud_alerts_table → fetch_and_score_batch → write_alerts "
            "→ log_summary) takes approximately 30 seconds total for a 3,600-second window.",
            color=GREEN, label="Expected result")

    h2(doc, "Step 11 — Verify End-to-End")
    code_block(doc, """\
psql -U postgres -d fraud_detection -c "
SELECT
  COUNT(*)                                                  AS total_alerts,
  SUM(CASE WHEN is_confirmed_fraud THEN 1 END)             AS confirmed_fraud,
  ROUND(AVG(combined_score)::numeric, 4)                   AS avg_score,
  COUNT(DISTINCT batch_start_sec)                          AS batches_processed
FROM fraud_alerts;
"

--  total_alerts | confirmed_fraud | avg_score | batches_processed
-- --------------+-----------------+-----------+------------------
--         1907  |             520 |    0.4691 |               48""")

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 10 — RESULTS
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "10. Results and Key Metrics")

    h2(doc, "10.1 Detection Performance")
    info_table(doc,
        ["Metric", "Value", "Interpretation"],
        [
            ["ROC-AUC",                  "0.9511",   "Excellent for unsupervised detection on imbalanced data"],
            ["Recall at threshold 0.40", "63.0%",    "310 of 492 fraud cases caught"],
            ["Precision at threshold 0.40","19.1%",  "1,317 false positives — feeds analyst review queue"],
            ["Best F1 threshold",        "0.55",     "Precision 55.9% | Recall 41.3% | F1 0.475"],
            ["Fraud mean score",         "0.467",    "Clearly separated from legit mean of 0.083"],
            ["Legit mean score",         "0.083",    "Std dev 0.065 — tight cluster away from fraud"],
        ]
    )

    h2(doc, "10.2 Pipeline Performance")
    info_table(doc,
        ["Stage", "Timing", "Data Volume"],
        [
            ["CSV ingest (284,807 rows)",    "~6 minutes",   "144 MB → PostgreSQL"],
            ["IF training (284,807 × 30)",   "~7 seconds",   "200 trees, n_jobs=-1"],
            ["Batch scoring (per 3,600s window)", "< 1 second", "avg ~2,000 tx per window"],
            ["Full 48-window backfill",      "~15 seconds",  "1,809 alerts written"],
            ["Grafana dashboard refresh",    "30 seconds",   "10 panels, live queries"],
        ]
    )

    h2(doc, "10.3 System State After Full Deployment")
    info_table(doc,
        ["Component", "Status", "Access"],
        [
            ["PostgreSQL 17",           "Running (Windows service)", "localhost:5432"],
            ["transactions table",      "284,807 rows",             "fraud_detection DB"],
            ["fraud_alerts table",      "1,907 rows, 48 batches",   "fraud_detection DB"],
            ["Isolation Forest model",  "Trained (1.7 MB)",         "models/isolation_forest.pkl"],
            ["Airflow",                 "Healthy, DAG unpaused",    "http://localhost:8080"],
            ["Grafana",                 "Dashboard live",           "http://localhost:3000"],
        ]
    )

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 11 — LESSONS LEARNED
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "11. Lessons Learned and Design Decisions")

    h2(doc, "11.1 Hybrid Detection Outperforms Either Method Alone")
    body(doc,
         "The rule engine alone would miss fraud that does not trigger any individual threshold. "
         "The Isolation Forest alone is a black box — useful for catching anomalies but impossible "
         "to explain to a compliance officer. The combined scorer gives you both: ML-quality "
         "detection with a rule-based explanation for every flagged transaction. This is the "
         "right architecture for regulated industries where explainability is mandatory.")

    h2(doc, "11.2 Windows is a Second-Class Citizen for Data Engineering Tools")
    body(doc,
         "Three significant Windows-specific issues were encountered and resolved during this project:")
    bullet(doc, "Airflow's scheduler cannot run natively on Windows (python-daemon / pwd). Solution: Docker.")
    bullet(doc, "The Write tool on Windows creates UTF-16 LE files by default, which git treats as binary. Solution: explicit UTF-8 re-encoding before committing.")
    bullet(doc, "Volume mount paths in Docker on Windows use forward slashes but Windows paths use backslashes — Docker Desktop handles the translation, but Compose files must use forward slashes.")

    h2(doc, "11.3 Path Resolution in Containerised Airflow")
    body(doc,
         "The DAG's project root calculation (two levels up from the DAG file) works correctly "
         "on the host but resolves to /opt inside Docker instead of /opt/airflow. "
         "The fix — mounting src and models at the correct container paths — took one debugging "
         "cycle to identify. For future projects, use an explicit AIRFLOW_PROJECT_ROOT "
         "environment variable rather than relative path resolution.")

    h2(doc, "11.4 DVC History Rewriting Must Be Done Early")
    body(doc,
         "Once a large file is committed to git, it must be purged from ALL historical commits — "
         "not just the latest one. git-filter-repo rewrites the entire commit graph, which is "
         "a destructive operation. On a solo project this is safe; on a team project, all "
         "collaborators must re-clone after the force-push. Always set up DVC before committing "
         "large data files.")

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 12 — NEXT STEPS
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "12. Next Steps and Production Hardening")

    info_table(doc,
        ["Enhancement", "Description", "Impact"],
        [
            ["Supervised model",         "Train a LightGBM or XGBoost classifier on labelled data to replace/augment IF", "Precision improves from 19% to 60%+"],
            ["Streaming with Kafka",     "Replace batch windowing with real-time Kafka consumer", "True sub-second fraud detection"],
            ["Feature engineering",     "Add velocity features (tx/hour per card), merchant category from real data", "Stronger signals for the rule engine"],
            ["Model retraining DAG",     "Schedule weekly IF retraining as a separate Airflow DAG", "Adapts to evolving fraud patterns"],
            ["Alert queue",             "Write high-risk alerts to a Slack/email channel or case management system", "Closes the analyst review loop"],
            ["CeleryExecutor",           "Replace SequentialExecutor with Celery + Redis for parallel task execution", "Production-grade throughput"],
            ["Cloud deployment",         "Migrate PostgreSQL to AWS RDS, Airflow to MWAA, Grafana to Grafana Cloud", "Eliminates local infrastructure dependency"],
            ["Data quality monitoring", "Add Great Expectations checks on ingest to catch schema drift automatically", "Prevents silent data quality failures"],
        ]
    )

    add_horizontal_rule(doc)

    # ══════════════════════════════════════════════════════════════════════════
    #  SECTION 13 — CONCLUSION
    # ══════════════════════════════════════════════════════════════════════════
    h1(doc, "13. Conclusion")
    body(doc,
         "This project demonstrates that a production-quality fraud detection pipeline can be "
         "built entirely with open-source tools on a local machine. The four-stage architecture — "
         "data versioning → ingest and clean → detect → orchestrate and visualise — reflects "
         "the structure of real enterprise fraud systems, just without the cloud infrastructure costs.")
    body(doc,
         "The most important architectural decision was the hybrid detection approach: combining a "
         "transparent rule engine with an unsupervised Isolation Forest. This gives the system "
         "both the detection power of machine learning and the interpretability required for "
         "financial compliance. With a ROC-AUC of 0.9511 achieved without a single labelled "
         "training example, the Isolation Forest proves that unsupervised anomaly detection "
         "is a powerful first line of defence against fraud.")
    body(doc,
         "The Airflow DAG ensures the pipeline runs continuously without manual intervention, "
         "the Grafana dashboard gives stakeholders instant visibility into alert volumes and "
         "fraud rates, and DVC ensures the exact dataset version used for training can always "
         "be reproduced — even years later.")
    body(doc,
         "The complete source code is available at: "
         "https://github.com/Desmondonam/real-time-Fraud-detection")

    add_horizontal_rule(doc)

    # ── footer note ───────────────────────────────────────────────────────────
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run("Built with Python 3.12 · PostgreSQL 17 · DVC 3.60 · scikit-learn 1.5 · "
                  "Apache Airflow 2.10.4 · Grafana OSS 11.4 · Docker 28.4")
    r.font.size = Pt(9)
    r.font.color.rgb = RGBColor(0xAA, 0xAA, 0xAA)
    r.italic = True

    doc.save(OUTPUT)
    print(f"Document saved: {OUTPUT}")


if __name__ == "__main__":
    build()
