# -*- coding: utf-8 -*-
"""
Main script to download, process, and analyze test questions from laacr.cz.
This script uses a local DuckDB database for efficient data storage and retrieval.
It is designed for periodic, automated execution and generates a full suite of
analytical outputs, creating a separate report for each test category.
"""

import os
import io
import json
import re
import subprocess
import warnings
import hashlib
from datetime import date, datetime, timedelta

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pdfplumber
import polars as pl
import requests

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================
DB_FILE = "tests.duckdb"
URL_BASE = "https://zkouseni.laacr.cz/Zkouseni/PDFReport?module=M09&report=vysledek&id="
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

AGGRESSIVE_PATIENCE = 100
NORMAL_PATIENCE = 30

warnings.simplefilter(action='ignore', category=FutureWarning)


# ==============================================================================
# PART 1: DATA SCRAPING & DATABASE OPERATIONS (No changes here)
# ==============================================================================
def initialize_database(conn):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS questions
                 (
                     test_id
                     UINTEGER,
                     test_date
                     DATE,
                     question_text
                     VARCHAR,
                     option_a
                     VARCHAR,
                     option_b
                     VARCHAR,
                     option_c
                     VARCHAR,
                     correct_option
                     VARCHAR
                 (
                     1
                 ),
                     points UTINYINT, test_type VARCHAR, is_practice_test BOOLEAN, official_test_number VARCHAR,
                     category VARCHAR
                     );
                 """)
    print("Database is ready.")


def get_latest_test_id_from_db(conn):
    result = conn.execute("SELECT MAX(test_id) FROM questions;").fetchone()
#    return result[0] if result and result[0] is not None else 0
    return 950000-2000



def parse_pdf_from_url(url: str):
    try:
        response = requests.get(url, timeout=30, headers=REQUEST_HEADERS)
        response.raise_for_status()
        with io.BytesIO(response.content) as pdf_stream, pdfplumber.open(pdf_stream) as pdf:
            full_text = "".join(page.extract_text() + "\n" for page in pdf.pages)
        test_type_match = re.search(r"Přezkušovací test\s+(.*)", full_text)
        test_type = test_type_match.group(1).strip() if test_type_match else "Unknown"
        is_practice = "Jméno Test Volný" in full_text
        official_number_match = re.search(r"Číslo testu\s+([\d/]+)", full_text)
        official_number = official_number_match.group(1).strip() if official_number_match else None
        if test_type == "Unknown" and "Přezkušovací test" not in full_text: return None
        full_text = re.sub(r'^Tisk:.*$', '', full_text, flags=re.MULTILINE)
        parsed_questions = []
        question_pattern = re.compile(r'(\d+)\.\s+([\s\S]+?)Počet bodů:\s*(\d+)', re.MULTILINE)
        for match in question_pattern.finditer(full_text):
            question_block, points = match.group(2).strip(), int(match.group(3))
            first_option_match = re.search(r'\n\s*[A-C]\.', question_block)
            question_text_block = question_block[
                :first_option_match.start()].strip() if first_option_match else question_block
            options_block = question_block[first_option_match.start():].strip() if first_option_match else ""
            question_text = ' '.join(question_text_block.split())
            options, correct_option = {}, None
            option_pattern = re.compile(r'([A-C])\.\s*([\s\S]*?)(?=\n[A-C]\.|\Z)', re.MULTILINE)
            for option_match in option_pattern.finditer(options_block):
                letter, raw_text = option_match.group(1), option_match.group(2)
                if '☺' in raw_text or '☻' in raw_text: correct_option = letter
                options[letter] = ' '.join(re.sub(r'[x☺☻●]', '', raw_text).strip().split())
            parsed_questions.append(
                {"question_text": question_text, "options": options, "correct_option": correct_option,
                 "points": points})
        date_match = re.search(r'Datum\s+([\d.]+)', full_text)
        test_date = datetime.strptime(date_match.group(1), "%d.%m.%Y").date() if date_match else None
        return {"test_date": test_date, "questions": parsed_questions, "test_type": test_type,
                "is_practice": is_practice, "official_number": official_number}
    except requests.exceptions.RequestException:
        return None
    except Exception as e:
        print(f"  ! An unexpected error occurred during parsing: {e}")
        return None


def get_question_category_map():
    """Loads the JSON file and creates a mapping from question hash to category."""
    try:
        with open("unikatni_otazky_obohatene.json", 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        category_map = {}
        for item in data:
            if 'hashid' in item and 'kategorie' in item and item['hashid']:
                category_map[item['hashid']] = item['kategorie']
        print(f"Loaded {len(category_map)} categories from JSON file.")
        return category_map
    except FileNotFoundError:
        print("Category file 'unikatni_otazky_obohatene.json' not found. Categories will not be assigned.")
        return {}
    except json.JSONDecodeError:
        print("Error decoding 'unikatni_otazky_obohatene.json'. Categories will not be assigned.")
        return {}


def download_new_data(conn):
    today = date.today()
    found_today_s_test = False
    print(f"Today's date is: {today}. Script will run with high patience until a test from this date is found.")
    start_id = get_latest_test_id_from_db(conn) + 1
    end_id = start_id + 5000
    print(f"Starting download from Test ID: {start_id}")
    error_count = 0
    category_map = get_question_category_map()
    for i in range(start_id, end_id + 1):
        if conn.execute("SELECT 1 FROM questions WHERE test_id = ?", [i]).fetchone():
            print(f"--- Skipping Test ID {i} (already in database) ---")
            error_count = 0
            continue
        print(f"--- Processing Test ID {i} ---")
        url = URL_BASE + str(i)
        data = parse_pdf_from_url(url)
        if data and data.get("questions"):
            error_count = 0
            if not found_today_s_test and data['test_date'] == today:
                found_today_s_test = True
                print(f"  -> Found a test from today ({today}). Switching to normal patience ({NORMAL_PATIENCE}).")
            with conn.cursor() as con:
                for question in data['questions']:
                    question_text = question['question_text']
                    question_hash = hashlib.md5(question_text.encode('utf-8')).hexdigest()
                    category = category_map.get(question_hash, 'Nezařazeno')
                    con.execute("INSERT INTO questions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                [i, data['test_date'], question_text, question['options'].get('A'),
                                 question['options'].get('B'), question['options'].get('C'), question['correct_option'],
                                 question['points'], data['test_type'], data['is_practice'], data['official_number'],
                                 category])
            print(f"  -> Successfully saved {len(data['questions'])} questions for test type: '{data['test_type']}'.")
        else:
            error_count += 1
            print(f"  -> Test ID {i} does not exist or failed to process.")
            patience_limit = NORMAL_PATIENCE if found_today_s_test else AGGRESSIVE_PATIENCE
            if error_count >= patience_limit:
                current_mode = "normal" if found_today_s_test else "aggressive"
                print(f"Reached {error_count} consecutive errors in '{current_mode}' mode. Stopping download.")
                break
    print("\n--- Download process finished. ---")


# ==============================================================================
# PART 2: DATA ANALYSIS AND REPORT GENERATION
# ==============================================================================

def load_full_dataset(conn):
    """Loads the entire dataset from DuckDB into a Polars DataFrame."""
    print("Loading full dataset from the database...")
    df = conn.execute("SELECT * FROM questions").pl()
    if df.is_empty():
        return None
    print(f"Loaded {len(df)} total records.")
    return df


def aggregate_questions(df_to_agg):
    """Helper function to perform aggregation on a given DataFrame."""
    if df_to_agg is None or df_to_agg.is_empty():
        return None

    df_with_struct = df_to_agg.with_columns(
        pl.struct(['option_a', 'option_b', 'option_c']).alias("options")
    )

    return df_with_struct.group_by("question_text").agg(
        pl.len().alias("occurrence_count"),
        pl.col("options").first(),
        pl.col("correct_option").first(),
        pl.col("points").max().alias("points"),
        pl.col("test_date").min().alias("first_seen"),
        pl.col("test_date").max().alias("last_seen")
    ).sort("occurrence_count", descending=True)


def generate_plot_overall(df_grouped, output_path):
    """Generates and saves the Top 50 Overall Questions plot for a given dataset."""
    print(f"  - Generating plot: {os.path.basename(output_path)}")
    plt.figure(figsize=(10, 15))
    top_50 = df_grouped.head(50)
    bars = plt.barh(top_50['question_text'], top_50['occurrence_count'])
    plt.gca().invert_yaxis()
    plt.xlabel("Number of Occurrences")
    plt.ylabel("Question Text")
    plt.title("Top 50 Most Frequent Questions")
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 0.1, bar.get_y() + bar.get_height() / 2, f'{int(width)}', va='center')
    plt.tight_layout()
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()


def generate_markdown_files(df, df_grouped, plot_paths, output_dir):
    """Generates README.md and question list files for a given category."""
    print(f"  - Generating Markdown files in '{output_dir}'")

    def point_mapper(a):
        if a == "option_a":
            return "a"
        if a == "option_b":
            return "b"
        if a == "option_c":
            return "c"
        else:
            return "X"

    df_sorted = df_grouped.sort("last_seen", "points", descending=[True, True])
    for with_answers in [False, True]:
        questions_md_content = []
        for row in df_sorted.iter_rows(named=True):
            meta = f"*Points: {row['points']} | Count: {row['occurrence_count']} | First Seen: {row['first_seen']:%d.%m.%Y} | Last Seen: {row['last_seen']:%d.%m.%Y}*"
            options_md = []
            correct_opt = row['correct_option']
            for key in sorted(row['options'].keys()):
                opt_char = point_mapper(key)
                opt_text = row['options'][key]
                line = f"- {opt_char}) {opt_text}"
                if with_answers and correct_opt and opt_char.upper() == correct_opt:
                    line = f"- **{opt_char}) {opt_text}**"
                options_md.append(line)
            question_header = f"### {row['question_text']}\n{meta}\n\n"
            questions_md_content.append(question_header + "\n".join(options_md))
        if with_answers:
            filename = "QUESTIONS_WITH_ANS.md"
            title = f"# List of All Unique Questions with Answers: {df['test_type'].first()}"
        else:
            filename = "QUESTIONS.md"
            title = f"# List of All Unique Questions: {df['test_type'].first()}"
        header = f"{title}\n\nTotal unique questions: **{len(df_grouped)}**\n\n---\n\n"
        with open(os.path.join(output_dir, filename), "w", encoding="utf-8") as f:
            f.write(header + "\n\n---\n\n".join(questions_md_content))




def generate_json_export(df_grouped, output_path):
    """Exports the unique questions data to a JSON file for a given category."""

    class CustomEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (datetime, date)): return obj.isoformat()
            return super().default(obj)

    print(f"  - Generating JSON export: {os.path.basename(output_path)}")
    data_to_export = df_grouped.select([
        "question_text", "options", "correct_option", "points",
        "occurrence_count", "last_seen", "first_seen"
    ])
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data_to_export.to_dicts(), f, ensure_ascii=False, indent=4, cls=CustomEncoder)


def generate_pdf_reports(output_dir):
    """Converts the generated Markdown files in a directory to PDF using Pandoc."""
    print(f"  - Generating PDF reports in '{output_dir}'")
    for md_file_name in ['README.md', 'QUESTIONS.md', 'QUESTIONS_WITH_ANS.md']:
        md_file_path = os.path.join(output_dir, md_file_name)
        if os.path.exists(md_file_path):
            pdf_file_path = md_file_path.replace('.md', '.pdf')
            try:
                subprocess.run(
                    ['pandoc', md_file_path, '-o', pdf_file_path, "-V", "geometry:margin=1in"],
                    check=True, capture_output=True, text=True
                )
            except FileNotFoundError:
                print("\n  ! ERROR: Could not generate PDFs. Is 'pandoc' installed and in your system's PATH?")
                return
            except subprocess.CalledProcessError as e:
                print(f"\n  ! ERROR: Pandoc failed to convert {md_file_name}. Error: {e.stderr}")


def generate_outputs_for_all_categories(df):
    """
    Main orchestrator. Finds all test categories and loops through them,
    generating a full set of reports for each one.
    """
    print("\n--- Starting output generation for all categories ---")

    # Get a list of all unique, known test types
    categories = df['test_type'].unique().to_list()
    categories = [cat for cat in categories if cat != "Unknown"]

    if not categories:
        print("No known categories found to generate reports for. Exiting generation.")
        return

    print(f"Found {len(categories)} categories to process: {categories}")

    for category in categories:
        category_suffix = category.replace(' ', '_').replace('/', '_')
        output_dir = os.path.join("output", category_suffix)
        os.makedirs(output_dir, exist_ok=True)

        print(f"\n{'=' * 20} GENERATING OUTPUTS FOR CATEGORY: {category} {'=' * 20}")

        # 1. Filter data for the current category
        df_category = df.filter(pl.col("test_type") == category)

        # 2. Re-aggregate the filtered data to get category-specific stats
        df_grouped_category = aggregate_questions(df_category)

        if df_grouped_category is None:
            print(f"  - No data to process for category '{category}'. Skipping.")
            continue

        # 3. Define paths for all output files
        plot_paths = {
            "overall": os.path.join(output_dir, "top_50_frequent_questions.png"),
        }
        json_path = os.path.join(output_dir, "unique_questions.json")

        # 4. Call generation functions with category-specific data
        generate_plot_overall(df_grouped_category, plot_paths["overall"])
        generate_markdown_files(df_category, df_grouped_category, plot_paths, output_dir)
        generate_json_export(df_grouped_category, json_path)
        generate_pdf_reports(output_dir)

    print("\n--- All category-specific outputs generated successfully. ---")


# ==============================================================================
# SCRIPT ENTRY POINT
# ==============================================================================
def main():
    """Main function to orchestrate the entire process."""
    print("=" * 60)
    print("STARTING THE TEST ANALYSIS SCRIPT")
    print("=" * 60)

    with duckdb.connect(database=DB_FILE, read_only=False) as conn:
        initialize_database(conn)
        download_new_data(conn)

    with duckdb.connect(database=DB_FILE, read_only=True) as conn:
        df_full = load_full_dataset(conn)

        if df_full is None or df_full.is_empty():
            print("\nNo data in the database to analyze. Exiting.")
            return

        generate_outputs_for_all_categories(df_full)

    print("\nScript finished successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()