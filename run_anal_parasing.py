# -*- coding: utf-8 -*-
"""
Main script to download, process, and analyze test questions from laacr.cz.
This script uses a local SQLite database for efficient transactional data storage.
By default, it incrementally downloads only newly created tests from the server.
Memory-optimized: Uses SQL for heavy aggregations instead of loading everything into RAM.
Includes global metadata generation and smart skipping of unchanged data.
"""

import os
import io
import sys
import json
import re
import time
import subprocess
import warnings
import hashlib
import sqlite3
import argparse
import concurrent.futures
from threading import Lock

# Globální zámek pro bezpečný zápis do SQLite z více vláken
db_lock = Lock()
from datetime import date, datetime, timedelta
from multiprocessing import Pool, cpu_count

import matplotlib.pyplot as plt
import pdfplumber
import polars as pl
import requests
from loguru import logger
from tqdm import tqdm

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================
DB_FILE = "tests.sqlite"
URL_BASE = "https://zkouseni.laacr.cz/Zkouseni/PDFReport?module=M09&report=vysledek&id="
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

warnings.simplefilter(action='ignore', category=FutureWarning)

# Nastavení loguru
logger.remove()
logger.add(sys.stderr, level="INFO",
           format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


# ==============================================================================
# PART 1: DATA SCRAPING & DATABASE OPERATIONS
# ==============================================================================
def get_question_hash(question_text, options):
    """Vytvoří unifikovaný hash z textu a seřazených odpovědí (ignoruje pořadí a whitespace)."""
    # Sjednotíme text (všechny bílé znaky na jednu mezeru a strip)
    normalized_text = " ".join(question_text.split()).strip()
    # Seřadíme hodnoty odpovědí pro eliminaci vlivu náhodného pořadí (shuffling)
    sorted_option_values = sorted([" ".join(str(v).split()).strip() for v in options.values() if v is not None])
    combined = normalized_text + "".join(sorted_option_values)
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def initialize_database(conn):
    """Initializes the SQLite database schema with a relational structure."""
    logger.info("Inicializace databázového schématu...")

    conn.execute("PRAGMA journal_mode=WAL;")

    conn.execute("""
                 CREATE TABLE IF NOT EXISTS categories
                 (
                     id
                     INTEGER
                     PRIMARY
                     KEY
                     AUTOINCREMENT,
                     name
                     TEXT
                     UNIQUE
                 );
                 """)

    conn.execute("""
                 CREATE TABLE IF NOT EXISTS questions
                 (
                     id
                     TEXT
                     PRIMARY
                     KEY,
                     text
                     TEXT,
                     option_a
                     TEXT,
                     option_b
                     TEXT,
                     option_c
                     TEXT,
                     correct_option
                     TEXT,
                     points
                     INTEGER,
                     explanation
                     TEXT,
                     category_id
                     INTEGER
                     REFERENCES
                     categories
                 (
                     id
                 )
                     );
                 """)

    conn.execute("""
                 CREATE TABLE IF NOT EXISTS tests
                 (
                     id
                     INTEGER
                     PRIMARY
                     KEY,
                     test_date
                     DATE,
                     is_practice
                     BOOLEAN,
                     official_test_number
                     TEXT,
                     test_type
                     TEXT,
                     odbornost
                     TEXT,
                     min_points
                     INTEGER,
                     max_points
                     INTEGER
                 );
                 """)

    conn.execute("""
                 CREATE TABLE IF NOT EXISTS failed_tests
                 (
                     id
                     INTEGER
                     PRIMARY
                     KEY
                 );
                 """)

    conn.execute("""
                 CREATE TABLE IF NOT EXISTS test_questions
                 (
                     test_id
                     INTEGER,
                     question_id
                     TEXT,
                     PRIMARY
                     KEY
                 (
                     test_id,
                     question_id
                 ),
                     FOREIGN KEY
                 (
                     test_id
                 ) REFERENCES tests
                 (
                     id
                 ),
                     FOREIGN KEY
                 (
                     question_id
                 ) REFERENCES questions
                 (
                     id
                 )
                     );
                 """)

    # Přidání indexů pro extrémní zrychlení čtení a agregací
    conn.execute("CREATE INDEX IF NOT EXISTS idx_test_type ON tests(test_type);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_test_practice ON tests(is_practice);")

    conn.commit()


def import_enriched_questions(conn):
    """Loads unique questions from the JSON file into the questions table."""
    try:
        with open("unikatni_otazky_obohatene.json", 'r', encoding='utf-8') as f:
            data = json.load(f)

        cursor = conn.cursor()
        for item in data:
            if 'hashid' in item and 'text_otazky' in item and item['hashid']:
                cat_name = item.get('kategorie')
                cat_id = None

                if cat_name and cat_name != 'Nezařazeno':
                    cursor.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat_name,))
                    cursor.execute("SELECT id FROM categories WHERE name = ?", (cat_name,))
                    res = cursor.fetchone()
                    if res:
                        cat_id = res[0]

                cursor.execute(
                    """INSERT
                    OR IGNORE INTO questions 
                       (id, text, option_a, option_b, option_c, correct_option, points, category_id, explanation) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        item['hashid'],
                        item['text_otazky'],
                        item.get('moznosti', {}).get('A'),
                        item.get('moznosti', {}).get('B'),
                        item.get('moznosti', {}).get('C'),
                        item.get('spravna_odpoved'),
                        item.get('body'),
                        cat_id,
                        item.get('vysvetleni')
                    )
                )
        conn.commit()
    except Exception:
        pass


def parse_pdf_from_url(url: str, session: requests.Session):
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        with io.BytesIO(response.content) as pdf_stream, pdfplumber.open(pdf_stream) as pdf:
            full_text = "".join(page.extract_text() + "\n" for page in pdf.pages)

        test_type_match = re.search(r"Přezkušovací test\s+(.*)", full_text)
        test_type = test_type_match.group(1).strip() if test_type_match else "Unknown"

        is_practice = "Jméno Test Volný" in full_text

        official_number_match = re.search(r"Číslo testu\s+([\d/]+)", full_text)
        official_number = official_number_match.group(1).strip() if official_number_match else None

        odbornost_match = re.search(r"Odbornost\s+(.+)", full_text)
        odbornost = odbornost_match.group(1).strip() if odbornost_match else None

        max_points_match = re.search(r"Maximální počet bodů\s+(\d+)", full_text)
        max_points = int(max_points_match.group(1)) if max_points_match else None

        min_points_match = re.search(r"Minimální počet bodů\s+(\d+)", full_text)
        min_points = int(min_points_match.group(1)) if min_points_match else None

        if test_type == "Unknown" and "Přezkušovací test" not in full_text:
            return None

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

            options = {}
            raw_options = []
            option_pattern = re.compile(r'([A-C])\.\s*([\s\S]*?)(?=\n[A-C]\.|\Z)', re.MULTILINE)

            for option_match in option_pattern.finditer(options_block):
                letter, raw_text = option_match.group(1), option_match.group(2)
                raw_options.append((letter, raw_text))

            correct_option = None

            for letter, raw_text in raw_options:
                if any(symbol in raw_text for symbol in ['☐', '☺', '☻', '●']):
                    correct_option = letter
                    break

            if not correct_option:
                for letter, raw_text in raw_options:
                    if 'x' in raw_text[:5].lower() or '☑' in raw_text:
                        correct_option = letter
                        break

            for letter, raw_text in raw_options:
                clean_text = re.sub(r'^\s*[xX]\s+', '', raw_text)
                clean_text = ' '.join(re.sub(r'[☺☻●☑☐]', '', clean_text).strip().split())
                options[letter] = clean_text

            question_hash = get_question_hash(question_text, options)

            parsed_questions.append({
                "hash": question_hash,
                "question_text": question_text,
                "options": options,
                "correct_option": correct_option,
                "points": points
            })

        date_match = re.search(r'Datum\s+([\d.]+)', full_text)
        test_date = datetime.strptime(date_match.group(1), "%d.%m.%Y").date() if date_match else None

        return {
            "test_date": test_date,
            "questions": parsed_questions,
            "test_type": test_type,
            "is_practice": is_practice,
            "official_number": official_number,
            "odbornost": odbornost,
            "min_points": min_points,
            "max_points": max_points
        }
    except requests.exceptions.RequestException as e:
        logger.debug(f"Síťová chyba při stahování: {e}")
        return "NETWORK_ERROR"
    except Exception as e:
        return None


def save_test_to_db(conn, current_id, data):
    """Pomocná funkce pro bezpečné uložení testu a jeho otázek do databáze."""
    question_hashes = []

    for question in data['questions']:
        q_hash = question['hash']
        question_hashes.append(q_hash)

        conn.execute(
            """INSERT
            OR IGNORE INTO questions 
               (id, text, option_a, option_b, option_c, correct_option, points)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                q_hash,
                question['question_text'],
                question['options'].get('A'),
                question['options'].get('B'),
                question['options'].get('C'),
                question.get('correct_option'),
                question.get('points')
            )
        )

    conn.execute(
        """INSERT
        OR IGNORE INTO tests 
           (id, test_date, is_practice, official_test_number, test_type, odbornost, min_points, max_points) 
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            current_id,
            data['test_date'].strftime("%Y-%m-%d") if data['test_date'] else None,
            data['is_practice'],
            data['official_number'],
            data['test_type'],
            data['odbornost'],
            data['min_points'],
            data['max_points']
        )
    )

    for q_hash in question_hashes:
        conn.execute(
            "INSERT OR IGNORE INTO test_questions (test_id, question_id) VALUES (?, ?)",
            (current_id, q_hash)
        )

    conn.commit()


def download_new_data(conn, args, session):
    """Paralelní stahování novinek pomocí ThreadPoolExecutor pro maximální výkon."""
    cursor = conn.cursor()

    start_fallback = args.start_id if args.start_id else 900000
    check_failed_cache = True

    if args.start_id:
        current_id = args.start_id
        check_failed_cache = False
    else:
        max_t = cursor.execute("SELECT MAX(id) FROM tests").fetchone()[0]
        max_f = cursor.execute("SELECT MAX(id) FROM failed_tests").fetchone()[0]
        max_t = max_t if max_t is not None else start_fallback
        max_f = max_f if max_f is not None else start_fallback
        current_id = max(max_t, max_f) + 1

    logger.info(f"Paralelní stahování zahájeno od ID: {current_id}")
    pbar = tqdm(desc="Stahování", unit="test")
    last_processed_date = None
    error_count = 0
    max_workers = 30

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            if args.end_id and current_id > args.end_id:
                break

            # Příprava balíčku ID, která ještě nemáme v DB ani failed_cache
            batch_ids = []
            probe_id = current_id
            while len(batch_ids) < max_workers:
                if args.end_id and probe_id > args.end_id:
                    break
                
                # Rychlá kontrola existence v DB/failed cache
                in_db = cursor.execute("SELECT 1 FROM tests WHERE id = ?", (probe_id,)).fetchone()
                in_failed = check_failed_cache and cursor.execute("SELECT 1 FROM failed_tests WHERE id = ?", (probe_id,)).fetchone()
                
                if not in_db and not in_failed:
                    batch_ids.append(probe_id)
                probe_id += 1
            
            if not batch_ids:
                if args.end_id and probe_id > args.end_id: break
                current_id = probe_id
                continue

            # Paralelní stažení a parsování
            future_to_id = {executor.submit(parse_pdf_from_url, URL_BASE + str(tid), session): tid for tid in batch_ids}
            
            # Sběr výsledků v pořadí dokončení
            results = {}
            for future in concurrent.futures.as_completed(future_to_id):
                results[future_to_id[future]] = future.result()
            
            stop_loop = False
            for tid in sorted(batch_ids):
                data = results[tid]
                current_id = tid + 1
                
                pbar.update(1)
                pbar.set_postfix({"ID": tid, "Chyby": error_count, "Datum": last_processed_date.strftime('%d.%m.%Y') if last_processed_date else '...'})

                if data == "NETWORK_ERROR":
                    # Při síťové chybě zkusíme sekvenčně s malým čekáním
                    time.sleep(2)
                    data = parse_pdf_from_url(URL_BASE + str(tid), session)

                if data and isinstance(data, dict) and data.get("questions"):
                    error_count = 0
                    if data['test_date']:
                        last_processed_date = data['test_date']
                    save_test_to_db(conn, tid, data)
                else:
                    error_count += 1
                    cursor.execute("INSERT OR IGNORE INTO failed_tests (id) VALUES (?)", (tid,))
                    conn.commit()

                    if args.end_id:
                        # Pokud je zadáno end_id, pokračujeme až do konce bez ohledu na chyby
                        continue

                    if error_count == 1000:
                        logger.warning(f"Dosaženo 1000 chyb. Zkouším skok o 5000 ID vpřed ({tid} -> {tid + 5000}).")
                        current_id = tid + 5000
                        error_count = 9900  # Zbývá 100 pokusů do limitu 10000
                        break

                    if error_count >= 10000:
                        logger.warning(f"Dosažen limit 10000 chyb. Končím.")
                        stop_loop = True
                        break
            
            if stop_loop: break
            
    pbar.close()


# ==============================================================================
# PART 2: MEMORY-OPTIMIZED SQL DATA ANALYSIS AND REPORTS
# ==============================================================================

class CustomJSONEncoder(json.JSONEncoder):
    """Společný encoder pro serializaci datumů do JSONu."""

    def default(self, obj):
        if isinstance(obj, (datetime, date)): return obj.isoformat()
        return super().default(obj)


def generate_global_metadata(conn, output_dir):
    """Generuje globální metadata soubor s informacemi o celém datasetu."""
    os.makedirs(output_dir, exist_ok=True)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM tests")
    total_tests = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM questions")
    total_questions = cursor.fetchone()[0]

    cursor.execute("""
                   SELECT test_type, COUNT(*)
                   FROM tests
                   WHERE test_type IS NOT NULL
                   GROUP BY test_type
                   """)
    test_types_breakdown = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("""
                   SELECT COALESCE(c.name, 'Nezařazeno'), COUNT(q.id)
                   FROM questions q
                            LEFT JOIN categories c ON q.category_id = c.id
                   GROUP BY c.name
                   """)
    categories_breakdown = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT MAX(test_date) FROM tests")
    latest_test_date = cursor.fetchone()[0]

    metadata = {
        "last_generated": datetime.now().isoformat(),
        "latest_test_date": latest_test_date,
        "total_tests": total_tests,
        "total_questions": total_questions,
        "breakdown_by_test_type": test_types_breakdown,
        "breakdown_by_category": categories_breakdown
    }

    metadata_path = os.path.join(output_dir, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    return metadata


def fetch_aggregated_questions_for_category(conn, test_type):
    """
    MEMORY OPTIMIZATION: Místo tahání milionů řádků do paměti a složitého seskupování
    v Pythonu/Polars, necháme databázi provést agregaci (COUNT, MIN, MAX) na úrovni SQLite.
    Tím ušetříme gigabyty RAM.
    """
    cursor = conn.cursor()
    query = """
            SELECT q.id                           AS question_id, \
                   q.text                         AS question_text, \
                   q.option_a, \
                   q.option_b, \
                   q.option_c, \
                   q.correct_option, \
                   MAX(q.points)                  AS points, \
                   q.explanation, \
                   COALESCE(c.name, 'Nezařazeno') AS category, \
                   COUNT(t.id)                    AS occurrence_count, \
                   MIN(t.test_date)               AS first_seen, \
                   MAX(t.test_date)               AS last_seen
            FROM questions q
                     JOIN test_questions tq ON q.id = tq.question_id
                     JOIN tests t ON tq.test_id = t.id
                     LEFT JOIN categories c ON q.category_id = c.id
            WHERE t.test_type = ?
            GROUP BY q.id
            ORDER BY occurrence_count DESC \
            """
    cursor.execute(query, (test_type,))
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()

    if not data:
        return None

    # Vytvoření odlehčeného DataFramu pouze pro jednu kategorii
    df = pl.DataFrame(data, schema=columns, orient="row")

    # Ošetření formátu datumu z DB (v SQLite uloženo jako text) do Polars Date typu
    if 'first_seen' in df.columns:
        df = df.with_columns(pl.col('first_seen').str.strptime(pl.Date, "%Y-%m-%d", strict=False))
    if 'last_seen' in df.columns:
        df = df.with_columns(pl.col('last_seen').str.strptime(pl.Date, "%Y-%m-%d", strict=False))

    # Zabalení možností (option_a, b, c) do jednoho structu/slovníku 'options',
    # aby to odpovídalo očekávanému formátu v generátorech Markdownu a JSONu.
    df_with_struct = df.with_columns(
        pl.struct(['option_a', 'option_b', 'option_c']).alias("options")
    )

    return df_with_struct


def generate_plot_overall(df_grouped, output_path):
    plt.figure(figsize=(10, 15))
    top_50 = df_grouped.head(50)
    bars = plt.barh(top_50['question_text'], top_50['occurrence_count'])
    plt.gca().invert_yaxis()
    plt.xlabel("Počet výskytů")
    plt.ylabel("Text otázky")
    plt.title("Top 50 nejčastějších otázek")
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 0.1, bar.get_y() + bar.get_height() / 2, f'{int(width)}', va='center')
    plt.tight_layout()
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()


def generate_markdown_files(df_grouped, test_type, output_dir):
    def point_mapper(a):
        if a == "option_a": return "a"
        if a == "option_b": return "b"
        if a == "option_c": return "c"
        return "X"

    df_sorted = df_grouped.sort("last_seen", "points", descending=[True, True])

    for with_answers in [False, True]:
        questions_md_content = []
        for row in df_sorted.iter_rows(named=True):
            first_seen_str = row['first_seen'].strftime("%d.%m.%Y") if row['first_seen'] else "N/A"
            last_seen_str = row['last_seen'].strftime("%d.%m.%Y") if row['last_seen'] else "N/A"
            meta = f"*Body: {row['points']} | Výskyty: {row['occurrence_count']} | Kategorie: {row['category']} | První výskyt: {first_seen_str} | Poslední výskyt: {last_seen_str}*"

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

            # Přidání vysvětlení
            if with_answers and row.get('explanation'):
                questions_md_content.append(f"\n> **Vysvětlení:** {row['explanation']}\n")

        if with_answers:
            filename = "QUESTIONS_WITH_ANS.md"
            title = f"# Seznam všech unikátních otázek (S ODPOVĚĎMI): {test_type}"
        else:
            filename = "QUESTIONS.md"
            title = f"# Seznam všech unikátních otázek: {test_type}"

        header = f"{title}\n\nCelkem unikátních otázek: **{len(df_grouped)}**\n\n---\n\n"
        with open(os.path.join(output_dir, filename), "w", encoding="utf-8") as f:
            f.write(header + "\n\n---\n\n".join(questions_md_content))


def generate_json_export(df_grouped, output_path):
    """JSON 2: Otázky včetně kategorie a případného vysvětlení (explanation)."""
    data_to_export = df_grouped.select([
        "question_id", "category", "question_text", "options", "correct_option", "points",
        "explanation", "occurrence_count", "last_seen", "first_seen"
    ])
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data_to_export.to_dicts(), f, ensure_ascii=False, indent=4, cls=CustomJSONEncoder)


def generate_categories_json(conn, test_type, output_path):
    """JSON 1: Rychlý SQL dotaz na unikátní kategorie pro daný typ testu."""
    cursor = conn.cursor()
    cursor.execute("""
                   SELECT DISTINCT c.name
                   FROM questions q
                            JOIN test_questions tq ON q.id = tq.question_id
                            JOIN tests t ON tq.test_id = t.id
                            JOIN categories c ON q.category_id = c.id
                   WHERE t.test_type = ?
                     AND c.name IS NOT NULL
                   ORDER BY c.name
                   """, (test_type,))

    unique_cats = [r[0] for r in cursor.fetchall()]

    data = {
        "test_type": test_type,
        "total_categories": len(unique_cats),
        "categories": unique_cats
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def generate_real_tests_json(conn, test_type, output_path):
    """JSON 3: Rychlý SQL dotaz na ostré testy a spojení IDček otázek přímo v databázi."""
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT t.id, t.test_date, t.official_test_number, GROUP_CONCAT(tq.question_id)
                   FROM tests t
                            JOIN test_questions tq ON t.id = tq.test_id
                   WHERE t.test_type = ?
                     AND t.is_practice = 0
                   GROUP BY t.id, t.test_date, t.official_test_number
                   """, (test_type,))

    rows = cursor.fetchall()
    real_tests_list = []

    for row in rows:
        real_tests_list.append({
            "test_id": row[0],
            "test_date": row[1],
            "official_test_number": row[2],
            "question_ids": row[3].split(',') if row[3] else []
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(real_tests_list, f, ensure_ascii=False, indent=4, cls=CustomJSONEncoder)


def convert_md_to_pdf(md_file_path):
    if os.path.exists(md_file_path):
        pdf_file_path = md_file_path.replace('.md', '.pdf')
        try:
            subprocess.run(
                ['pandoc', md_file_path, '-o', pdf_file_path, "-V", "geometry:margin=1in"],
                check=True, capture_output=True, text=True, timeout=60
            )
            return f"[OK] Úspěšně převedeno: {os.path.basename(md_file_path)}"
        except FileNotFoundError:
            return "[ERROR] Nelze vygenerovat PDF. Je 'pandoc' nainstalován v systému?"
        except subprocess.TimeoutExpired:
            return f"[ERROR] Timeout pandocu při převodu {os.path.basename(md_file_path)}"
        except subprocess.CalledProcessError as e:
            return f"[ERROR] Selhání pandocu na souboru {os.path.basename(md_file_path)}. Detail: {e.stderr}"
    return f"[SKIP] Markdown soubor nenalezen: {os.path.basename(md_file_path)}"


def generate_outputs_for_all_categories(conn, skip_pdf_gen=False):
    """Hlavní smyčka pro generování reportů - optimalizováno na využití paměti."""
    logger.info("Zahajuji generování výstupů a reportů pro jednotlivé kategorie (optimalizováno na paměť).")

    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT test_type FROM tests WHERE test_type IS NOT NULL AND test_type != 'Unknown'")
    categories = [r[0] for r in cursor.fetchall()]

    if not categories:
        logger.warning("Nebyly nalezeny žádné známé kategorie. Generování ukončeno.")
        return

    md_files_to_convert = []

    for category in categories:
        logger.info(f"Zpracovávám: {category}")
        category_suffix = category.replace(' ', '_').replace('/', '_')
        output_dir = os.path.join("output", category_suffix)
        os.makedirs(output_dir, exist_ok=True)

        df_grouped_category = fetch_aggregated_questions_for_category(conn, category)

        if df_grouped_category is None:
            continue

        plot_paths = {"overall": os.path.join(output_dir, "top_50_frequent_questions.png")}
        json_unique_qs = os.path.join(output_dir, "unique_questions.json")
        json_cats = os.path.join(output_dir, "question_categories.json")
        json_real_tests = os.path.join(output_dir, "real_tests.json")

        generate_plot_overall(df_grouped_category, plot_paths["overall"])
        generate_markdown_files(df_grouped_category, category, output_dir)

        generate_json_export(df_grouped_category, json_unique_qs)
        generate_categories_json(conn, category, json_cats)
        generate_real_tests_json(conn, category, json_real_tests)

        for md_file in ['README.md', 'QUESTIONS.md', 'QUESTIONS_WITH_ANS.md']:
            md_files_to_convert.append(os.path.join(output_dir, md_file))

    if skip_pdf_gen:
        logger.warning("Přeskočeno generování PDF (--skip-pdf-gen bylo aktivováno).")
    elif md_files_to_convert:
        logger.info(f"Spouštím paralelní generování PDF pro {len(md_files_to_convert)} souborů...")
        num_workers = max(1, cpu_count() - 1)

        with Pool(processes=num_workers) as pool:
            results = list(tqdm(
                pool.imap(convert_md_to_pdf, md_files_to_convert),
                total=len(md_files_to_convert),
                desc="Převod do PDF",
                unit="soubor"
            ))

            for res in results:
                if "[ERROR]" in res:
                    logger.error(res)

    logger.success("Všechny analytické a datové reporty byly úspěšně vytvořeny.")


# ==============================================================================
# SCRIPT ENTRY POINT
# ==============================================================================
def main():
    """Main function to orchestrate the entire process."""
    parser = argparse.ArgumentParser(description="Analyzátor testů - Paměťově optimalizovaná verze.")
    parser.add_argument('--skip-pdf-gen', action='store_true', help='Přeskočí generování PDF pomocí Pandocu.')
    parser.add_argument('--skip-scraping', action='store_true',
                        help='Zcela přeskočí stahování dat a rovnou vygeneruje reporty.')
    parser.add_argument('--start-id', type=int, help='Počáteční ID (pokud nezadáno, najde si sám konec databáze).')
    parser.add_argument('--end-id', type=int, help='Konečné ID, u kterého se skript zastaví.')
    parser.add_argument('--clear-failed', action='store_true',
                        help='Vymaže cache neúspěšných testů a zkusí je stáhnout znovu.')
    parser.add_argument('--force-generate', action='store_true',
                        help='Vynutí generování reportů i v případě, že se v databázi neobjevila žádná nová data.')

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("STARTING THE TEST ANALYSIS SCRIPT (Memory Optimized SQL Edition)")

    if args.skip_pdf_gen:
        logger.info("Zvolen režim: BEZ generování PDF")
    if args.skip_scraping:
        logger.info("Zvolen režim: POUZE ANALÝZA (bez stahování)")

    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)

    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        initialize_database(conn)
        import_enriched_questions(conn)

        if args.clear_failed:
            conn.execute("DELETE FROM failed_tests")
            conn.commit()
            logger.info("Cache neúspěšných testů (failed_tests) byla úspěšně vymazána. Skript zkusí staré díry znovu.")

        if not args.skip_scraping:
            download_new_data(conn, args, session)
        else:
            logger.info("Fáze stahování přeskočena.")

        logger.info("=" * 60)

        # Kontrola, zda má vůbec smysl generovat reporty (pokud nepřibyla data)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tests")
        current_total_tests = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM questions")
        current_total_questions = cursor.fetchone()[0]

        metadata_path = os.path.join("output", "metadata.json")
        if os.path.exists(metadata_path) and not args.force_generate:
            try:
                with open(metadata_path, "r", encoding="utf-8") as f:
                    old_meta = json.load(f)

                if old_meta.get("total_tests") == current_total_tests and old_meta.get(
                        "total_questions") == current_total_questions:
                    logger.success(
                        "V databázi nejsou žádné nové testy ani otázky. Přeskakuji generování reportů. (Použijte --force-generate pro vynucení).")
                    return
            except Exception as e:
                logger.warning(f"Nelze přečíst stará metadata, generování proběhne naplno: {e}")

        logger.info("Zahajuji analýzu a generování souborů...")

        # Generování globálního metadata.json do rootu složky output/
        generate_global_metadata(conn, "output")

        generate_outputs_for_all_categories(conn, skip_pdf_gen=args.skip_pdf_gen)

    logger.success("Celý skript proběhl bez kritických chyb.")


if __name__ == "__main__":
    main()