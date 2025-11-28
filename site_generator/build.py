import json
import os
import shutil
import hashlib
from datetime import date, datetime
import calendar

import duckdb
import matplotlib.pyplot as plt
import polars as pl
from dateutil.relativedelta import relativedelta
from jinja2 import Environment, FileSystemLoader

# Czech month abbreviations
CZECH_MONTH_ABBR = [
    "", "Led", "Úno", "Bře", "Dub", "Kvě", "Čvn",
    "Čvc", "Srp", "Zář", "Říj", "Lis", "Pro"
]

def create_occurrence_plot(dates, output_path):
    """Creates and saves a timeline plot of question occurrences."""
    if not dates:
        # Create a blank plot if there's no data
        fig, ax = plt.subplots(figsize=(10, 2))
        ax.text(0.5, 0.5, "No occurrence data available", ha='center', va='center')
        ax.set_axis_off()
        plt.savefig(output_path)
        plt.close(fig)
        return

    # Ensure dates are datetime objects
    dates = [d for d in dates if d]
    if not dates:
        return # Exit if list becomes empty after filtering
        
    fig, ax = plt.subplots(figsize=(10, 2))
    ax.plot(dates, [0] * len(dates), 'o', markersize=10, alpha=0.7)
    
    ax.yaxis.set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True, nbins='auto'))
    fig.autofmt_xdate()
    ax.set_title("Timeline of Appearances")
    plt.grid(axis='x', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)

def generate_heatmap_svg(dates: list, months_to_show=6) -> str:
    """Generates an SVG calendar heatmap for the last `months_to_show`."""
    if not dates:
        return ""

    today = date.today()
    occurrence_dates = set(d.date() if isinstance(d, datetime) else d for d in dates)

    cell_size = 8
    cell_padding = 2
    month_width = (cell_size + cell_padding) * 5 # Approx. 5 weeks per month for layout
    total_width = month_width * months_to_show + (months_to_show - 1) * cell_padding # Add padding between months
    total_height = (cell_size + cell_padding) * 7 + 15 # 7 days + label
    
    svg_parts = [f'<svg width="{total_width}" height="{total_height}" xmlns="http://www.w3.org/2000/svg">']
    
    for i in range(months_to_show):
        month_date = today - relativedelta(months=months_to_show - 1 - i)
        month_start_date = month_date.replace(day=1)
        
        # Month label in Czech
        month_label = CZECH_MONTH_ABBR[month_date.month]
        
        month_offset_x = i * month_width + i * cell_padding # Offset for current month
        svg_parts.append(f'<text x="{month_offset_x + month_width / 2}" y="10" font-family="sans-serif" font-size="10" text-anchor="middle">{month_label}</text>')
        
        first_day_weekday = month_start_date.weekday() # Monday is 0, Sunday is 6
        days_in_month = calendar.monthrange(month_start_date.year, month_start_date.month)[1]
        
        for day_of_month in range(1, days_in_month + 1):
            current_date = date(month_start_date.year, month_start_date.month, day_of_month)
            day_weekday = current_date.weekday() # 0=Mon, ..., 6=Sun
            
            week_num = (day_of_month - 1 + first_day_weekday) // 7
            
            x = month_offset_x + day_weekday * (cell_size + cell_padding)
            y = 15 + week_num * (cell_size + cell_padding)

            fill = "#0d6efd" if current_date in occurrence_dates else "#ebedf0"
            svg_parts.append(f'<rect x="{x}" y="{y}" width="{cell_size}" height="{cell_size}" fill="{fill}" rx="1" ry="1" />')

    svg_parts.append('</svg>')
    return "".join(svg_parts)


def main():
    """Generates the static site."""

    # -- 1. Configuration --
    DB_FILE = "tests.duckdb"
    QUESTIONS_JSON = "output/ULL_Pilot/unique_questions.json"
    ENRICHED_JSON = "unikatni_otazky_obohatene.json"
    OUTPUT_DIR = "output/site"
    PLOTS_DIR = os.path.join(OUTPUT_DIR, "plots")
    QUESTIONS_DIR = os.path.join(OUTPUT_DIR, "questions")
    TEMPLATE_DIR = "site_generator/templates"

    # -- 2. Setup --
    print("Setting up directories and templates...")
    os.makedirs(PLOTS_DIR, exist_ok=True)
    os.makedirs(QUESTIONS_DIR, exist_ok=True)
        
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=True)
    index_template = env.get_template("index.html.j2")
    question_template = env.get_template("question_detail.html.j2")
    table_template = env.get_template("table.html.j2")

    # -- 3. Data Loading --
    print("Loading data...")
    try:
        with open(QUESTIONS_JSON, 'r', encoding='utf-8') as f:
            unique_questions_data = json.load(f)
        
        with open(ENRICHED_JSON, 'r', encoding='utf-8') as f:
            enriched_data = json.load(f)
            
        with duckdb.connect(database=DB_FILE, read_only=True) as conn:
            all_occurrences_df = conn.execute("SELECT question_text, test_date FROM questions WHERE test_type = 'ULL Pilot'").pl()
    
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Create a lookup map from the enriched data
    enriched_map = {
        item['hashid']: {
            'kategorie': item.get('kategorie'),
            'vysvetleni': item.get('vysvetleni')
        } 
        for item in enriched_data if 'hashid' in item
    }

    # -- 4. Processing and Generation --
    print("Generating pages, plots, and heatmaps...")
    for question in unique_questions_data:
        question_text = question['question_text']
        # The hashid is not present in unique_questions.json, we must compute it
        hash_id = hashlib.md5(question_text.encode('utf-8')).hexdigest()
        question['hashid'] = hash_id
        
        # Enrich the question data
        extra_info = enriched_map.get(hash_id, {})
        question.update(extra_info)

        occurrence_dates = all_occurrences_df.filter(pl.col("question_text") == question_text)['test_date'].to_list()
        
        plot_path = os.path.join(PLOTS_DIR, f"{hash_id}.png")
        create_occurrence_plot(occurrence_dates, plot_path)
        
        question['heatmap_svg'] = generate_heatmap_svg(occurrence_dates)

        occurrence_dates_str = sorted(list(set([d.strftime("%Y-%m-%d") for d in occurrence_dates])), reverse=True)
        detail_html_content = question_template.render(
            title="Detail otázky",
            question=question,
            occurrence_dates=occurrence_dates_str
        )
        with open(os.path.join(QUESTIONS_DIR, f"{hash_id}.html"), 'w', encoding='utf-8') as f:
            f.write(detail_html_content)

    print(f"Generated {len(unique_questions_data)} detail pages and plots.")

    # -- 5. Main and Table Page Generation --
    print("Generating main and table pages...")
    index_html_content = index_template.render(title="Otázky pro piloty ULL", questions=unique_questions_data)
    with open(os.path.join(OUTPUT_DIR, "index.html"), 'w', encoding='utf-8') as f:
        f.write(index_html_content)

    table_html_content = table_template.render(title="Všechny otázky v tabulce", questions=unique_questions_data)
    with open(os.path.join(OUTPUT_DIR, "table.html"), 'w', encoding='utf-8') as f:
        f.write(table_html_content)

    # -- 6. Asset Copying --
    print("Copying static assets...")
    assets_to_copy = ["output/ULL_Pilot/top_50_frequent_questions.png", "output/ULL_Pilot/QUESTIONS_WITH_ANS.pdf"]
    for asset_path in assets_to_copy:
        try:
            shutil.copy(asset_path, os.path.join(OUTPUT_DIR, os.path.basename(asset_path)))
            print(f"Successfully copied asset: {asset_path}")
        except FileNotFoundError:
            print(f"Warning: Asset file not found at '{asset_path}'.")

    print("\nStatic site generation complete!")


if __name__ == "__main__":
    main()
