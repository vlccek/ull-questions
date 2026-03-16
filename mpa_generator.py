import os
import json
import html
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import math
from datetime import datetime

# --- KONFIGURACE ---
ITEMS_PER_PAGE = 500

COMMON_HEAD = """
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="https://cdn.tailwindcss.com"></script>
    <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://unpkg.com/lucide@latest"></script>
    <style>
        [x-cloak] { display: none !important; }
        .sidebar-scroll { max-height: calc(100vh - 140px); overflow-y: auto; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-thumb { background: #475569; border-radius: 10px; }
        .dot { width: 4px; height: 4px; background: #3b82f6; border-radius: 50%; position: absolute; top: -1px; transform: translateX(-50%); }
        .tooltip { position: relative; display: inline-block; }
        .tooltip .tooltiptext { visibility: hidden; width: 220px; background-color: #1e293b; color: #fff; text-align: center; border-radius: 12px; padding: 12px; position: absolute; z-index: 100; bottom: 125%; left: 50%; margin-left: -110px; opacity: 0; transition: opacity 0.3s; font-size: 12px; font-weight: normal; line-height: 1.5; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.2); border: 1px solid #334155; }
        .tooltip:hover .tooltiptext { visibility: visible; opacity: 1; }
    </style>
"""

def get_timeline_svg(seen_dates, min_date, max_date, color="#3b82f6"):
    """Generuje absolutní časovou osu s vyznačenými roky."""
    if not seen_dates or not min_date or not max_date: return ""
    total_days, viewbox_w = (max_date - min_date).days or 1, 1000
    ticks_html = ""
    for year in range(min_date.year, max_date.year + 1):
        year_dt = datetime(year, 1, 1)
        if year_dt < min_date: year_dt = min_date
        x = ((year_dt - min_date).days / total_days) * viewbox_w
        if 0 <= x <= viewbox_w:
            ticks_html += f'<line x1="{x}" y1="0" x2="{x}" y2="15" stroke="#cbd5e1" stroke-width="1" /><text x="{x}" y="35" text-anchor="middle" font-size="12" font-weight="bold" fill="#94a3b8">{year}</text>'
    path_data = []
    for d_str in sorted(list(set(seen_dates))):
        try:
            d_dt = datetime.strptime(d_str, "%Y-%m-%d")
            x = round(((d_dt - min_date).days / total_days) * viewbox_w, 1)
            path_data.append(f"M{x},2v10")
        except: continue
    return f"""<div class="mt-8 mb-2"><div class="text-[10px] font-black text-slate-400 uppercase tracking-widest flex items-center gap-2 mb-2"><i data-lucide="activity" class="w-3 h-3" style="color: {color}"></i> Historie výskytů (v kalendářním čase)</div><svg viewBox="0 0 {viewbox_w} 40" class="w-full h-10 overflow-visible"><line x1="0" y1="7.5" x2="{viewbox_w}" y2="7.5" stroke="#f1f5f9" stroke-width="1" />{ticks_html}<path d='{"".join(path_data)}' stroke="{color}" stroke-width="2" stroke-linecap="round" /></svg></div>"""

def get_sidebar(sorted_tests, root_path, current_test=None):
    nav_items = ""
    for test_name, count in sorted_tests:
        folder = test_name.replace(" ", "_")
        active = "bg-slate-800 text-blue-400 ring-1 ring-slate-700 font-bold" if test_name == current_test else "text-slate-400 hover:bg-slate-800 hover:text-white"
        nav_items += f'<a href="{root_path}{folder}/index.html" class="w-full block px-4 py-2.5 rounded-xl transition flex justify-between items-center group {active}"><span class="truncate text-sm">{test_name}</span><span class="text-[10px] bg-slate-950 px-2 py-0.5 rounded-full border border-slate-800 group-hover:text-blue-400">{count}</span></a>'
    
    return f"""
        <!-- Mobile Header -->
        <div class="lg:hidden bg-slate-900 text-white p-4 flex justify-between items-center sticky top-0 z-50 shadow-lg">
            <a href="{root_path}index.html" class="flex items-center gap-2">
                <div class="bg-blue-600 p-1.5 rounded-lg"><i data-lucide="plane" class="w-5 h-5"></i></div>
                <span class="font-black tracking-tighter uppercase italic text-sm">ULL Analýza</span>
            </a>
            <button @click="mobileMenu = !mobileMenu" class="p-2 bg-slate-800 rounded-lg text-slate-300">
                <i data-lucide="menu" x-show="!mobileMenu"></i>
                <i data-lucide="x" x-show="mobileMenu"></i>
            </button>
        </div>

        <!-- Sidebar Overlay (Mobile) -->
        <div x-show="mobileMenu" x-cloak class="fixed inset-0 bg-slate-900/60 backdrop-blur-sm z-40 lg:hidden" @click="mobileMenu = false"></div>

        <!-- Sidebar Container -->
        <aside :class="mobileMenu ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'" 
               class="fixed lg:sticky top-0 left-0 w-72 bg-slate-900 text-white flex-shrink-0 flex flex-col h-screen shadow-2xl z-50 transition-transform duration-300 ease-in-out lg:translate-x-0">
            <div class="p-8 border-b border-slate-800 hidden lg:block text-center">
                <a href="{root_path}index.html" class="inline-flex items-center gap-3">
                    <div class="bg-blue-600 p-2.5 rounded-xl shadow-lg"><i data-lucide="plane" class="w-6 h-6 text-white"></i></div>
                    <span class="text-xl font-black tracking-tighter uppercase italic">ULL Analýza</span>
                </a>
            </div>
            <nav class="flex-1 p-4 sidebar-scroll space-y-1">
                <a href="{root_path}index.html" class="w-full block px-4 py-3 rounded-xl transition flex items-center gap-3 font-bold {'bg-blue-600 text-white shadow-xl shadow-blue-900/40' if current_test == 'Dashboard' else 'text-slate-400 hover:bg-slate-800 hover:text-white'}">
                    <i data-lucide="layout-dashboard" class="w-5 h-5"></i> Dashboard
                </a>
                <a href="https://ull-trainer.jevlk.cz/" target="_blank" class="w-full block px-4 py-3 rounded-xl transition flex items-center gap-3 font-bold text-indigo-400 hover:bg-slate-800 hover:text-indigo-300">
                    <i data-lucide="zap" class="w-5 h-5"></i> Procvičování
                </a>
                <div class="pt-8 pb-3 px-4 text-[10px] font-black text-slate-500 uppercase tracking-[0.2em]">Moduly</div>
                {nav_items}
            </nav>
        </aside>
    """

def deploy_site(output_dir="output"):
    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    with open(os.path.join(output_dir, "metadata.json"), "r", encoding="utf-8") as f: metadata = json.load(f)
    
    cursor.execute("SELECT MIN(test_date)::text as min_d, MAX(test_date)::text as max_d FROM tests")
    row = cursor.fetchone()
    db_min_str, db_max_str = row['min_d'], row['max_d']
    db_min = datetime.strptime(db_min_str, "%Y-%m-%d")
    db_max = datetime.strptime(db_max_str, "%Y-%m-%d")
    sorted_tests = sorted(metadata["breakdown_by_test_type"].items(), key=lambda x: x[1], reverse=True)

    # --- DASHBOARD ---
    with open(os.path.join(output_dir, "index.html"), "w", encoding="utf-8") as f:
        sidebar = get_sidebar(sorted_tests, "", "Dashboard")
        f.write(f'<!DOCTYPE html><html lang="cs"><head>{COMMON_HEAD}<title>Dashboard</title></head><body class="bg-slate-50 text-slate-900 font-sans flex flex-col lg:flex-row min-h-screen" x-data="{{ mobileMenu: false }}">{sidebar}<main class="flex-1 p-4 sm:p-10 overflow-y-auto"><h2 class="text-2xl sm:text-4xl font-black text-slate-800 tracking-tighter mb-8 uppercase italic">Dashboard</h2><a href="https://ull-trainer.jevlk.cz/" target="_blank" class="block mb-8 p-6 sm:p-10 bg-gradient-to-br from-indigo-600 to-blue-700 rounded-2xl sm:rounded-[40px] text-white shadow-xl shadow-indigo-200 hover:shadow-indigo-300 transition-all group overflow-hidden relative"><div class="absolute right-0 top-0 opacity-10 translate-x-1/4 -translate-y-1/4 group-hover:scale-110 transition-transform duration-500"><i data-lucide="zap" class="w-64 h-64"></i></div><div class="relative z-10 flex flex-col sm:flex-row items-center justify-between gap-6"><div><h3 class="text-2xl sm:text-3xl font-black tracking-tight mb-2">Chcete se otestovat nanečisto?</h3><p class="text-indigo-100 font-medium text-sm sm:text-base">Vyzkoušejte novou aplikaci pro interaktivní procvičování všech otázek.</p></div><div class="flex items-center gap-3 bg-white/20 px-6 py-3 rounded-xl font-bold backdrop-blur-sm group-hover:bg-white/30 transition text-sm sm:text-base whitespace-nowrap">Spustit procvičování <i data-lucide="arrow-right" class="w-5 h-5"></i></div></div></a><div class="grid grid-cols-1 sm:grid-cols-3 gap-4 sm:gap-8 mb-8"><div class="bg-white p-6 sm:p-8 rounded-2xl sm:rounded-[32px] border border-slate-200 shadow-sm"><div class="text-blue-600 mb-2 font-black text-3xl sm:text-5xl tracking-tighter">{metadata["total_questions"]}</div><div class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Unikátních otázek</div></div><div class="bg-white p-6 sm:p-8 rounded-2xl sm:rounded-[32px] border border-slate-200 shadow-sm"><div class="text-emerald-600 mb-2 font-black text-3xl sm:text-5xl tracking-tighter">{metadata["total_tests"]}</div><div class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Analyzovaných testů</div></div><div class="bg-white p-6 sm:p-8 rounded-2xl sm:rounded-[32px] border border-slate-200 shadow-sm"><div class="text-purple-600 mb-2 font-black text-xl sm:text-2xl tracking-tight">{db_max_str}</div><div class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Poslední data</div></div></div><div class="grid grid-cols-1 lg:grid-cols-2 gap-8"><div class="bg-white p-6 sm:p-10 rounded-2xl sm:rounded-[40px] border border-slate-200 shadow-sm transition hover:shadow-xl"><h3 class="font-bold mb-6 text-slate-400 uppercase text-[10px] tracking-widest text-center">Distribuce kategorií</h3><div class="h-[300px] sm:h-[450px]"><canvas id="catChart"></canvas></div></div><div class="bg-white p-6 sm:p-10 rounded-2xl sm:rounded-[40px] border border-slate-200 shadow-sm transition hover:shadow-xl"><h3 class="font-bold mb-6 text-slate-400 uppercase text-[10px] tracking-widest text-center">Počty v modulech</h3><div class="h-[300px] sm:h-[450px]"><canvas id="testChart"></canvas></div></div></div></main><script>lucide.createIcons();new Chart(document.getElementById("catChart"),{{type:"doughnut",data:{{labels:{list(metadata["breakdown_by_category"].keys())},datasets:[{{data:{list(metadata["breakdown_by_category"].values())},backgroundColor:["#2563eb","#10b981","#f59e0b","#ef4444","#8b5cf6","#ec4899","#06b6d4","#f97316","#6366f1","#14b8a6"],borderWidth:0,hoverOffset:30}}]}},options:{{responsive:true,maintainAspectRatio:false,cutout:"75%",plugins:{{legend:{{display:false}}}}}}}});new Chart(document.getElementById("testChart"),{{type:"bar",data:{{labels:{list(metadata["breakdown_by_test_type"].keys())},datasets:[{{label:"Otázek",data:{list(metadata["breakdown_by_test_type"].values())},backgroundColor:"#3b82f6",borderRadius:8}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:"y",plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{display:false}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:9,weight:"600"}}}}}}}}}}}});</script></body></html>')

    # --- TEST PAGES ---
    for test_name, _ in sorted_tests:
        folder_name = test_name.replace(" ", "_"); test_path = os.path.join(output_dir, folder_name)
        for mode in ["all", "real"]:
            where_clause = "WHERE t.test_type = %s"
            if mode == "real": where_clause += " AND t.is_practice = FALSE"
            cursor.execute(f"SELECT q.id, q.text as question_text, string_agg(t.test_date::text, ',') as seen_dates FROM questions q JOIN test_questions tq ON q.id = tq.question_id JOIN tests t ON tq.test_id = t.id {where_clause} GROUP BY q.id, q.text", (test_name,))

            deduped = {}
            for row in cursor:
                if row['seen_dates']:
                    tmp = row['seen_dates']
                else:
                    tmp = ""
                txt, dates = row['question_text'], tmp.split(',')
                if txt not in deduped: deduped[txt] = {"dates": dates, "count": len(dates)}
                else: deduped[txt]["dates"].extend(dates); deduped[txt]["count"] += len(dates)
            if not deduped and mode == "real": continue
            
            with open(os.path.join(test_path, "unique_questions.json"), "r", encoding="utf-8") as f: json_questions = {q['question_text']: q for q in json.load(f)}
            final_list = []
            for txt, data in deduped.items():
                json_data = json_questions.get(txt, {})
                final_list.append({"question_text": txt, "category": json_data.get("category", "Nezařazeno"), "options": json_data.get("options", {}), "correct_option": json_data.get("correct_option", ""), "explanation": json_data.get("explanation", ""), "occurrence_count": data["count"], "seen_dates": data["dates"]})
            final_list.sort(key=lambda x: x['occurrence_count'], reverse=True)
            top_20 = final_list[:20]
            num_pages = (len(final_list) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            
            for p in range(num_pages):
                page_questions = final_list[p*ITEMS_PER_PAGE : (p+1)*ITEMS_PER_PAGE]
                sidebar = get_sidebar(sorted_tests, "../", test_name)
                filename = (f"index.html" if mode == "all" and p == 0 else f"real.html" if mode == "real" and p == 0 else f"{mode}_page{p+1}.html")
                all_active = "bg-blue-600 text-white shadow-lg shadow-blue-500/20" if mode == "all" else "bg-white text-slate-400 border border-slate-200 hover:bg-slate-50"
                real_active = "bg-emerald-600 text-white shadow-lg shadow-emerald-500/20" if mode == "real" else "bg-white text-slate-400 border border-slate-200 hover:bg-slate-50"
                
                toggle_html = f'<div class="flex flex-col sm:flex-row gap-2 sm:gap-4 mb-8"><a href="index.html" class="px-4 py-3 rounded-xl font-bold text-xs uppercase tracking-widest transition {all_active} flex items-center justify-center gap-2"><i data-lucide="layers" class="w-4 h-4"></i> Všechny výskyty</a><div class="tooltip w-full sm:w-auto"><a href="real.html" class="w-full px-4 py-3 rounded-xl font-bold text-xs uppercase tracking-widest transition {real_active} flex items-center justify-center gap-2"><i data-lucide="shield-check" class="w-4 h-4"></i> Pouze reálné zkoušky</a><span class="tooltiptext">Pouze otázky z ostrých testů z oficiálních zkoušek.</span></div></div>'
                downloads = f"""<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 sm:gap-6 mb-8">
                    <a href="QUESTIONS.pdf" class="flex items-center gap-4 bg-white p-4 sm:p-6 rounded-2xl border border-slate-200 hover:shadow-lg transition group shadow-sm"><div class="p-3 bg-blue-50 text-blue-600 rounded-xl group-hover:bg-blue-600 group-hover:text-white transition shadow-sm"><i data-lucide="file-text" class="w-5 h-5"></i></div><div><div class="font-black text-slate-800 text-sm tracking-tight">PDF Seznam</div></div></a>
                    <a href="QUESTIONS_WITH_ANS.pdf" class="flex items-center gap-4 bg-white p-4 sm:p-6 rounded-2xl border border-slate-200 hover:shadow-lg transition group shadow-sm"><div class="p-3 bg-emerald-50 text-emerald-600 rounded-xl group-hover:bg-emerald-600 group-hover:text-white transition shadow-sm"><i data-lucide="check-square" class="w-5 h-5"></i></div><div><div class="font-black text-slate-800 text-sm tracking-tight">PDF Klíč</div></div></a>
                    <a href="unique_questions.json" class="flex items-center gap-4 bg-white p-4 sm:p-6 rounded-2xl border border-slate-200 hover:shadow-lg transition group shadow-sm"><div class="p-3 bg-amber-50 text-amber-600 rounded-xl group-hover:bg-amber-600 group-hover:text-white transition shadow-sm"><i data-lucide="database" class="w-5 h-5"></i></div><div><div class="font-black text-slate-800 text-sm tracking-tight">JSON Data</div></div></a>
                </div><div class="bg-white p-4 sm:p-10 rounded-2xl sm:rounded-[40px] border border-slate-200 shadow-sm mb-8 transition hover:shadow-xl"><h3 class="font-bold mb-6 text-slate-400 uppercase text-[10px] tracking-widest text-center">Top 20 nejčastějších otázek</h3><div class="h-[300px] sm:h-[400px]"><canvas id="topChart"></canvas></div></div>""" if p == 0 else ""
                
                p_btns = [f'<a href="{"index.html" if mode=="all" and pi==0 else "real.html" if mode=="real" and pi==0 else f"{mode}_page{pi+1}.html"}" class="px-4 py-2 rounded-lg text-[10px] font-black border {("bg-blue-600 text-white shadow-xl" if mode=="all" else "bg-emerald-600 text-white shadow-xl") if pi == p else "bg-white text-slate-500 hover:bg-slate-50 border-slate-200"}">{pi+1}</a>' for pi in range(num_pages)]
                pagin = f'<div class="flex flex-wrap gap-1.5 mb-8">{"".join(p_btns)}</div>'
                tl_color = "#3b82f6" if mode == "all" else "#10b981"
                
                cards_html = ""
                for q in page_questions:
                    txt_esc = html.escape(q["question_text"])
                    opts = "".join([f'<div class="p-4 sm:p-5 rounded-xl sm:rounded-[24px] border flex items-start gap-3 sm:gap-4 {("bg-emerald-50 border-emerald-200 ring-2 ring-emerald-500/5") if ok.split("_")[1].upper() == q["correct_option"] else "bg-slate-50 border-slate-100"}"><div class="w-7 h-7 sm:w-8 sm:h-8 shrink-0 rounded-full flex items-center justify-center font-black text-xs sm:text-sm {("bg-emerald-500 text-white shadow-lg") if ok.split("_")[1].upper() == q["correct_option"] else "bg-slate-200 text-slate-500"}">{ok.split("_")[1].upper()}</div><span class="pt-0.5 sm:pt-1.5 font-bold text-sm sm:text-lg leading-relaxed {("text-emerald-900") if ok.split("_")[1].upper() == q["correct_option"] else "text-slate-600"}">{html.escape(ot or "")}</span></div>' for ok, ot in q["options"].items()])
                    exp = f'<div class="mt-6 sm:mt-8 p-6 sm:p-8 bg-slate-900 rounded-2xl sm:rounded-[32px] shadow-xl relative overflow-hidden text-sm sm:text-base"><div class="text-blue-400 font-black text-[10px] uppercase mb-3 tracking-widest flex items-center gap-2"><i data-lucide="info" class="w-3 h-3"></i> Vysvětlení</div><p class="text-slate-300 leading-relaxed">{html.escape(q["explanation"])}</p></div>' if q["explanation"] else ""
                    tl = get_timeline_svg(q["seen_dates"], db_min, db_max, tl_color)
                    cards_html += f'<div class="bg-white rounded-2xl sm:rounded-[44px] border border-slate-200 p-6 sm:p-10 shadow-sm hover:shadow-xl transition-all duration-300" x-show="search === \'\' || `{txt_esc.lower()}`.includes(search.toLowerCase())"><div class="flex justify-between items-center mb-6 sm:mb-8"><span class="bg-slate-100 text-slate-500 text-[9px] font-black px-3 py-1.5 rounded-lg border border-slate-200 uppercase tracking-widest">{q["category"]}</span><div class="text-[9px] font-black text-slate-400 uppercase tracking-widest bg-slate-50 px-2 py-1.5 rounded-lg border border-slate-100">Četnost: <span class="{"text-blue-600" if mode=="all" else "text-emerald-600"} font-black text-xs">{q["occurrence_count"]}</span></div></div><h4 class="text-lg sm:text-2xl font-black text-slate-800 mb-8 sm:mb-10 leading-tight tracking-tight">{txt_esc}</h4><div class="grid grid-cols-1 gap-3 sm:gap-4">{opts}</div>{exp}{tl}</div>'
                
                chart_init = f"<script>new Chart(document.getElementById('topChart'),{{type:'bar',data:{{labels:{[q['question_text'][:35]+'...' for q in top_20]},datasets:[{{label:'Četnost',data:{[q['occurrence_count'] for q in top_20]},backgroundColor:'{tl_color}',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{display:false}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:8,weight:'600'}}}}}}}}}}}});</script>" if p == 0 else ""
                with open(os.path.join(test_path, filename), "w", encoding="utf-8") as f:
                    f.write(f'<!DOCTYPE html><html lang="cs"><head>{COMMON_HEAD}<title>{test_name}</title></head><body class="bg-slate-50 text-slate-900 font-sans flex flex-col lg:flex-row min-h-screen" x-data="{{ search: \'\', mobileMenu: false }}">{sidebar}<main class="flex-1 p-4 sm:p-10 overflow-y-auto bg-slate-50"><h2 class="text-2xl sm:text-4xl font-black text-slate-800 tracking-tighter mb-8 uppercase italic tracking-tight">{test_name}</h2>{toggle_html}{downloads}<div class="sticky top-0 lg:top-0 z-30 py-4 sm:py-6 bg-slate-50/90 backdrop-blur-md mb-8 border-b border-slate-200/50"><div class="relative"><i data-lucide="search" class="absolute left-5 top-1/2 -translate-y-1/2 text-slate-400 w-5 h-5"></i><input type="text" x-model="search" placeholder="Hledej v textu otázky..." class="w-full pl-12 pr-4 py-4 rounded-xl sm:rounded-[32px] border border-slate-200 shadow-lg shadow-slate-200/40 outline-none focus:ring-4 focus:ring-blue-100 font-bold text-base sm:text-xl tracking-tight"></div></div>{pagin}<div class="grid grid-cols-1 gap-6 sm:gap-12 pb-24">{cards_html}</div>{pagin}</main><script>lucide.createIcons();</script>{chart_init}</body></html>')
        print(f"✅ {test_name}")

if __name__ == "__main__": deploy_site()
