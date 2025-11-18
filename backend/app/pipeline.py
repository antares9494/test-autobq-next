"""
import os
import re
import tempfile
import uuid

import camelot
import pandas as pd

REGLER_PATH = os.path.join(os.path.dirname(__file__), "bdd_criteres.xlsx")

def extraire_tables_en_excel(pdf_path: str) -> pd.DataFrame:
    tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream")
    if tables.n == 0:
        return pd.DataFrame()
    dfs = [t.df for t in tables]
    return pd.concat(dfs, ignore_index=True)

def clean_cell(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    return re.sub(r"\s+", " ", s)

DATE_PATTERN = re.compile(r"^(?P<d>\d{1,2})[./-](?P<m>\d{1,2})([./-](?P<y>\d{2,4}))?$")

def is_date_like(s: str) -> bool:
    s = clean_cell(s)
    if not s:
        return False
    m = DATE_PATTERN.match(s)
    if not m:
        return False
    try:
        d, mo = int(m.group('d')), int(m.group('m'))
        return 1 <= d <= 31 and 1 <= mo <= 12
    except Exception:
        return False

NUMERIC_CHARS_PATTERN = re.compile(r"^[+\-]?[0-9\s.,]+$")

def _strip_currency(s: str) -> str:
    s = s.replace('€', '')
    s = re.sub(r"\bEUR\b", '', s, flags=re.IGNORECASE)
    return s.strip()

def parse_amount(s):
    s_orig = clean_cell(s)
    if not s_orig:
        return None, None
    s = _strip_currency(s_orig)
    if not NUMERIC_CHARS_PATTERN.match(s):
        return None, None
    s = s.replace(' ', '')
    if '.' in s and ',' in s:
        s = s.replace('.', '')
        s = s.replace(',', '.', 1)
    elif ',' in s:
        s = s.replace(',', '.')
    if s.count('.') > 1:
        parts = s.split('.');
        s = ''.join(parts[:-1]) + '.' + parts[-1]
    try:
        v = float(s)
    except ValueError:
        return None, None
    rep_norm = f"{v:.2f}".replace('.', ',')
    return v, rep_norm

def is_amount_like(s: str) -> bool:
    v, _ = parse_amount(s)
    return v is not None

def nettoyer_releve_sg(df: pd.DataFrame) -> pd.DataFrame:
    df = df.iloc[:, :5].copy()
    to_drop = [False] * len(df)
    last_main_idx = None
    for idx in range(len(df)):
        row = df.iloc[idx]
        date_cell = clean_cell(row.iloc[0])
        val_cell = clean_cell(row.iloc[1])
        lib_cell = clean_cell(row.iloc[2])
        deb_cell = clean_cell(row.iloc[3])
        cred_cell = clean_cell(row.iloc[4])
        has_date = is_date_like(date_cell) or is_date_like(val_cell)
        has_amount = is_amount_like(deb_cell) or is_amount_like(cred_cell)
        has_label_text = lib_cell != ''
        if has_date and has_amount and has_label_text:
            last_main_idx = idx
            continue
        is_continuation = not has_date and not has_amount and has_label_text and last_main_idx is not None
        if is_continuation:
            old_lib = clean_cell(df.iloc[last_main_idx, 2])
            new_lib = (old_lib + ' ' + lib_cell).strip() if old_lib else lib_cell
            df.iloc[last_main_idx, 2] = new_lib
            to_drop[idx] = True
    df = df.loc[[not d for d in to_drop]].reset_index(drop=True)
    df.drop(columns=df.columns[1], inplace=True)
    keep_mask = []
    for idx in range(len(df)):
        row = df.iloc[idx]
        date_cell = clean_cell(row.iloc[0])
        lib_cell = clean_cell(row.iloc[1])
        deb_cell = clean_cell(row.iloc[2])
        cred_cell = clean_cell(row.iloc[3])
        has_date = is_date_like(date_cell)
        has_amount = is_amount_like(deb_cell) or is_amount_like(cred_cell)
        has_label_text = lib_cell != ''
        keep_mask.append(has_date and has_amount and has_label_text)
    df_clean = df.loc[keep_mask].reset_index(drop=True)
    for idx in range(len(df_clean)):
        for col in [2,3]:
            raw = df_clean.iat[idx, col]
            _, rep_norm = parse_amount(raw)
            df_clean.iat[idx, col] = rep_norm if rep_norm is not None else ''
    return df_clean

DF_EXPR = None
DF_KEYWORDS = None

def init_rules(path: str = None):
    global DF_EXPR, DF_KEYWORDS
    path = path or REGLER_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(f"Fichier de règles introuvable : {path}")
    df_expr = pd.read_excel(path, sheet_name='expressions')
    df_keywords = pd.read_excel(path, sheet_name='mots_cles')
    df_expr.columns = [c.strip().lower() for c in df_expr.columns]
    df_keywords.columns = [c.strip().lower() for c in df_keywords.columns]
    df_expr['regex'] = df_expr['regex'].astype(bool)
    df_expr['en_debut'] = df_expr['en_debut'].astype(bool)
    DF_EXPR, DF_KEYWORDS = df_expr, df_keywords

def nettoyer_libelle(libelle_brut: object) -> str:
    if DF_EXPR is None or DF_KEYWORDS is None:
        raise RuntimeError('Règles non initialisées : appelez init_rules(path) avant.')
    if libelle_brut is None or (isinstance(libelle_brut, float) and pd.isna(libelle_brut)):
        return ''
    libelle_original = re.sub(r"\s+", ' ', str(libelle_brut).strip())
    if libelle_original == '':
        return ''
    libelle = libelle_original
    correspondance = False
    for _, row in DF_EXPR.iterrows():
        expr = str(row['expression']).strip()
        remplacement = str(row['remplacer_par']) if not pd.isna(row['remplacer_par']) else ''
        is_regex = bool(row['regex'])
        en_debut = bool(row['en_debut'])
        if not expr:
            continue
        if is_regex:
            pattern = expr if not en_debut else rf'^{expr}'
            new_libelle, n = re.subn(pattern, remplacement, libelle, flags=re.IGNORECASE)
            if n > 0:
                libelle = new_libelle.strip()
                correspondance = True
        else:
            lib_up = libelle.upper()
            expr_up = expr.upper()
            if en_debut and lib_up.startswith(expr_up):
                if remplacement:
                    reste = libelle[len(expr):].lstrip()
                    libelle = (remplacement + ' ' + reste).strip()
                else:
                    libelle = libelle[len(expr):].lstrip()
                correspondance = True
            elif expr_up in lib_up:
                pattern = re.compile(re.escape(expr), flags=re.IGNORECASE)
                libelle = pattern.sub(remplacement if remplacement else '', libelle)
                libelle = re.sub(r"\s+", ' ', libelle).strip()
                correspondance = True
    lib_up = libelle.upper()
    for _, row in DF_KEYWORDS.iterrows():
        mot_cle = str(row['mot_cle']).strip()
        remplacement = str(row['remplacer_par']).strip() if not pd.isna(row['remplacer_par']) else ''
        if mot_cle and mot_cle.upper() in lib_up:
            return (remplacement.upper() if remplacement else mot_cle.upper())
    if correspondance:
        return libelle.upper()
    return libelle_original

def nettoyer_libelles_dataframe(df_in: pd.DataFrame) -> pd.DataFrame:
    if df_in.shape[1] < 4:
        raise ValueError('Le DataFrame doit contenir au moins 4 colonnes (Date, Libellé, Débit, Crédit).')
    df = df_in.iloc[:, :4].copy()
    df.columns = ['Date', 'Libellé brut', 'Débit', 'Crédit']
    df['Libellé nettoyé'] = df['Libellé brut'].apply(nettoyer_libelle)
    return df[['Date', 'Libellé nettoyé', 'Débit', 'Crédit']].copy()

def to_number(x):
    if pd.isna(x):
        return None
    if isinstance(x, str):
        s = x.strip().replace('\xa0','').replace(' ','').replace(',', '.')
        if s == '':
            return None
        try:
            return float(s)
        except:
            return None
    try:
        return float(x)
    except:
        return None

def make_output_name(input_name: str) -> str:
    base = os.path.splitext(os.path.basename(input_name))[0]
    base = re.sub(r'_RAW.*$', '', base, flags=re.IGNORECASE)
    return f"{base}_ecritures.xlsx"

AMOUNT_RE = r'([\d]+(?:[.,]\d{1,2})?)\s*(?:€|E)?'
BRUT_RE = re.compile(r'BRUT\s*' + AMOUNT_RE, re.IGNORECASE)
COM_RE = re.compile(r'(?:COM|COMMISSION)\s*' + AMOUNT_RE, re.IGNORECASE)

def _to_float_strict(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace('\xa0','').replace(' ','').replace(',', '.')
    try:
        return float(s)
    except:
        return None

def _extract_amount(pattern, text):
    m = pattern.search(text)
    if not m:
        return None
    return _to_float_strict(m.group(1))

def parse_remise_cb(libelle: str):
    if not isinstance(libelle, str):
        return None
    up = libelle.upper()
    if 'REMISE CB' not in up or 'BRUT' not in up or not ('COM' in up or 'COMMISSION' in up):
        return None
    brut = _extract_amount(BRUT_RE, libelle)
    com = _extract_amount(COM_RE, libelle)
    if brut is None or com is None:
        return None
    net = brut - com
    if ' DU ' in up or up.startswith('DU ') or up.endswith(' DU'):
        core = libelle.split('DU')[0].strip()
    elif 'BRUT' in up:
        core = libelle.split('BRUT')[0].strip()
    else:
        core = libelle.strip()
    return (core, brut, com, net)

def validate_accounts(acc471_in: str, acc512_in: str):
    acc471 = acc471_in.strip() if acc471_in else ''
    acc512 = acc512_in.strip() if acc512_in else ''
    if acc471 == '':
        acc471 = '471000'
    if acc512 == '':
        acc512 = '512000'
    if not re.fullmatch(r'47\d{4}', acc471):
        acc471 = '471000'
    if not re.fullmatch(r'512\d{3}', acc512):
        acc512 = '512000'
    return int(acc471), int(acc512)

def run_full_pipeline(uploaded_file_bytes: bytes, original_name: str, rules_path: str = None, compte471_input: str = '', compte512_input: str = ''):
    if rules_path:
        init_rules(rules_path)
    else:
        init_rules(REGLER_PATH)
    compte471, compte512 = validate_accounts(compte471_input, compte512_input)
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
        tmp.write(uploaded_file_bytes)
        tmp_path = tmp.name
    df_camelot = extraire_tables_en_excel(tmp_path)
    if df_camelot.empty:
        raise RuntimeError('Aucun tableau détecté dans le PDF (Camelot).')
    df_sg = nettoyer_releve_sg(df_camelot)
    df_labels = nettoyer_libelles_dataframe(df_sg)
    out_rows = []
    for _, row in df_labels.iterrows():
        date = row['Date']
        lib = '' if pd.isna(row['Libellé nettoyé']) else str(row['Libellé nettoyé'])
        debit = to_number(row['Débit'])
        credit = to_number(row['Crédit'])
        if debit is not None and abs(debit) < 1e-12:
            debit = None
        if credit is not None and abs(credit) < 1e-12:
            credit = None
        parsed = parse_remise_cb(lib)
        if parsed:
            core, brut, frais, net = parsed
            out_rows.append([date, None, compte471, core, None, None, brut])
            out_rows.append([date, None, 627000, f'FRAIS : {core}', None, frais, None])
            out_rows.append([date, None, compte512, core, None, net, None])
            continue
        if debit is not None and credit is None:
            out_rows.append([date, None, compte471, lib, None, debit, None])
            out_rows.append([date, None, compte512, lib, None, None, debit])
        elif credit is not None and debit is None:
            out_rows.append([date, None, compte471, lib, None, None, credit])
            out_rows.append([date, None, compte512, lib, None, credit, None])
    out_df = pd.DataFrame(out_rows, columns=['Date','Pièce','Compte','Libellé','Journal','Débit','Crédit'])
    out_name = make_output_name(original_name)
    tmp_dir = tempfile.gettempdir()
    unique = f"{uuid.uuid4().hex}_{out_name}"
    out_path = os.path.join(tmp_dir, unique)
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        out_df.to_excel(writer, index=False, sheet_name='BQ')
    preview = out_df.head(30).to_dict(orient='records')
    return {'preview': preview, 'file_path': out_path, 'n_rows': len(out_df)}
"""