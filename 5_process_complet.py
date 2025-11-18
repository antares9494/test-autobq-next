
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
5_process_complet.py

Pipeline complet Streamlit :
1) Upload d'un relevé PDF (Société Générale, PDF natif)
2) Extraction des tableaux avec Camelot
3) Nettoyage spécifique SG (regroupement libellés, filtrage des lignes, normalisation montants)
4) Nettoyage des libellés avec règles bdd_criteres.xlsx
5) Conversion en écritures comptables ACD (471 / 512 + gestion REMISE CB)
"""

import io
import os
import re
import tempfile

import camelot
import pandas as pd
import streamlit as st
from io import BytesIO

# =====================================================================
# CONFIG STREAMLIT
# =====================================================================

st.set_page_config(page_title="Processus complet SG → ACD", layout="wide")
st.title("🔁 Processus complet : PDF SG → Écritures ACD")
st.caption(
    "Pipeline : PDF natif Société Générale → Camelot → Nettoyage SG → Nettoyage libellés → Écritures ACD."
)

# =====================================================================
# 1) PARTIE 1_CAMELOT.PY : extraction tableaux PDF
# =====================================================================

def extraire_tables_en_excel(pdf_path: str) -> pd.DataFrame:
    """
    Extraction des tableaux d'un PDF natif avec Camelot (mode stream uniquement)
    et fusion de tous les tableaux dans un DataFrame unique.
    """
    tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream")

    if tables.n == 0:
        return pd.DataFrame()

    dfs = []
    for table in tables:
        dfs.append(table.df)

    df_final = pd.concat(dfs, ignore_index=True)
    return df_final


# =====================================================================
# 2) PARTIE 2_PT-SG-CAMELOT.PY : nettoyage spécifique Société Générale
# =====================================================================

def clean_cell(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s


DATE_PATTERN = re.compile(
    r"""^
    (?P<d>\d{1,2})
    [./-]
    (?P<m>\d{1,2})
    (
        [./-]
        (?P<y>\d{2,4})
    )?
    $
    """,
    re.VERBOSE,
)

def is_date_like(s: str) -> bool:
    s = clean_cell(s)
    if not s:
        return False
    m = DATE_PATTERN.match(s)
    if not m:
        return False
    try:
        d = int(m.group("d"))
        mo = int(m.group("m"))
        if not (1 <= d <= 31 and 1 <= mo <= 12):
            return False
    except Exception:
        return False
    return True


NUMERIC_CHARS_PATTERN = re.compile(r"^[+\-]?[0-9\s.,]+$")

def _strip_currency(s: str) -> str:
    s = s.replace("€", "")
    s = re.sub(r"\bEUR\b", "", s, flags=re.IGNORECASE)
    return s.strip()

def parse_amount(s):
    s_orig = clean_cell(s)
    if not s_orig:
        return None, None

    s = _strip_currency(s_orig)

    if not NUMERIC_CHARS_PATTERN.match(s):
        return None, None

    s = s.replace(" ", "")  # espaces milliers

    if "." in s and "," in s:
        # suppression des séparateurs milliers ".", et on garde la virgule comme décimale
        s = s.replace(".", "")
        s = s.replace(",", ".", 1)
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]

    try:
        value = float(s)
    except ValueError:
        return None, None

    rep_norm = f"{value:.2f}".replace(".", ",")
    return value, rep_norm

def is_amount_like(s: str) -> bool:
    v, _ = parse_amount(s)
    return v is not None


def nettoyer_releve_sg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie un DataFrame issu de Camelot pour un relevé Société Générale
    (Structure : 0=Date, 1=Valeur, 2=Nature, 3=Débit, 4=Crédit).

    Pipeline :
      1) Regrouper les libellés multiples
      2) Supprimer la colonne 'Valeur' (colonne 1)
      3) Supprimer toutes les lignes non-transactions
      4) Normaliser les montants "1934,93"
    """

    # On ne garde que les 5 premières colonnes
    df = df.iloc[:, :5].copy()

    # ---------- Étape 1 : regroupement des libellés multiples ----------
    to_drop = [False] * len(df)
    last_main_idx = None

    for idx in range(len(df)):
        row = df.iloc[idx]  # row est une Series

        # IMPORTANT : indexation POSITIONNELLE avec .iloc
        date_cell = clean_cell(row.iloc[0])
        val_cell  = clean_cell(row.iloc[1])
        lib_cell  = clean_cell(row.iloc[2])
        deb_cell  = clean_cell(row.iloc[3])
        cred_cell = clean_cell(row.iloc[4])

        has_date = is_date_like(date_cell) or is_date_like(val_cell)
        has_amount = is_amount_like(deb_cell) or is_amount_like(cred_cell)
        has_label_text = lib_cell != ""

        if has_date and has_amount and has_label_text:
            # Nouvelle transaction principale
            last_main_idx = idx
            continue

        is_continuation = (
            not has_date
            and not has_amount
            and has_label_text
            and last_main_idx is not None
        )

        if is_continuation:
            old_lib = clean_cell(df.iloc[last_main_idx, 2])
            new_lib = (old_lib + " " + lib_cell).strip() if old_lib else lib_cell
            df.iloc[last_main_idx, 2] = new_lib
            to_drop[idx] = True

    df = df.loc[[not d for d in to_drop]].reset_index(drop=True)

    # ---------- Étape 2 : suppression de la colonne Valeur ----------
    df.drop(columns=df.columns[1], inplace=True)
    # Maintenant, par position :
    # col 0 = Date, col 1 = Nature, col 2 = Débit, col 3 = Crédit

    # ---------- Étape 3 : suppression des lignes non-transactions ----------
    keep_mask = []

    for idx in range(len(df)):
        row = df.iloc[idx]

        date_cell = clean_cell(row.iloc[0])
        lib_cell  = clean_cell(row.iloc[1])
        deb_cell  = clean_cell(row.iloc[2])
        cred_cell = clean_cell(row.iloc[3])

        has_date = is_date_like(date_cell)
        has_amount = is_amount_like(deb_cell) or is_amount_like(cred_cell)
        has_label_text = lib_cell != ""

        is_transaction = has_date and has_amount and has_label_text
        keep_mask.append(is_transaction)

    df_clean = df.loc[keep_mask].reset_index(drop=True)

    # ---------- Étape 4 : normalisation des montants Débit / Crédit ----------
    for idx in range(len(df_clean)):
        for col in [2, 3]:  # 2=Débit, 3=Crédit (toujours par position)
            raw = df_clean.iat[idx, col]
            _, rep_norm = parse_amount(raw)
            df_clean.iat[idx, col] = rep_norm if rep_norm is not None else ""

    return df_clean


# =====================================================================
# 3) PARTIE 3_CLEANER.PY : nettoyage des libellés via bdd_criteres.xlsx
# =====================================================================

REGLER_PATH = os.path.join(os.path.dirname(__file__), "bdd_criteres.xlsx")

@st.cache_data(show_spinner=False)
def load_rules(path: str):
    """
    Charge les règles de bdd_criteres.xlsx.
    Onglets attendus :
      - expressions : expression, remplacer_par, regex (bool), en_debut (bool)
      - mots_cles   : mot_cle, remplacer_par
    """
    if not os.path.exists(path):
        st.error(f"Fichier de règles introuvable : {path}")
        st.stop()

    try:
        df_expr = pd.read_excel(path, sheet_name="expressions")
        df_keywords = pd.read_excel(path, sheet_name="mots_cles")
    except Exception as e:
        st.error("Erreur lors du chargement de bdd_criteres.xlsx")
        st.exception(e)
        st.stop()

    # Normaliser les noms de colonnes
    df_expr.columns = [c.strip().lower() for c in df_expr.columns]
    df_keywords.columns = [c.strip().lower() for c in df_keywords.columns]

    required_expr_cols = {"expression", "remplacer_par", "regex", "en_debut"}
    required_kw_cols = {"mot_cle", "remplacer_par"}

    if not required_expr_cols.issubset(df_expr.columns):
        st.error(
            f"Onglet 'expressions' invalide. Colonnes requises : {required_expr_cols}"
        )
        st.stop()

    if not required_kw_cols.issubset(df_keywords.columns):
        st.error(
            f"Onglet 'mots_cles' invalide. Colonnes requises : {required_kw_cols}"
        )
        st.stop()

    df_expr["regex"] = df_expr["regex"].astype(bool)
    df_expr["en_debut"] = df_expr["en_debut"].astype(bool)

    return df_expr, df_keywords


DF_EXPR, DF_KEYWORDS = load_rules(REGLER_PATH)

def nettoyer_libelle(libelle_brut: object) -> str:
    """
    Applique les règles de bdd_criteres.xlsx à un libellé :
      1) expressions (regex / texte simple, début ou n'importe où)
      2) mots_cles (si trouvé, remplace tout le libellé)

    Logique :
      - Si aucune règle ne s'applique, on renvoie le libellé d'origine (nettoyé).
    """
    if libelle_brut is None or (isinstance(libelle_brut, float) and pd.isna(libelle_brut)):
        return ""

    libelle_original = str(libelle_brut).strip()
    libelle_original = re.sub(r"\s+", " ", libelle_original)

    if libelle_original == "":
        return ""

    libelle = libelle_original
    correspondance = False

    # Règles "expressions"
    for _, row in DF_EXPR.iterrows():
        expr = str(row["expression"]).strip()
        remplacement = str(row["remplacer_par"]) if not pd.isna(row["remplacer_par"]) else ""
        is_regex = bool(row["regex"])
        en_debut = bool(row["en_debut"])

        if not expr:
            continue

        if is_regex:
            pattern = expr if not en_debut else rf"^{expr}"
            new_libelle, n = re.subn(pattern, remplacement, libelle, flags=re.IGNORECASE)
            if n > 0:
                libelle = new_libelle.strip()
                correspondance = True
        else:
            lib_up = libelle.upper()
            expr_up = expr.upper()

            if en_debut:
                if lib_up.startswith(expr_up):
                    if remplacement:
                        reste = libelle[len(expr):].lstrip()
                        libelle = (remplacement + " " + reste).strip()
                    else:
                        libelle = libelle[len(expr):].lstrip()
                    correspondance = True
            else:
                if expr_up in lib_up:
                    if remplacement:
                        pattern = re.compile(re.escape(expr), flags=re.IGNORECASE)
                        libelle = pattern.sub(remplacement, libelle)
                    else:
                        pattern = re.compile(re.escape(expr), flags=re.IGNORECASE)
                        libelle = pattern.sub("", libelle)
                    libelle = re.sub(r"\s+", " ", libelle).strip()
                    correspondance = True

    # Règles "mots_cles" (priorité forte)
    lib_up = libelle.upper()
    for _, row in DF_KEYWORDS.iterrows():
        mot_cle = str(row["mot_cle"]).strip()
        remplacement = str(row["remplacer_par"]).strip() if not pd.isna(row["remplacer_par"]) else ""
        if not mot_cle:
            continue

        if mot_cle.upper() in lib_up:
            return remplacement.upper() if remplacement else mot_cle.upper()

    if correspondance:
        return libelle.upper()

    # Aucune règle : garder libellé original nettoyé
    return libelle_original


def nettoyer_libelles_dataframe(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    df_in : DataFrame à 4 colonnes :
      0 : Date
      1 : Libellé brut
      2 : Débit
      3 : Crédit

    Retourne un DataFrame :
      Date, Libellé nettoyé, Débit, Crédit
    """
    if df_in.shape[1] < 4:
        raise ValueError("Le DataFrame doit contenir au moins 4 colonnes (Date, Libellé, Débit, Crédit).")

    df = df_in.iloc[:, :4].copy()
    df.columns = ["Date", "Libellé brut", "Débit", "Crédit"]
    df["Libellé nettoyé"] = df["Libellé brut"].apply(nettoyer_libelle)
    df_out = df[["Date", "Libellé nettoyé", "Débit", "Crédit"]].copy()
    return df_out


# =====================================================================
# 4) PARTIE 4_SCRIPTOR-V6.PY : conversion en écritures ACD
# =====================================================================

def to_number(x):
    if pd.isna(x):
        return None
    if isinstance(x, str):
        s = x.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        if s == "":
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
BRUT_RE   = re.compile(r'BRUT\s*' + AMOUNT_RE, re.IGNORECASE)
COM_RE    = re.compile(r'(?:COM|COMMISSION)\s*' + AMOUNT_RE, re.IGNORECASE)

def _to_float_strict(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace('\xa0', '').replace(' ', '').replace(',', '.')
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
    com  = _extract_amount(COM_RE, libelle)
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
    acc471 = acc471_in.strip()
    acc512 = acc512_in.strip()
    if acc471 == "":
        acc471 = "471000"
    if acc512 == "":
        acc512 = "512000"
    if not re.fullmatch(r'47\d{4}', acc471):
        st.warning("Le compte 471 doit comporter 6 chiffres et commencer par '47'. Valeur par défaut utilisée (471000).")
        acc471 = "471000"
    if not re.fullmatch(r'512\d{3}', acc512):
        st.warning("Le compte 512 doit comporter 6 chiffres et commencer par '512'. Valeur par défaut utilisée (512000).")
        acc512 = "512000"
    return int(acc471), int(acc512)


# =====================================================================
# INTERFACE STREAMLIT : PIPELINE COMPLET
# =====================================================================

uploaded_pdf = st.file_uploader(
    "Dépose ton relevé Société Générale (PDF natif, non scanné)",
    type=["pdf"],
    accept_multiple_files=False,
)

with st.expander("⚙️ Comptes comptables (optionnel)"):
    st.caption("Par défaut : 471000 et 512000. Laisse vide pour garder les valeurs par défaut.")
    c1, c2 = st.columns(2)
    compte_471_input = c1.text_input("Compte d'attente (6 chiffres, commence par 47)", value="", placeholder="471000")
    compte_512_input = c2.text_input("Compte banque (6 chiffres, commence par 512)", value="", placeholder="512000")

run = st.button("▶️ Lancer le processus complet", type="primary")

if uploaded_pdf and run:
    try:
        compte471, compte512 = validate_accounts(compte_471_input, compte_512_input)

        # Étape 1 : sauvegarde PDF en temporaire pour Camelot
        with st.spinner("Étape 1/4 — Extraction des tableaux (Camelot)..."):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(uploaded_pdf.getbuffer())
                tmp_path = tmp.name

            df_camelot = extraire_tables_en_excel(tmp_path)

            if df_camelot.empty:
                st.error("Aucun tableau détecté dans le PDF (Camelot).")
                st.stop()

        st.success(f"Étape 1/4 : Extraction terminée ({df_camelot.shape[0]} lignes).")
        st.subheader("Aperçu après Camelot")
        st.dataframe(df_camelot.head(30), use_container_width=True)

        # Étape 2 : nettoyage spécifique SG
        with st.spinner("Étape 2/4 — Nettoyage spécifique Société Générale..."):
            df_sg = nettoyer_releve_sg(df_camelot)

        st.success(f"Étape 2/4 : Nettoyage SG terminé ({df_sg.shape[0]} lignes).")
        st.subheader("Aperçu après Nettoyage SG")
        st.dataframe(df_sg.head(30), use_container_width=True)

        # Étape 3 : nettoyage des libellés via règles
        with st.spinner("Étape 3/4 — Nettoyage des libellés (bdd_criteres.xlsx)..."):
            # df_sg : 0=Date,1=Nature(libellé brut),2=Débit,3=Crédit
            df_labels = nettoyer_libelles_dataframe(df_sg)

        st.success(f"Étape 3/4 : Nettoyage libellés terminé ({df_labels.shape[0]} lignes).")
        st.subheader("Aperçu après Nettoyage des libellés")
        st.dataframe(df_labels.head(30), use_container_width=True)

        # Étape 4 : conversion en écritures ACD
        with st.spinner("Étape 4/4 — Génération des écritures comptables ACD..."):
            out_rows = []
            for _, row in df_labels.iterrows():
                date = row["Date"]
                lib  = "" if pd.isna(row["Libellé nettoyé"]) else str(row["Libellé nettoyé"])
                debit  = to_number(row["Débit"])
                credit = to_number(row["Crédit"])

                if debit is not None and abs(debit) < 1e-12:
                    debit = None
                if credit is not None and abs(credit) < 1e-12:
                    credit = None

                parsed = parse_remise_cb(lib)
                if parsed:
                    core, brut, frais, net = parsed
                    out_rows.append([date, None, compte471, core, None, None, brut])
                    out_rows.append([date, None, 627000, f"FRAIS : {core}", None, frais, None])
                    out_rows.append([date, None, compte512, core, None, net, None])
                    continue

                if debit is not None and credit is None:
                    out_rows.append([date, None, compte471, lib, None, debit, None])
                    out_rows.append([date, None, compte512, lib, None, None, debit])
                elif credit is not None and debit is None:
                    out_rows.append([date, None, compte471, lib, None, None, credit])
                    out_rows.append([date, None, compte512, lib, None, credit, None])
                else:
                    # ligne anormale (débit+crédit ou aucun) -> ignorée
                    continue

            out_df = pd.DataFrame(
                out_rows,
                columns=["Date", "Pièce", "Compte", "Libellé", "Journal", "Débit", "Crédit"]
            )

        if out_df.empty:
            st.warning("Aucune écriture générée.")
        else:
            st.success(f"✅ Processus complet terminé. {len(out_df)} lignes d'écritures générées.")
            st.subheader("Aperçu des écritures générées")
            st.dataframe(out_df.head(100), use_container_width=True)

            xbio = io.BytesIO()
            with pd.ExcelWriter(xbio, engine="openpyxl") as writer:
                out_df.to_excel(writer, index=False, sheet_name="BQ")
            xbio.seek(0)

            out_name = make_output_name(uploaded_pdf.name)
            st.download_button(
                "💾 Télécharger le fichier d'écritures ACD (Excel)",
                data=xbio.getvalue(),
                file_name=out_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except Exception as e:
        st.error("Une erreur est survenue pendant le processus complet.")
        st.exception(e)

elif not uploaded_pdf and run:
    st.warning("Merci de déposer d’abord un PDF avant de lancer le processus.")