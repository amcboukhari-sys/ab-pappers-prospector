#!/usr/bin/env python3
"""
AB INTELLIGENCE — Pappers Compliance Prospector v3
Tourne en local OU via GitHub Actions (cron quotidien).
Variables d'environnement prioritaires sur les valeurs codées en dur.
"""

import requests
import time
import os
import sys
import csv
import json
import base64
import tempfile
from datetime import date
from urllib.parse import quote

# ================================================================
# ⚙️  CONFIG — lues depuis les secrets GitHub Actions en priorité
# ================================================================
PAPPERS_API_KEY  = os.getenv("PAPPERS_API_KEY",  "")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
SLACK_BOT_TOKEN  = os.getenv("SLACK_BOT_TOKEN",  "")   # xoxb-... — requis pour notif Slack
SLACK_USER_ID    = os.getenv("SLACK_USER_ID",    "U0AQA9JMFNZ")

GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")       # ID du dossier Google Drive
GDRIVE_SA_JSON   = os.getenv("GDRIVE_SA_JSON_B64", "")     # Service Account JSON (base64)

AIRTABLE_BASE_ID = "appWSQ2iH7b5CNRgy"
AIRTABLE_TABLE   = "tblDeBkovrsOA68FR"
PAPPERS_URL      = "https://api.pappers.fr/v2/recherche"

MAX_PAGES_EFFECTIF = 4    # 400 résultats max (plan gratuit = ~4 pages)
MAX_PAGES_NAF      = 4    # idem par code NAF
DELAY              = 0.4  # secondes entre requêtes

# ================================================================
# 📐  TRANCHES EFFECTIF INSEE
# ================================================================
TRANCHES_MIN = {
    "00":0,"01":1,"02":3,"03":6,"11":10,"12":20,
    "21":50,"22":100,"31":200,"32":250,
    "41":500,"42":1000,"51":2000,"52":5000,"53":10000
}
TRANCHES_LABEL = {
    "00":"0 salarié","01":"1-2","02":"3-5","03":"6-9",
    "11":"10-19","12":"20-49","21":"50-99","22":"100-199",
    "31":"200-249","32":"250-499","41":"500-999","42":"1 000-1 999",
    "51":"2 000-4 999","52":"5 000-9 999","53":"10 000+"
}

# ================================================================
# 🏦  CODES NAF LBC-FT
# ================================================================
LBCFT_NAF = {
    "6419Z":"Intermédiations monétaires / Banques",
    "6491Z":"Crédit-bail",
    "6492Z":"Distribution de crédit",
    "6499Z":"Services financiers / PSAN",
    "6420Z":"Sociétés holding",
    "6611Z":"Administration de marchés financiers",
    "6612Z":"Courtage valeurs mobilières",
    "6619A":"Supports juridiques patrimoine immo",
    "6619B":"Activités auxiliaires services financiers",
    "6630Z":"Gestion de fonds",
    "6511Z":"Assurance vie",
    "6512Z":"Autres assurances",
    "6521Z":"Réassurance vie",
    "6522Z":"Autres réassurances",
    "6621Z":"Évaluation risques / assurance",
    "6622Z":"Agents et courtiers assurances",
    "6629Z":"Activités auxiliaires assurance",
    "6530Z":"Caisses de retraite",
    "6810Z":"Marchands de biens immobiliers",
    "6831Z":"Agences immobilières",
    "6832A":"Administration d'immeubles",
    "6832B":"Supports juridiques gestion patrimoine",
    "6910Z":"Activités juridiques (avocats, notaires)",
    "6920Z":"Activités comptables (EC, CAC)",
    "9200Z":"Jeux de hasard et d'argent / Casinos",
    "7911Z":"Agences de voyage",
    "7912Z":"Voyagistes",
    "8299Z":"Domiciliation d'entreprises",
}
LBCFT_SET  = set(LBCFT_NAF.keys())
DORA_SET   = {"6419Z","6491Z","6492Z","6499Z","6611Z","6612Z",
              "6619A","6619B","6630Z","6511Z","6512Z","6521Z","6522Z"}

# ================================================================
# 🔬  ÉLIGIBILITÉ COMPLIANCE
# ================================================================
def eligibilite(tranche, ca, naf):
    e   = TRANCHES_MIN.get(str(tranche), 0)
    naf = naf.upper().strip()
    ca  = float(ca) if ca else 0.0
    ok  = ca > 0

    alerte    = e >= 50
    sapin2    = e >= 500  and (ok and ca >= 1e8   or not ok)
    vigilance = e >= 5000
    csddd     = (e >= 3000 and (ok and ca >= 9e8  or not ok)) or \
                (e >= 1000 and (ok and ca >= 4.5e8 or not ok))
    lbcft     = naf in LBCFT_SET
    dora      = naf in DORA_SET

    score = sapin2*3 + vigilance*4 + csddd*2 + lbcft*3 + dora*2 + alerte*1
    if score >= 9:   niveau = "Critique"
    elif score >= 5: niveau = "Élevé"
    elif score >= 2: niveau = "Modéré"
    else:            niveau = "Standard"

    lois = []
    if sapin2:             lois.append("Sapin II")
    if vigilance:          lois.append("Devoir de Vigilance")
    if csddd and not vigilance: lois.append("CSDDD (futur)")
    if alerte:             lois.append("Alerte Éthique")
    if lbcft:              lois.append("LBC-FT")
    if dora:               lois.append("DORA")

    return dict(sapin2=sapin2, vigilance=vigilance, csddd=csddd,
                alerte=alerte, lbcft=lbcft, niveau=niveau, lois=lois,
                ok=any([sapin2, vigilance, csddd, alerte, lbcft]))

# ================================================================
# 🔌  PAPPERS
# ================================================================
class Pappers:
    def __init__(self, key):
        self.key = key; self.calls = 0
        self.sess = requests.Session()
        self.sess.headers["Accept"] = "application/json"

    def _get(self, params):
        params["api_token"] = self.key
        try:
            r = self.sess.get(PAPPERS_URL, params=params, timeout=30)
            self.calls += 1
            if r.status_code == 429:
                print("  ⏳ Rate limit — pause 60s..."); time.sleep(60)
                return self._get({k:v for k,v in params.items() if k!="api_token"})
            if r.status_code != 200:
                print(f"  ⚠️  HTTP {r.status_code} — quota atteint ou erreur"); return {}
            return r.json()
        except Exception as e:
            print(f"  ⚠️  Erreur: {e}"); return {}

    def search_effectif(self, tmin="21", tmax="53", page=1):
        return self._get({"par_page":100,"page":page,
                          "tranche_effectif_min":tmin,"tranche_effectif_max":tmax,
                          "entreprise_cessee":"false"})

    def search_naf(self, codes, page=1):
        return self._get({"par_page":100,"page":page,
                          "code_naf":codes,"entreprise_cessee":"false"})

    def search_ca(self, ca_min, tmin="41", page=1):
        return self._get({"par_page":100,"page":page,
                          "chiffre_affaires_min":ca_min,
                          "tranche_effectif_min":tmin,
                          "entreprise_cessee":"false"})


def paginate(fn, *args, max_pages=4):
    results, page = [], 1
    while page <= max_pages:
        data  = fn(*args, page=page)
        batch = data.get("resultats") or []
        total = data.get("total", 0)
        if not batch: break
        results.extend(batch)
        print(f"      p.{page}  +{len(batch):4d}  [{len(results):,}/{total:,}]")
        if total and len(results) >= total: break
        if len(batch) < 100: break
        page += 1
        time.sleep(DELAY)
    return results

# ================================================================
# 🔄  PARSING Pappers → Airtable
# ================================================================
def parse(raw):
    siege = raw.get("siege") or {}
    nom   = (raw.get("nom_entreprise") or raw.get("denomination")
             or raw.get("nom_complet") or "")
    siren = str(raw.get("siren") or "")
    siret = siege.get("siret") or raw.get("siret") or ""
    forme = raw.get("forme_juridique") or ""
    naf   = (raw.get("code_naf") or siege.get("code_naf") or "").upper().strip()
    lib   = (raw.get("libelle_code_naf") or siege.get("libelle_code_naf")
             or LBCFT_NAF.get(naf,""))
    tr    = (raw.get("tranche_effectif") or raw.get("tranche_effectif_salaries")
             or siege.get("tranche_effectif") or "")
    eff_l = TRANCHES_LABEL.get(str(tr), f"code {tr}" if tr else "")
    ca    = 0.0
    fins  = raw.get("finances") or []
    if fins: ca = float(fins[0].get("chiffre_affaires") or 0)
    adr   = siege.get("adresse_ligne_1") or siege.get("adresse") or ""
    ville = siege.get("ville") or siege.get("commune") or ""
    cp    = siege.get("code_postal") or ""
    dcr   = (raw.get("date_creation") or "")[:10]
    elig  = eligibilite(tr, ca, naf)

    rec = {
        "Entreprise":                        nom,
        "SIREN":                             siren,
        "SIRET Siège":                       siret,
        "Forme Juridique":                   forme,
        "Code NAF":                          naf,
        "Secteur d'activité":                lib,
        "Effectif (tranche)":                eff_l,
        "Ville":                             ville,
        "Code Postal":                       cp,
        "Adresse":                           adr,
        "Sapin II (Art.17)":                 elig["sapin2"],
        "Devoir de Vigilance - Actuel":      elig["vigilance"],
        "Devoir de Vigilance - CSDDD 2027+": elig["csddd"],
        "Alerte Éthique (≥50 sal.)":         elig["alerte"],
        "LBC-FT":                            elig["lbcft"],
        "Lois Applicables":                  elig["lois"],
        "Niveau d'exposition":               elig["niveau"],
        "URL Pappers":                       f"https://www.pappers.fr/entreprise/{siren}" if siren else "",
        "Statut Prospection":                "À contacter",
        "Date Import":                       date.today().isoformat(),
    }
    if ca > 0: rec["Chiffre d'affaires (€)"] = ca
    if dcr:    rec["Date Création"] = dcr
    return rec, elig["ok"], elig

# ================================================================
# 📤  AIRTABLE
# ================================================================
class Airtable:
    def __init__(self, key, base_id):
        self.sess = requests.Session()
        self.sess.headers.update({"Authorization":f"Bearer {key}",
                                   "Content-Type":"application/json"})
        self.url = f"https://api.airtable.com/v0/{base_id}/{AIRTABLE_TABLE}"

    def existing_sirens(self):
        sirens, params = set(), {"fields[]":"SIREN","pageSize":100}
        while True:
            d = self.sess.get(self.url, params=params, timeout=30).json()
            for r in d.get("records",[]):
                s = r.get("fields",{}).get("SIREN")
                if s: sirens.add(str(s))
            if not d.get("offset"): break
            params["offset"] = d["offset"]
        return sirens

    def insert(self, records):
        n = 0
        for i in range(0, len(records), 10):
            batch = records[i:i+10]
            r = self.sess.post(self.url,
                               json={"records":[{"fields":x} for x in batch]},
                               timeout=30)
            if r.status_code in (200,201): n += len(batch)
            else: print(f"\n  ⚠️  Airtable {r.status_code}: {r.text[:200]}")
            time.sleep(0.25)
        return n

# ================================================================
# 📱  SLACK NOTIFICATION
# ================================================================
def notify_slack(stats, inserted):
    if not SLACK_BOT_TOKEN:
        print("  ℹ️  Pas de SLACK_BOT_TOKEN — notification ignorée")
        return
    msg = (
        f"✅ *Pappers Import terminé !* {inserted:,} entreprises ajoutées dans Airtable.\n\n"
        f"📊 *Répartition compliance :*\n"
        f"• 🔴 Sapin II (art.17)           : {stats['sapin2']:,}\n"
        f"• 🟠 Devoir de Vigilance          : {stats['vigilance']:,}\n"
        f"• 🟡 CSDDD 2027+ (anticipation)  : {stats['csddd']:,}\n"
        f"• 🔵 Alerte Éthique (≥50 sal.)   : {stats['alerte']:,}\n"
        f"• 🟣 LBC-FT (secteur assujetti)  : {stats['lbcft']:,}\n\n"
        f"🔗 <https://airtable.com/{AIRTABLE_BASE_ID}|Ouvrir dans Airtable>"
    )
    try:
        r = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                     "Content-Type": "application/json"},
            json={"channel": SLACK_USER_ID, "text": msg, "mrkdwn": True},
            timeout=10
        )
        data = r.json()
        if data.get("ok"):
            print(f"  ✅ Slack DM envoyé à {SLACK_USER_ID}")
        else:
            print(f"  ⚠️  Slack error: {data.get('error')}")
    except Exception as e:
        print(f"  ⚠️  Slack exception: {e}")

def send_csv_to_slack(filepath):
    """Envoie le CSV en fichier attaché sur Slack."""
    if not SLACK_BOT_TOKEN:
        print("  ℹ️  Pas de SLACK_BOT_TOKEN — envoi CSV Slack ignoré")
        return
    try:
        filename = os.path.basename(filepath)
        with open(filepath, "rb") as f:
            r = requests.post(
                "https://slack.com/api/files.upload",
                headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
                data={
                    "channels": SLACK_USER_ID,
                    "title": filename,
                    "initial_comment": f"📎 Backup CSV du {date.today().isoformat()} — {filename}",
                    "filename": filename,
                },
                files={"file": (filename, f, "text/csv")},
                timeout=30
            )
        data = r.json()
        if data.get("ok"):
            print(f"  ✅ CSV envoyé sur Slack : {filename}")
        else:
            print(f"  ⚠️  Slack upload error: {data.get('error')}")
    except Exception as e:
        print(f"  ⚠️  Slack upload exception: {e}")

# ================================================================
# 📁  CSV BACKUP + GOOGLE DRIVE UPLOAD
# ================================================================
CSV_COLUMNS = [
    "Entreprise","SIREN","SIRET Siège","Forme Juridique","Code NAF",
    "Secteur d'activité","Effectif (tranche)","Chiffre d'affaires (€)",
    "Ville","Code Postal","Adresse","Date Création",
    "Sapin II (Art.17)","Devoir de Vigilance - Actuel",
    "Devoir de Vigilance - CSDDD 2027+","Alerte Éthique (≥50 sal.)",
    "LBC-FT","Lois Applicables","Niveau d'exposition",
    "URL Pappers","Statut Prospection","Date Import"
]

def save_csv(records):
    """Sauvegarde les records dans un CSV local et retourne le chemin."""
    filename = f"pappers_prospects_{date.today().isoformat()}.csv"
    filepath = os.path.join(tempfile.gettempdir(), filename)
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, delimiter=";",
                           extrasaction="ignore")
        w.writeheader()
        for rec in records:
            row = dict(rec)
            # Convertir les listes en texte
            if isinstance(row.get("Lois Applicables"), list):
                row["Lois Applicables"] = ", ".join(row["Lois Applicables"])
            # Convertir les booléens en Oui/Non
            for k in ["Sapin II (Art.17)","Devoir de Vigilance - Actuel",
                       "Devoir de Vigilance - CSDDD 2027+",
                       "Alerte Éthique (≥50 sal.)","LBC-FT"]:
                if k in row:
                    row[k] = "Oui" if row[k] else "Non"
            w.writerow(row)
    print(f"\n  💾 CSV sauvegardé : {filepath} ({len(records)} lignes)")
    return filepath

def upload_to_gdrive(filepath):
    """Upload le CSV vers Google Drive via Service Account."""
    if not GDRIVE_SA_JSON or not GDRIVE_FOLDER_ID:
        print("  ℹ️  Pas de config Google Drive — upload ignoré")
        return
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("  ⚠️  google-api-python-client non installé — upload ignoré")
        return

    try:
        sa_json = json.loads(base64.b64decode(GDRIVE_SA_JSON))
        creds = Credentials.from_service_account_info(
            sa_json, scopes=["https://www.googleapis.com/auth/drive.file"])
        service = build("drive", "v3", credentials=creds)

        filename = os.path.basename(filepath)
        meta = {"name": filename, "parents": [GDRIVE_FOLDER_ID]}
        media = MediaFileUpload(filepath, mimetype="text/csv")
        f = service.files().create(body=meta, media_body=media, fields="id,webViewLink").execute()
        print(f"  ☁️  Upload Google Drive OK : {f.get('webViewLink','')}")
    except Exception as e:
        print(f"  ⚠️  Erreur Google Drive : {e}")

# ================================================================
# 🚀  MAIN
# ================================================================
def main():
    print("\n" + "═"*60)
    print("  AB INTELLIGENCE — Pappers Compliance Prospector v3")
    print("═"*60)

    p  = Pappers(PAPPERS_API_KEY)
    at = Airtable(AIRTABLE_API_KEY, AIRTABLE_BASE_ID)

    print("\n📋  SIREN existants dans Airtable...")
    existing = at.existing_sirens()
    print(f"    → {len(existing):,} déjà en base")

    pool = {}

    # ── 1. Effectif ≥ 50 salariés ─────────────────────────────
    print("\n" + "─"*60)
    print("🔍  [1/3] Effectif ≥ 50 salariés")
    print("─"*60)
    for r in paginate(p.search_effectif, "21", "53", max_pages=MAX_PAGES_EFFECTIF):
        s = str(r.get("siren") or "")
        if s and s not in existing: pool[s] = r
    print(f"    → {len(pool):,} uniques")

    # ── 2. Sapin II avec CA connu ──────────────────────────────
    print("\n" + "─"*60)
    print("🔍  [2/3] Sapin II — CA ≥ 100M€ + effectif ≥ 500")
    print("─"*60)
    before = len(pool)
    for r in paginate(p.search_ca, 100_000_000, "41", max_pages=4):
        s = str(r.get("siren") or "")
        if s and s not in existing: pool[s] = r
    print(f"    → +{len(pool)-before:,} nouvelles")

    # ── 3. LBC-FT par batches de 5 codes NAF ──────────────────
    naf_list = list(LBCFT_NAF.keys())
    batches  = [naf_list[i:i+5] for i in range(0, len(naf_list), 5)]
    print("\n" + "─"*60)
    print(f"🔍  [3/3] LBC-FT — {len(naf_list)} codes NAF / {len(batches)} batches")
    print("─"*60)
    before = len(pool)
    for i, batch in enumerate(batches):
        codes = ",".join(batch)
        print(f"\n  Batch {i+1}/{len(batches)}: {codes}")
        for r in paginate(p.search_naf, codes, max_pages=MAX_PAGES_NAF):
            s = str(r.get("siren") or "")
            if s and s not in existing: pool[s] = r
    print(f"\n    → +{len(pool)-before:,} nouvelles LBC-FT")

    # ── Calcul éligibilité ─────────────────────────────────────
    print("\n" + "─"*60)
    print(f"⚙️   Traitement de {len(pool):,} entreprises...")
    to_insert = []
    stats = dict(total=0,skip=0,sapin2=0,vigilance=0,csddd=0,alerte=0,lbcft=0,
                 critique=0,eleve=0,modere=0)

    for siren, raw in pool.items():
        rec, ok, elig = parse(raw)
        if not ok: stats["skip"] += 1; continue
        stats["total"] += 1
        if elig["sapin2"]:    stats["sapin2"]    += 1
        if elig["vigilance"]: stats["vigilance"] += 1
        if elig["csddd"]:     stats["csddd"]     += 1
        if elig["alerte"]:    stats["alerte"]    += 1
        if elig["lbcft"]:     stats["lbcft"]     += 1
        n = elig["niveau"]
        if n=="Critique": stats["critique"] += 1
        elif n=="Élevé":  stats["eleve"]    += 1
        elif n=="Modéré": stats["modere"]   += 1
        to_insert.append(rec)

    print(f"\n  📊 {stats['total']:,} prospects :")
    print(f"     Sapin II          : {stats['sapin2']:,}")
    print(f"     Devoir Vigilance  : {stats['vigilance']:,}")
    print(f"     CSDDD 2027+       : {stats['csddd']:,}")
    print(f"     Alerte Éthique    : {stats['alerte']:,}")
    print(f"     LBC-FT            : {stats['lbcft']:,}")
    print(f"     🔴 Critique       : {stats['critique']:,}")
    print(f"     🟠 Élevé          : {stats['eleve']:,}")

    # ── Backup CSV → Slack + Google Drive ────────────────────
    if to_insert:
        csv_path = save_csv(to_insert)
        send_csv_to_slack(csv_path)
        upload_to_gdrive(csv_path)

    # ── Import Airtable ────────────────────────────────────────
    print(f"\n📤  Import de {len(to_insert):,} records dans Airtable...")
    inserted = at.insert(to_insert)
    print(f"\n{'═'*60}")
    print(f"  ✅ {inserted:,} entreprises importées")
    print(f"  📞 Appels Pappers : {p.calls}")
    print(f"{'═'*60}\n")

    # ── Notification Slack ─────────────────────────────────────
    if inserted > 0:
        notify_slack(stats, inserted)

if __name__ == "__main__":
    main()
