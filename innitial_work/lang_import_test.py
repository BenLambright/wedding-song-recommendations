# Dataproc PySpark script: Compare gcld3, langid, and fastText for es/en/fr
# - Installs deps on driver, ships them to executors
# - Trains a tiny fastText model on-the-fly (es/en/fr)
# - Produces a comparison DataFrame with majority-vote consensus

import os, sys, subprocess, shutil, tempfile, textwrap, uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark import SparkFiles

spark = (SparkSession.builder
         .appName("LangDetect-Compare-gcld3-langid-fasttext")
         .getOrCreate())

# ---------- 1) Install Python deps on driver and distribute to executors ----------
site_dir = "/tmp/pip_site_pkgs"
site_zip = "/tmp/pip_site_pkgs.zip"
pkgs = ["gcld3==3.0.13", "langid==1.1.6", "fasttext==0.9.2"]

def install_and_package_deps():
    if os.path.exists(site_dir):
        shutil.rmtree(site_dir)
    os.makedirs(site_dir, exist_ok=True)

    print("Installing dependencies on driver...", flush=True)
    # Install into a local folder so we can zip and ship it
    cmd = [sys.executable, "-m", "pip", "install", "--no-cache-dir", "-q", "-t", site_dir] + pkgs
    try:
        subprocess.check_call(cmd)
        print("Driver install: SUCCESS", flush=True)
    except subprocess.CalledProcessError as e:
        print("Driver install: FAILED", flush=True)
        print(e, flush=True)
        raise

    if os.path.exists(site_zip):
        os.remove(site_zip)
    shutil.make_archive(site_zip.replace(".zip", ""), "zip", site_dir)
    spark.sparkContext.addFile(site_zip)

install_and_package_deps()

# A helper that executors can call to ensure they can import the shipped packages
def ensure_executor_site():
    import sys
    from pyspark import SparkFiles
    zip_path = SparkFiles.get(os.path.basename(site_zip))
    if zip_path and zip_path not in sys.path:
        sys.path.insert(0, zip_path)

# ---------- 2) Import checks (driver) & friendly messages ----------
def try_import(pkgname, import_stmt):
    try:
        exec(import_stmt, globals(), globals())
        print(f"[OK] Imported {pkgname}", flush=True)
        return True
    except Exception as e:
        print(f"[FAIL] Could not import {pkgname}: {e}", flush=True)
        return False

print("\nVerifying imports on driver:")
ok_gcld3   = try_import("gcld3",   "from gcld3 import NNetLanguageIdentifier")
ok_langid  = try_import("langid",  "import langid")
ok_fasttxt = try_import("fasttext","import fasttext")

# ---------- 3) Build a tiny on-the-fly fastText model for es/en/fr ----------
def train_fasttext_model():
    import fasttext, os
    train_txt = "/tmp/ft_train_es_en_fr.txt"
    model_bin = "/tmp/ft_model_es_en_fr.bin"

    samples_en = [
        "Hello, how are you today?",
        "This is a simple English sentence.",
        "Spark makes big data processing easier.",
        "Language detection can be tricky sometimes.",
        "We are testing different libraries."
    ]
    samples_es = [
        "Hola, ¿cómo estás hoy?",
        "Esta es una oración simple en español.",
        "Spark facilita el procesamiento de grandes datos.",
        "La detección de idioma a veces es complicada.",
        "Estamos probando diferentes librerías."
    ]
    samples_fr = [
        "Bonjour, comment ça va aujourd'hui ?",
        "Ceci est une phrase simple en français.",
        "Spark facilite le traitement des données massives.",
        "La détection de la langue peut être délicate.",
        "Nous testons différentes bibliothèques."
    ]

    with open(train_txt, "w", encoding="utf-8") as f:
        for s in samples_en: f.write(f"__label__en {s}\n")
        for s in samples_es: f.write(f"__label__es {s}\n")
        for s in samples_fr: f.write(f"__label__fr {s}\n")

    # Quick, tiny supervised model (sufficient for a dependency smoke test)
    model = fasttext.train_supervised(
        input=train_txt,
        lr=1.0,
        epoch=25,
        wordNgrams=2,
        verbose=0,
        minCount=1
    )
    model.save_model(model_bin)
    return model_bin

print("\nTraining tiny fastText model (es/en/fr) on driver...")
# Ensure driver has imports available to train
from importlib import import_module
import_module("fasttext")
ft_model_path_driver = train_fasttext_model()
print(f"fastText model saved at: {ft_model_path_driver}", flush=True)

# Ship model to executors
spark.sparkContext.addFile(ft_model_path_driver)

# ---------- 4) Create sample data (es/en/fr) ----------
data = [
    # English
    ("Hello there! This library should detect English.",),
    ("We are running language detection tests in Spark.",),
    # Spanish
    ("¡Hola! Esta biblioteca debería detectar español.",),
    ("Estamos ejecutando pruebas de detección de idioma en Spark.",),
    # French
    ("Salut ! Cette bibliothèque devrait détecter le français.",),
    ("Nous exécutons des tests de détection de langue dans Spark.",),
    # Short / mixed
    ("Hello mundo, bonjour world.",),
    ("El sistema devrait reconnaître plusieurs langues.",),
]

df = spark.createDataFrame(data, ["text"])

# ---------- 5) Define UDFs that import libs on executors (from the shipped zip) ----------
# gcld3 UDF
@F.udf(StringType())
def detect_gcld3(text):
    try:
        ensure_executor_site()
        from gcld3 import NNetLanguageIdentifier
        if not text:
            return None
        identifier = NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000)
        res = identifier.FindLanguage(text)
        # gcld3 returns ISO 639-1 or similar (e.g., 'en', 'es', 'fr')
        return res.language
    except Exception:
        return None

# langid UDF
@F.udf(StringType())
def detect_langid(text):
    try:
        ensure_executor_site()
        import langid
        if not text:
            return None
        lang, _ = langid.classify(text)
        return lang
    except Exception:
        return None

# fastText UDF with lazy model load per executor
_fasttext_model = None
def _get_fasttext_model():
    global _fasttext_model
    if _fasttext_model is None:
        ensure_executor_site()
        import fasttext
        model_path = SparkFiles.get(os.path.basename(ft_model_path_driver))
        _fasttext_model = fasttext.load_model(model_path)
    return _fasttext_model

@F.udf(StringType())
def detect_fasttext(text):
    try:
        if not text:
            return None
        m = _get_fasttext_model()
        labels, probs = m.predict(text)
        if not labels:
            return None
        # labels like "__label__en"
        return labels[0].replace("__label__", "")
    except Exception:
        return None

# ---------- 6) Majority-vote consensus UDF ----------
@F.udf(StringType())
def consensus3(a, b, c):
    # Prefer majority; on ties, fall back to gcld3 (a)
    votes = [x for x in [a, b, c] if x]
    if not votes:
        return None
    # Count votes
    best = None
    bestc = 0
    for code in set(votes):
        cnt = votes.count(code)
        if cnt > bestc:
            best, bestc = code, cnt
    if bestc >= 2:
        return best
    # tie (all different) -> prefer gcld3 (a)
    return a

# ---------- 7) Apply detectors in Spark and display results ----------
res = (df
       .withColumn("gcld3_lang", detect_gcld3(F.col("text")))
       .withColumn("langid_lang", detect_langid(F.col("text")))
       .withColumn("fasttext_lang", detect_fasttext(F.col("text")))
       .withColumn("consensus_lang", consensus3("gcld3_lang", "langid_lang", "fasttext_lang"))
      )

print("\n=== Detection Results (first 100 rows) ===")
res.show(100, truncate=False)

# ---------- 8) Final dependency status line ----------
print("\n=== Dependency Import Status (Driver) ===")
print(f"gcld3:   {'OK' if ok_gcld3 else 'FAIL'}")
print(f"langid:  {'OK' if ok_langid else 'FAIL'}")
print(f"fastText:{'OK' if ok_fasttxt else 'FAIL'}")

# Keep Spark session alive in notebooks (no-op in spark-submit)
_ = res.count()  # materialize
