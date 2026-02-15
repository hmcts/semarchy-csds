import py7zr
import tarfile
import os

# =========================
# PATHS (Windows)
# =========================
SOURCE_7Z = r"C:\Users\gary.wilson\Downloads\Downloads.7z"
TEMP_DIR = r"C:\Users\gary.wilson\Downloads\extracted"
OUTPUT_TAR_GZ = r"C:\Users\gary.wilson\Downloads\Downloads.tar.gz"

# =========================
# PREP
# =========================
os.makedirs(TEMP_DIR, exist_ok=True)

# =========================
# EXTRACT .7Z
# =========================
print("Extracting .7z...")
with py7zr.SevenZipFile(SOURCE_7Z, mode="r") as archive:
    archive.extractall(path=TEMP_DIR)

# =========================
# CREATE .TAR.GZ
# =========================
print("Creating .tar.gz...")
with tarfile.open(OUTPUT_TAR_GZ, "w:gz") as tar:
    tar.add(TEMP_DIR, arcname="")

print(f"Done! Created: {OUTPUT_TAR_GZ}")
