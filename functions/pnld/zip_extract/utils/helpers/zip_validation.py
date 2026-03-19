
import zipfile

def is_true_zip(file_path: str) -> bool:
    """
    Returns True if the file is a generic ZIP archive (not an Office/OOXML ZIP).
    Specifically excludes DOCX/XLSX/PPTX by checking for their known folder structure.
    """
    # Step 1: Check ZIP container
    if not zipfile.is_zipfile(file_path):
        return False

    office_dirs = ('word/', 'xl/', 'ppt/')

    with zipfile.ZipFile(file_path, 'r') as z:
        try:
            names = z.namelist()  # list of strings
        except Exception:
            # If namelist fails for any reason, treat as non-true ZIP
            return False

        # Step 2: Detect Office-style structure
        for name in names:
            # Be defensive: ensure 'name' is a string
            if not isinstance(name, str):
                try:
                    name = name.decode('utf-8', errors='ignore')
                except Exception:
                    continue

            # Check if it starts with any Office directory
            if name.startswith(office_dirs):
                return False  # Office document (OOXML)

        # Additional robust check: OOXML almost always has [Content_Types].xml
        if '[Content_Types].xml' in names:
            return False

        return True
