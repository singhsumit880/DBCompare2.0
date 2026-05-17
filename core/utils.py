"""
core/utils.py
Shared utility functions: zip, unzip, temp directory management.
All temp dirs are anchored to the package root — not the CWD.
"""
import os
import shutil
import zipfile
import logging

# Absolute path to the project root (one level above core/)
_PKG_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_temp_dir(name: str) -> str:
    """Return (and create) an absolute temp directory under the project root."""
    path = os.path.join(_PKG_ROOT, name)
    os.makedirs(path, exist_ok=True)
    return path


def cleanup_dir(path: str) -> None:
    """Remove a directory tree, ignoring errors."""
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)


def unzip_vyb(vyb_file: str, extract_to: str) -> None:
    """Extract all contents of a .vyb (ZIP) archive into extract_to."""
    with zipfile.ZipFile(vyb_file, 'r') as zf:
        zf.extractall(extract_to)


def zip_vyp(vyp_file: str, vyb_file: str) -> None:
    """Zip a single .vyp file into a .vyb archive."""
    with zipfile.ZipFile(vyb_file, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(vyp_file, os.path.basename(vyp_file))


def find_vyp_in_dir(directory: str) -> str | None:
    """Return the first .vyp file found in directory, or None."""
    for fname in os.listdir(directory):
        if fname.lower().endswith('.vyp'):
            return os.path.join(directory, fname)
    return None


def safe_output_paths(temp_dir: str, output_filename: str) -> tuple[str, str]:
    """
    Given an output filename (e.g. Sanitized_foo.vyp or Sanitized_foo.vyb),
    return (output_vyp_path, output_vyb_path) using splitext — never .replace().
    """
    base = os.path.splitext(output_filename)[0]
    return (
        os.path.join(temp_dir, base + '.vyp'),
        os.path.join(temp_dir, base + '.vyb'),
    )
