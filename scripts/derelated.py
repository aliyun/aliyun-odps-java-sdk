import os
import re
from pathlib import Path

# Get the replacement target from the environment variable (default: CLOUD_PROVIDER)
TARGET_VAR = os.getenv("CLOUD_PROVIDER_VAR", "CLOUD_PROVIDER")
# REPLACE_PATTERN = re.compile(r'\baliyun\b', re.IGNORECASE) 
REPLACE_PATTERN = re.compile(r'(?<![A-Za-z0-9])aliyun(?![A-Za-z0-9])', re.IGNORECASE)

# Excluded Directory (compatible with Maven/Gradle projects)
EXCLUDE_DIRS = {".git", "target", "build", "node_modules", "__pycache__"}

def replace_content(text):
    return REPLACE_PATTERN.sub(TARGET_VAR, text)

def replace_name(name):
    return REPLACE_PATTERN.sub(TARGET_VAR, name)  # 无需 flags 参数

def process_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        new_content = replace_content(content)
        if content != new_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)

        new_name = replace_name(file_path.name)
        if new_name != file_path.name:
            new_path = file_path.parent / new_name
            file_path.rename(new_path)
    
    except Exception as e:
        print(f"Processing file {file_path} failed: {str(e)}")

def process_directory(root_dir):
    for dir_path in sorted(Path(root_dir).rglob("*"), reverse=True):
        if not dir_path.is_dir():
            continue
        if any(exclude in dir_path.parts for exclude in EXCLUDE_DIRS):
            continue

        new_dir_name = replace_name(dir_path.name)
        if new_dir_name != dir_path.name:
            new_dir_path = dir_path.parent / new_dir_name
            dir_path.rename(new_dir_path)

    for file_path in Path(root_dir).rglob("*"):
        if not file_path.is_file():
            continue
        if any(exclude in file_path.parts for exclude in EXCLUDE_DIRS):
            continue
        
        process_file(file_path)

if __name__ == "__main__":
    # Support custom replacement values through environment variables
    print(f"Use replacement variable: {TARGET_VAR}")
    process_directory(".")
    print(f"Replacement is done! All matches have been replaced with {TARGET_VAR}")
