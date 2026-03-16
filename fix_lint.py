import os

files = [
    r"src\extractors\base_extractor.py",
    r"src\extractors\postgres_extractor.py",
    r"src\extractors\sqlserver_extractor.py",
    r"src\loaders\warehouse_loader.py",
    r"src\pipeline.py",
    r"src\seed_data.py",
    r"src\verify_sources.py",
]

for filepath in files:
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Fix W293 — remove whitespace from blank lines
    lines = content.split("\n")
    lines = [line if line.strip() != "" else "" for line in lines]
    content = "\n".join(lines)

    # Fix W292 — ensure newline at end of file
    if not content.endswith("\n"):
        content += "\n"

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Fixed: {filepath}")

print("Done.")