from pathlib import Path

root = Path(__file__).resolve().parent.parent
lines = (root / "system_auto_pilot.py").read_text(encoding="utf-8").splitlines(keepends=True)
n = len(lines)
parts = 4
size = (n + parts - 1) // parts
out_dir = Path(__file__).resolve().parent / "sap_paste_parts"
out_dir.mkdir(exist_ok=True)
for i in range(parts):
    chunk = lines[i * size : (i + 1) * size]
    part_no = i + 1
    delim = f"SAP_PART{part_no}_EOF"
    op = ">" if i == 0 else ">>"
    target = "system_auto_pilot.py"
    header = f"""# Paste part {part_no}/{parts} on Ubuntu (project root)
cat << '{delim}' {op} {target}
"""
    footer = f"{delim}\n"
    path = out_dir / f"part{part_no}_of_{parts}.sh.txt"
    path.write_text(header + "".join(chunk) + footer, encoding="utf-8")
    print(path, "lines", len(chunk))
