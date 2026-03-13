import json

data = json.load(open("fr_family_1367_audit.json", "r", encoding="utf-8"))

print("=== MISSING COLOR OR SIZE ===")
for d in data:
    if not d["size"] or not d["color"]:
        print(f"  {d['asin']} {d['sku']:<28} color={d['color']!r:<25} size={d['size']!r:<15} title={d['title'][:70]}")

print(f"\n=== TRANSPARENT (needs fix) ===")
for d in data:
    if d["color"] == "Transparent":
        print(f"  {d['asin']} {d['sku']:<28} size={d['size']!r:<15} cat_parent={d['catalog_parent']}  title={d['title'][:60]}")

print(f"\nTotal children: {len(data)}")
# Count by color
by_color = {}
for d in data:
    c = d["color"] or "(empty)"
    by_color[c] = by_color.get(c, 0) + 1
print(f"By color: {json.dumps(by_color, ensure_ascii=False)}")
