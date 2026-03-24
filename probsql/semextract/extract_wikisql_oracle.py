"""
Extract oracle training data for the three micro-engines from WikiSQL ground truth.

WikiSQL gives us labeled (question, schema, where_column, where_value, operator) tuples —
exactly what we need to train QuestionDecomposer, ValueSpotter, and ColumnResolver.
"""

import json
import re
from collections import Counter
from pathlib import Path

WIKISQL_DIR = Path(__file__).parent.parent.parent / "wikisql_data" / "data"
OUTPUT_DIR = Path(__file__).parent / "oracle" / "dataset"

AGG_OPS = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']
COND_OPS = ['=', '>', '<', '>=', '<=']


def classify_value_type(value, column_name, column_type):
    """Classify what type a value is based on its content and context."""
    val_str = str(value).strip()
    col_lower = column_name.lower()

    # Number
    try:
        float(val_str.replace(",", ""))
        if re.match(r'^\d{4}$', val_str):
            return "year_string"
        if re.match(r'^\d{4}[-–]\d{2,4}$', val_str):
            return "season_string"
        return "number"
    except ValueError:
        pass

    # Year-like strings
    if re.match(r'^\d{4}$', val_str):
        return "year_string"
    if re.match(r'^\d{4}[-–]\d{2,4}$', val_str):
        return "season_string"

    # Person name (capitalized multi-word, common name columns)
    name_cols = {"player", "name", "winner", "candidate", "person", "commander",
                 "director", "artist", "incumbent", "instructor", "author",
                 "manager", "coach", "captain", "rider", "driver"}
    if any(nc in col_lower for nc in name_cols):
        return "person_name"

    # Location/place
    loc_cols = {"country", "city", "location", "venue", "capital", "state",
                "headquarters", "base", "region", "district", "county"}
    if any(lc in col_lower for lc in loc_cols):
        return "location"

    # Institution/organization
    inst_cols = {"school", "club", "team", "university", "college", "company",
                 "organization", "party", "network", "airline", "carrier"}
    if any(ic in col_lower for ic in inst_cols):
        return "institution"

    # Category/type
    cat_cols = {"position", "type", "category", "genre", "status", "result",
                "class", "division", "conference", "league", "branch", "tier",
                "rating", "rank"}
    if any(cc in col_lower for cc in cat_cols):
        return "category"

    # Date/time string
    if re.match(r'\w+ \d{1,2},? \d{4}', val_str) or re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', val_str):
        return "date_string"

    # Default: general string
    return "string"


def find_value_in_question(question, value):
    """Find the exact position and span of a value in a question (case-insensitive)."""
    val_str = str(value)
    q_lower = question.lower()
    v_lower = val_str.lower()

    # Exact match
    idx = q_lower.find(v_lower)
    if idx >= 0:
        return question[idx:idx + len(v_lower)], idx

    # Try without special chars
    v_cleaned = re.sub(r'[^\w\s]', '', v_lower)
    q_cleaned = re.sub(r'[^\w\s]', '', q_lower)
    idx = q_cleaned.find(v_cleaned)
    if idx >= 0:
        return v_cleaned, idx

    return None, -1


def extract_select_hint(question, headers, sel_idx):
    """Extract what the question is asking about (the SELECT column hint)."""
    q_lower = question.lower()
    sel_col = headers[sel_idx].lower()

    # Check if SELECT column name appears in question
    if sel_col in q_lower:
        return sel_col, "direct"

    # Check column name words
    col_words = re.findall(r'\b\w+\b', sel_col)
    for w in col_words:
        if len(w) > 2 and w in q_lower:
            return w, "partial"

    # Infer from question word
    q_word = q_lower.split()[0] if q_lower.split() else ""
    if q_word in ("who", "whom"):
        return "person/name", "question_word"
    elif q_word == "where":
        return "location/place", "question_word"
    elif q_word == "when":
        return "date/time", "question_word"
    elif q_word in ("how",) and "many" in q_lower:
        return "count", "question_word"

    return sel_col, "inferred"


def extract_training_data(split="dev"):
    """Extract structured training data from WikiSQL."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    tables = {}
    with open(WIKISQL_DIR / f"{split}.tables.jsonl") as f:
        for line in f:
            t = json.loads(line)
            tables[t["id"]] = t

    examples = []
    with open(WIKISQL_DIR / f"{split}.jsonl") as f:
        for line in f:
            examples.append(json.loads(line))

    decomposer_data = []
    spotter_data = []
    resolver_data = []

    for ex in examples:
        question = ex["question"]
        table = tables[ex["table_id"]]
        headers = table["header"]
        types = table["types"]
        sql = ex["sql"]
        conds = sql["conds"]

        if not conds:
            continue

        sel_idx = sql["sel"]
        sel_col = headers[sel_idx]
        sel_hint, sel_method = extract_select_hint(question, headers, sel_idx)

        for col_idx, op_idx, value in conds:
            where_col = headers[col_idx]
            where_op = COND_OPS[op_idx]
            value_type = classify_value_type(value, where_col, types[col_idx])
            value_in_q, value_pos = find_value_in_question(question, value)

            # QuestionDecomposer training
            decomposer_data.append({
                "question": question,
                "select_column": sel_col,
                "select_hint": sel_hint,
                "select_method": sel_method,
                "where_column": where_col,
                "where_value": str(value),
                "value_in_question": value_in_q,
                "headers": headers,
            })

            # ValueSpotter training
            spotter_data.append({
                "question": question,
                "value": str(value),
                "value_type": value_type,
                "value_in_question": value_in_q,
                "value_position": value_pos,
                "column_name": where_col,
            })

            # ColumnResolver training
            resolver_data.append({
                "value": str(value),
                "value_type": value_type,
                "correct_column": where_col,
                "correct_column_index": col_idx,
                "all_columns": [{"name": h, "type": t} for h, t in zip(headers, types)],
                "column_count": len(headers),
            })

    # Save
    with open(OUTPUT_DIR / f"decomposer_{split}.json", "w") as f:
        json.dump(decomposer_data, f)
    with open(OUTPUT_DIR / f"spotter_{split}.json", "w") as f:
        json.dump(spotter_data, f)
    with open(OUTPUT_DIR / f"resolver_{split}.json", "w") as f:
        json.dump(resolver_data, f)

    print(f"Extracted from {split} split:")
    print(f"  QuestionDecomposer: {len(decomposer_data)} examples")
    print(f"  ValueSpotter: {len(spotter_data)} examples")
    print(f"  ColumnResolver: {len(resolver_data)} examples")

    # Print stats
    print(f"\n  Value types:")
    vt_counts = Counter(d["value_type"] for d in spotter_data)
    for vt, c in vt_counts.most_common():
        print(f"    {vt}: {c}")

    print(f"\n  Value found in question: {sum(1 for d in spotter_data if d['value_in_question'])}/{len(spotter_data)}")

    print(f"\n  Select methods:")
    sm_counts = Counter(d["select_method"] for d in decomposer_data)
    for sm, c in sm_counts.most_common():
        print(f"    {sm}: {c}")

    return decomposer_data, spotter_data, resolver_data


if __name__ == "__main__":
    extract_training_data("dev")
    extract_training_data("train")
