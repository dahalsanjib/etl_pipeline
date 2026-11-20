import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

START = date(2026, 11, 13)
DAYS = 5
STORE_FILE_DAYS = {1, 3}

SUBCLASSES = {"SC10": "Grocery", "SC20": "Dairy", "SC30": "Personal Care"}

ITEMS = [
    ("I1001", "Rice 1kg", "SC10", "Himal", 120, 150),
    ("I1002", "Sugar 1kg", "SC10", "Himal", 95, 120),
    ("I1003", "Milk 1L", "SC20", "DairyBest", 85, 110),
    ("I1004", "Curd 500g", "SC20", "DairyBest", 70, 95),
    ("I1005", "Soap", "SC30", "CleanCo", 40, 60),
    ("I1006", "Shampoo 200ml", "SC30", "CleanCo", 200, 280),
]

DISTRICTS = {"D01": "Kathmandu", "D02": "Lalitpur", "D03": "Bhaktapur"}

STORES = [
    ("S001", "Thamel Store", "Kathmandu", "D01"),
    ("S002", "Patan Store", "Lalitpur", "D02"),
    ("S003", "Bhaktapur Store", "Bhaktapur", "D03"),
]


def day_date(day_no):
    return START + timedelta(days=day_no - 1)


def item_rows(day_no):
    rows = []
    for item_id, name, sc, brand, cost, price in ITEMS:
        sc_name = SUBCLASSES[sc]
        if day_no >= 4 and item_id == "I1003":
            brand, price = "DairyPrime", 120
        if day_no >= 4 and sc == "SC30":
            sc_name = "Personal & Home Care"
        rows.append([item_id, name, sc, sc_name, brand, cost, price, "ACTIVE"])
    return rows


def store_rows(day_no):
    rows = []
    for store_id, name, city, district_id in STORES:
        if day_no >= 3 and store_id == "S003":
            district_id = "D02"
        rows.append([store_id, name, city, district_id, DISTRICTS[district_id]])
    return rows


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def sale_line(txn_no, line_no, d, store, item, qty, price, discount=0):
    txn_id = f"T{d:%Y%m%d}{txn_no:03d}"
    return [txn_id, line_no, d.isoformat(), store, item, qty, price, discount, qty * price - discount]


def with_value(row, index, value):
    row = list(row)
    row[index] = value
    return row


def sales_extras(day_no, d, sales):
    standardised = []
    rejects = []
    if day_no == 2:
        standardised.append(sale_line(5, 1, d, " s002", "i1004 ", 2, 95))
        rejects.append((sale_line(6, 1, d, "", "I1001", 1, 150), "NULL_STORE_ID"))
    if day_no == 3:
        rejects.append((sale_line(5, 1, d, "S001", "", 2, 150), "NULL_ITEM_ID"))
        rejects.append((sale_line(6, 1, d, "S002", "", 1, 60), "NULL_ITEM_ID"))
        rejects.append((sale_line(7, 1, d, "S003", "I1002", -2, 120), "NEGATIVE_QTY"))
        rejects.append((sale_line(8, 1, d, "S001", "I9999", 3, 99), "ITEM_NOT_IN_MASTER"))
        rejects.append((list(sales[0]), "DUPLICATE_LINE"))
        rejects.append((with_value(sale_line(9, 1, d, "S001", "I1001", 2, 150), 5, "abc"), "INVALID_VALUE"))
    if day_no == 4:
        rejects.append((sale_line(5, 1, d, "S999", "I1001", 1, 150), "UNKNOWN_STORE"))
        rejects.append((sale_line(6, 1, d, "S001", "I1006", 2, -280), "NEGATIVE_PRICE"))
        rejects.append((sale_line(7, 1, d, "S002", "I1005", 3, 60, -20), "NEGATIVE_DISCOUNT"))
        rejects.append((sale_line(8, 1, d, "S003", "I1001", 0, 150), "ZERO_QTY"))
    if day_no == 5:
        rejects.append((with_value(sale_line(5, 1, d, "S001", "I1002", 2, 120), 2, (d - timedelta(days=1)).isoformat()), "TXN_DATE_MISMATCH"))
        rejects.append((with_value(sale_line(6, 1, d, "S002", "I1003", 2, 120), 8, 280), "NET_MISMATCH"))
        rejects.append((sale_line(7, 1, d, "S003", "I1005", 2, 60, 500), "DISCOUNT_TOO_HIGH"))
        rejects.append((with_value(sale_line(8, 1, d, "S001", "I1004", 1, 95), 0, ""), "NULL_TXN_KEY"))
        rejects.append((with_value(sale_line(9, 1, d, "S002", "I1001", 1, 150), 2, "2026-13-45"), "INVALID_VALUE"))
    return standardised, rejects


def item_extras(day_no, items):
    extras = []
    if day_no == 2:
        extras.append((list(items[1]), "DUPLICATE_KEY"))
        extras.append((["I1007", "Tea 250g", "SC10", "Grocery", "Himal", 40, -50, "ACTIVE"], "NEGATIVE_AMOUNT"))
    if day_no == 3:
        extras.append((["I1008", "Biscuit", "SC10", "Grocery", "Himal", "xyz", 30, "ACTIVE"], "INVALID_VALUE"))
        extras.append((["I1009", "Noodles", "", "", "Himal", 20, 35, "ACTIVE"], "NULL_SUBCLASS_ID"))
    if day_no == 4:
        extras.append((["", "Unknown item", "SC10", "Grocery", "Himal", 10, 15, "ACTIVE"], "NULL_ITEM_ID"))
    return extras


def store_extras(day_no, stores):
    extras = []
    if day_no == 3:
        extras.append((list(stores[1]), "DUPLICATE_KEY"))
        extras.append((["S004", "Lakeside Store", "Pokhara", "", ""], "NULL_DISTRICT_ID"))
    return extras


def inventory_extras(day_no, d, inventory):
    extras = []
    iso = d.isoformat()
    if day_no == 3:
        extras.append(([iso, "S002", "", 15, 0], "NULL_ITEM_ID"))
        extras.append(([iso, "", "I1003", 15, 0], "NULL_STORE_ID"))
    if day_no == 4:
        extras.append(([iso, "S001", "I9999", 12, 0], "ITEM_NOT_IN_MASTER"))
        extras.append(([iso, "S001", "I1001", "abc", 0], "INVALID_VALUE"))
        extras.append(([iso, "S001", "I1003", -5, 0], "NEGATIVE_QTY"))
    if day_no == 5:
        extras.append(([iso, "S777", "I1001", 20, 0], "UNKNOWN_STORE"))
        base = next(r for r in inventory if r[1] == "S002" and r[2] == "I1002")
        extras.append((list(base), "DUPLICATE_KEY"))
        extras.append(((with_value([iso, "S003", "I1006", 30, 0], 0, (d - timedelta(days=1)).isoformat())), "SNAPSHOT_DATE_MISMATCH"))
    return extras


def feed_stats(total_rows, rejects):
    reasons = {}
    for _, reason in rejects:
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "bronze_rows": total_rows,
        "clean_rows": total_rows - len(rejects),
        "reject_rows": len(rejects),
        "reject_reasons": reasons,
    }


def main():
    out = Path("data/source")
    out.mkdir(parents=True, exist_ok=True)
    Path("tests").mkdir(exist_ok=True)
    rng = random.Random(2026)
    manifest = {"days": {}}

    for day_no in range(1, DAYS + 1):
        d = day_date(day_no)
        stamp = f"{d:%Y%m%d}"
        items = item_rows(day_no)
        price_of = {r[0]: r[6] for r in items}
        feeds = {}

        item_ext = item_extras(day_no, items)
        item_all = items + [r for r, _ in item_ext]
        write_csv(out / f"item_master_{stamp}.csv",
                  ["ITEM_ID", "ITEM_NAME", "SUBCLASS_ID", "SUBCLASS_NAME", "BRAND", "UNIT_COST", "UNIT_PRICE", "STATUS"],
                  item_all)
        feeds["item_master"] = feed_stats(len(item_all), item_ext)

        if day_no in STORE_FILE_DAYS:
            stores = store_rows(day_no)
            store_ext = store_extras(day_no, stores)
            store_all = stores + [r for r, _ in store_ext]
            write_csv(out / f"store_master_{stamp}.csv",
                      ["STORE_ID", "STORE_NAME", "CITY", "DISTRICT_ID", "DISTRICT_NAME"],
                      store_all)
            feeds["store_master"] = feed_stats(len(store_all), store_ext)

        sales = []
        for txn_no in range(1, 5):
            store = rng.choice(STORES)[0]
            for line_no in range(1, rng.randint(1, 2) + 1):
                item = rng.choice(ITEMS)[0]
                sales.append(sale_line(txn_no, line_no, d, store, item, rng.randint(1, 5), price_of[item]))

        standardised, rejects = sales_extras(day_no, d, sales)
        all_sales = sales + standardised + [r for r, _ in rejects]
        write_csv(out / f"pos_sales_{stamp}.csv",
                  ["TXN_ID", "TXN_LINE_NO", "TXN_DATE", "STORE_ID", "ITEM_ID", "QTY", "UNIT_PRICE", "DISCOUNT_AMT", "NET_SALES_AMT"],
                  all_sales)
        source_net = sum(r[8] for r in all_sales)
        reject_net = sum(r[0][8] for r in rejects)
        sales_stats = feed_stats(len(all_sales), rejects)
        sales_stats.update({
            "standardised_rows": len(standardised),
            "source_net": source_net,
            "reject_net": reject_net,
            "clean_net": source_net - reject_net,
        })
        feeds["pos_sales"] = sales_stats

        inventory = []
        for store_id, *_ in STORES:
            for item_id, *_ in ITEMS:
                inventory.append([d.isoformat(), store_id, item_id, rng.randint(10, 50), rng.choice([0, 0, 20])])
        if day_no == 4:
            inventory = [r for r in inventory if not (r[1] == "S002" and r[2] in ("I1005", "I1006"))]
        inv_ext = inventory_extras(day_no, d, inventory)
        inv_all = inventory + [r for r, _ in inv_ext]
        write_csv(out / f"inventory_snapshot_{stamp}.csv",
                  ["SNAPSHOT_DATE", "STORE_ID", "ITEM_ID", "ONHAND_QTY", "ONORDER_QTY"],
                  inv_all)
        feeds["inventory_snapshot"] = feed_stats(len(inv_all), inv_ext)

        manifest["days"][d.isoformat()] = feeds

    totals = {}
    for feeds in manifest["days"].values():
        for name, stats in feeds.items():
            t = totals.setdefault(name, {"bronze_rows": 0, "clean_rows": 0, "reject_rows": 0})
            for key in ("bronze_rows", "clean_rows", "reject_rows"):
                t[key] += stats[key]
            if name == "pos_sales":
                for key in ("source_net", "reject_net", "clean_net"):
                    t[key] = t.get(key, 0) + stats[key]
    manifest["totals"] = totals
    Path("tests/expected_totals.json").write_text(json.dumps(manifest, indent=2))
    print("Done: files in data/source, answer key in tests/expected_totals.json")


if __name__ == "__main__":
    main()