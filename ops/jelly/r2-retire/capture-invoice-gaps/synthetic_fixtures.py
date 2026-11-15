"""Offline fabricated billing pages; these are never provider evidence."""
from pathlib import Path


def baseline(root, gaps):
    root = Path(root).resolve()
    data = root / "baseline"
    data.mkdir(mode=0o700)
    account = {"account": {"uuid": "SYNTHETIC", "team": {"uuid": gaps.TEAM, "name": "AY"}}}

    def rows(count):
        return [{"product": "SYNTHETIC", "category": "SYNTHETIC", "description": "SYNTHETIC-%d" % n,
                 "amount": str(n), "duration": "1", "duration_unit": "SYNTHETIC"} for n in range(count)]

    bodies = {"account.json": {"body": account}, "capture-lifetimes.json": []}
    for month, invoice, number, _, total in gaps.PAGES:
        body = {"invoice_items": rows(total), "meta": {"total": total}, "links": {}}
        bodies["invoice-full-2026-%s.json" % month] = {"invoice_uuid": invoice, "invoice_items": body["invoice_items"]}
        if number == 2:
            body["invoice_items"] = body["invoice_items"][:200]
            body["links"] = {"pages": {"next": "https://api.digitalocean.com/v2/customers/my/invoices/%s?page=2&per_page=200" % invoice}}
            bodies["invoice-2026-%s.json" % month] = {"body": body}
    for month, count in gaps.COMPLETE_MONTHS.items():
        bodies["invoice-2026-%s.json" % month] = {
            "status": 200, "request_id": "SYNTHETIC", "path": "/v2/customers/my/invoices/00000000-0000-0000-0000-%012d" % int(month),
            "body": {"invoice_items": rows(count), "meta": {"total": count}, "links": {}}}
    pins = {}
    for name, value in bodies.items():
        raw = gaps.canonical(value)
        gaps.write_new(data / name, raw)
        pins[name] = {"sha256": gaps.digest(raw), "size_bytes": len(raw)}
    pins_file = root / "pins.json"
    gaps.write_new(pins_file, gaps.canonical(pins))
    return data, pins_file
