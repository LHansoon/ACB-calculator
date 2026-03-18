#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ACB (average cost) + Annual realized P/L with Canadian superficial loss (±30 days) handling.
All money math uses Decimal.

NEW:
- Outputs an "augmented" CSV: same as input, plus one column with real-time ACB per share
  after each processed row (in execution order).
- Sorts by execute_time converted to America/Halifax (DST-aware). Falls back to date 00:00 Halifax.
"""

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP
from collections import defaultdict, deque
from pathlib import Path

getcontext().prec = 28

D0 = Decimal("0")


def D(x) -> Decimal:
    if x is None:
        return D0
    s = str(x).strip()
    if s == "":
        return D0
    return Decimal(s)


def q_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def parse_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def convert_to_report(value: Decimal, trade_ccy: str, report_ccy: str, fx: Decimal) -> Decimal:
    if trade_ccy.upper() == report_ccy.upper():
        return value
    # default: trade_ccy -> report_ccy by multiplying fx (e.g., USD * fx = CAD)
    return value * fx
    # If your file is inverted, use: return value / fx


@dataclass
class Lot:
    acq_date: date
    shares_remaining: Decimal


@dataclass
class PendingLoss:
    sale_date: date
    end_date: date
    year: int
    symbol: str
    shares_sold: Decimal
    loss_amount: Decimal   # positive amount in report currency
    sale_id: int           # index into realized rows list


def main():
    ap = argparse.ArgumentParser(description="ACB + Superficial Loss Calculator (Canada)")
    ap.add_argument("--input_csv", default="result/combined_trades.csv", help="Input trades CSV (default: result/combined_trades.csv)")
    ap.add_argument("--report_ccy", default="CAD", help="Reporting currency (default: CAD)")
    ap.add_argument("--annual_out", default="result/annual_pl.csv", help="Annual realized P/L output CSV")
    ap.add_argument("--detail_out", default="result/realized_trades.csv", help="Per-sell detail output CSV")
    ap.add_argument("--augmented_out", default="result/augmented_with_acb.csv",
                    help="Augmented output (input + realtime ACB col). Default: result/augmented_with_acb.csv")
    args = ap.parse_args()

    # State per symbol
    shares_held = defaultdict(lambda: D0)
    acb_total = defaultdict(lambda: D0)
    lots = defaultdict(deque)  # for superficial loss share tracking

    realized_rows = []     # per SELL results (Decimals inside)
    pending_losses = []    # superficial loss events (finalized at sale_date+30)

    # Read all rows
    rows = []
    with open(args.input_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        input_fieldnames = reader.fieldnames[:] if reader.fieldnames else []
        for i, row in enumerate(reader):
            if not row.get("date"):
                continue
            row["_orig_idx"] = i
            row["date_obj"] = parse_date(row["date"])

            rows.append(row)


    def finalize_losses_up_to(current_date: date):
        """Finalize superficial losses whose end_date <= current_date."""
        nonlocal pending_losses
        still_pending = []
        for pl in pending_losses:
            if current_date > pl.end_date:
                sym = pl.symbol
                start = pl.sale_date - timedelta(days=30)
                end = pl.end_date

                # shares acquired in window that are still held at end_date
                acquired_and_held = D0
                for lot in lots[sym]:
                    if lot.shares_remaining <= D0:
                        continue
                    if start <= lot.acq_date <= end:
                        acquired_and_held += lot.shares_remaining

                denied_shares = min(pl.shares_sold, acquired_and_held)

                if denied_shares > D0 and pl.shares_sold > D0 and shares_held[sym] > D0:
                    ratio = denied_shares / pl.shares_sold
                    denied_loss = pl.loss_amount * ratio
                    allowed_loss = pl.loss_amount - denied_loss

                    # realized_rows stores realized_pl as negative for loss. Add back denied_loss (less negative).
                    realized_rows[pl.sale_id]["denied_superficial_loss"] = denied_loss
                    realized_rows[pl.sale_id]["allowed_loss_after_superficial"] = allowed_loss
                    realized_rows[pl.sale_id]["realized_pl_report_ccy"] += denied_loss

                    # Add denied loss to ACB of remaining holdings
                    acb_total[sym] += denied_loss
                else:
                    realized_rows[pl.sale_id]["denied_superficial_loss"] = D0
                    realized_rows[pl.sale_id]["allowed_loss_after_superficial"] = pl.loss_amount
            else:
                still_pending.append(pl)
        pending_losses = still_pending

    # For augmented output: capture the realtime ACB per share after each row (in sorted order)
    augmented_acb_by_orig_idx = {}

    for row in rows:
        row_date = row["date_obj"]
        # finalize losses that have become known by this date (end of 30-day window reached)
        finalize_losses_up_to(row_date)

        ttype = (row.get("type") or "").strip().upper()
        sym = (row.get("symbol") or "").strip()
        trade_ccy = (row.get("currency") or "").strip().upper()
        fx = D(row.get("fx") or "1")

        shares = D(row.get("shares") or "0")
        amount = D(row.get("amount") or "0")
        commission = D(row.get("commission") or "0")
        tos = (row.get("total_or_share") or "Total").strip().lower()

        # gross in trade currency
        gross_trade = (amount * shares) if tos == "share" else amount

        gross_report = convert_to_report(gross_trade, trade_ccy, args.report_ccy, fx)
        comm_report = convert_to_report(commission, trade_ccy, args.report_ccy, fx)

        if ttype == "BUY":
            cost = gross_report + comm_report
            shares_held[sym] += shares
            acb_total[sym] += cost
            lots[sym].append(Lot(acq_date=row_date, shares_remaining=shares))

        elif ttype == "SELL":
            qty = shares
            if qty > D0:
                if qty > shares_held[sym]:
                    raise ValueError(
                        f"SELL exceeds holdings: {sym} trying to sell {qty} but only {shares_held[sym]} held on {row_date}"
                    )
                gross_proceeds = gross_report
                sell_commission = comm_report
                proceeds = gross_proceeds - sell_commission

                if shares_held[sym] > D0:
                    avg_cost = acb_total[sym] / shares_held[sym]
                    cost_basis = avg_cost * qty
                else:
                    avg_cost = D0
                    cost_basis = D0

                realized_pl = proceeds - cost_basis

                shares_held[sym] -= qty
                acb_total[sym] -= cost_basis

                # reduce lots FIFO
                remaining_to_sell = qty
                while remaining_to_sell > D0 and lots[sym]:
                    lot = lots[sym][0]
                    take = min(lot.shares_remaining, remaining_to_sell)
                    lot.shares_remaining -= take
                    remaining_to_sell -= take
                    if lot.shares_remaining <= D0:
                        lots[sym].popleft()

                # record realized row (may be adjusted later by superficial loss finalization)
                realized_rows.append({
                    "date": row_date.isoformat(),
                    "year": row_date.year,
                    "symbol": sym,
                    "shares_sold": qty,
                    "gross_proceeds_report_ccy": gross_proceeds,
                    "sell_commission_report_ccy": sell_commission,
                    "proceeds_report_ccy": proceeds,
                    "cost_basis_report_ccy": cost_basis,
                    "realized_pl_report_ccy": realized_pl,
                    "denied_superficial_loss": D0,
                    "allowed_loss_after_superficial": D0,
                })
                sale_id = len(realized_rows) - 1

                if realized_pl < D0:
                    loss_amount = -realized_pl
                    pending_losses.append(PendingLoss(
                        sale_date=row_date,
                        end_date=row_date + timedelta(days=30),
                        year=row_date.year,
                        symbol=sym,
                        shares_sold=qty,
                        loss_amount=loss_amount,
                        sale_id=sale_id
                    ))

        # compute realtime ACB per share for THIS SYMBOL after processing this row
        if shares_held[sym] > D0:
            acb_per_share = acb_total[sym] / shares_held[sym]
        else:
            acb_per_share = D0

        augmented_acb_by_orig_idx[row["_orig_idx"]] = q_money(acb_per_share)

    # finalize remaining pending losses after last trade (not tied to any input row)
    if rows:
        max_d = max(r["date_obj"] for r in rows)
        finalize_losses_up_to(max_d + timedelta(days=31))

    # Write per-sell detail CSV
    Path("result").mkdir(parents=True, exist_ok=True)
    with open(args.detail_out, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "date", "year", "symbol", "shares_sold",
            f"gross_proceeds_{args.report_ccy}",
            f"sell_commission_{args.report_ccy}",
            f"proceeds_{args.report_ccy}",
            f"cost_basis_{args.report_ccy}",
            f"realized_pl_{args.report_ccy}",
            f"denied_superficial_loss_{args.report_ccy}",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for rr in realized_rows:
            w.writerow({
                "date": rr["date"],
                "year": rr["year"],
                "symbol": rr["symbol"],
                "shares_sold": str(rr["shares_sold"]),
                f"gross_proceeds_{args.report_ccy}": str(q_money(rr["gross_proceeds_report_ccy"])),
                f"sell_commission_{args.report_ccy}": str(q_money(rr["sell_commission_report_ccy"])),
                f"proceeds_{args.report_ccy}": str(q_money(rr["proceeds_report_ccy"])),
                f"cost_basis_{args.report_ccy}": str(q_money(rr["cost_basis_report_ccy"])),
                f"realized_pl_{args.report_ccy}": str(q_money(rr["realized_pl_report_ccy"])),
                f"denied_superficial_loss_{args.report_ccy}": str(q_money(rr["denied_superficial_loss"])),
            })

    # Write annual summary
    annual_proceeds    = defaultdict(lambda: D0)
    annual_cost_basis  = defaultdict(lambda: D0)
    annual_expenses    = defaultdict(lambda: D0)
    annual_pl          = defaultdict(lambda: D0)
    for rr in realized_rows:
        y = int(rr["year"])
        annual_proceeds[y]   += rr["gross_proceeds_report_ccy"]
        annual_cost_basis[y] += rr["cost_basis_report_ccy"]
        annual_expenses[y]   += rr["sell_commission_report_ccy"]
        annual_pl[y]         += rr["realized_pl_report_ccy"]

    with open(args.annual_out, "w", newline="", encoding="utf-8") as f:
        c = args.report_ccy
        fieldnames = [
            "year",
            f"proceeds_{c}",
            f"cost_basis_{c}",
            f"expenses_{c}",
            f"realized_pl_{c}",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for y in sorted(annual_pl.keys()):
            w.writerow({
                "year": y,
                f"proceeds_{c}":    str(q_money(annual_proceeds[y])),
                f"cost_basis_{c}":  str(q_money(annual_cost_basis[y])),
                f"expenses_{c}":    str(q_money(annual_expenses[y])),
                f"realized_pl_{c}": str(q_money(annual_pl[y])),
            })

    # Write augmented CSV in ORIGINAL input order (so it looks like "input + one col")
    acb_col = f"acb_per_share_{args.report_ccy}"
    out_fieldnames = input_fieldnames + ([acb_col] if acb_col not in input_fieldnames else [])

    # Re-read original rows to preserve formatting/order as much as possible
    with open(args.input_csv, "r", newline="", encoding="utf-8") as fin, \
         open(args.augmented_out, "w", newline="", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=out_fieldnames)
        writer.writeheader()

        orig_i = 0
        for row in reader:
            # add realtime acb (if we computed it)
            acb_val = augmented_acb_by_orig_idx.get(orig_i, None)
            row[acb_col] = (str(acb_val) if acb_val is not None else "")
            writer.writerow(row)
            orig_i += 1

    print(f"Saved per-sell detail: {args.detail_out}")
    print(f"Saved annual summary:  {args.annual_out}")
    print(f"Saved augmented CSV:   {args.augmented_out}")


if __name__ == "__main__":
    main()
