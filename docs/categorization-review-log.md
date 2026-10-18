# Categorization review log

Manual review passes over the Uncategorized bucket. Each entry: date, the
decision (who/what), the rule registered, and the backfill applied.
Rules are stored in `finance.category_rules` (source=`manual`); backfills
move only transactions currently in the Uncategorized bucket, then
`monthly_category_summary` is rebuilt (same SQL as the pipeline).

## 2026-10-17 — pass 1 (chuck, via session review)

| Payee (pattern / match) | Category | Note |
|---|---|---|
| `BYU Echeck` / exact | Education → Tuition (105) | BYU tuition eChecks ($7,096) |
| `ATM` / exact | Entertainment (21) | cash withdrawals — chuck: "almost always" entertainment |
| `PayPal` / exact | Shopping (71) | PayPal purchases |
| `PayPal Purchase` / exact | Transfer (78) | INST XFER = funding the PayPal balance, not a purchase |
| `Cash Dd` / contains | Health & Fitness → Dentist (39) | Gary L Cash DDS via Payrix (payee embeds a date; contains on the name) |
| `Seagate` / contains | Shopping → Electronics & Software (69) | computer purchase |
| `Casago` / contains | Travel → Vacation (84) | family lake vacation (AirBnB/hotel) |
| `For Love and Le` / contains | Shopping → Clothing (68) | dresses (spring dance, fall formal) |
| `Check #2623` / exact | Home → Home Improvement (48) | new AC unit ($13,091) |
| `Check #2618` / exact | Taxes → Property Tax (409) | property taxes ($9,619) |
| `Check #2614` / exact | Education (18) | college guidance / SAT prep — kid expense ($4,500) |
| `Check #2631` / exact | Health & Fitness → Doctor (40) | Round Rock ER ($1,043) |
| `Check #2624` / exact | Kids → Kids Activities (59) | cheer camp ($400) |
| `Check #2620` / exact | Financial → Life Insurance (26) | term life insurance ($924) |
| `P F Chang` / contains | Food & Dining → Restaurants (34) | restaurant (via Olo) |
| `Olo.com` / contains | Food & Dining → Restaurants (34) | restaurant delivery platform |
| `Deckers` / contains | Shopping → Clothing (68) | shoes (Ugg) |

Result: Uncategorized 12-month spend $41.0k → $2.8k (93% resolved).
Remaining tail: mostly older `Check #NNNN` (SHARE CHECK) payments whose
payee is unknown from the export — need chuck's memory to resolve.

## Conventions

- Rules are case-sensitive (engine matches in Python); patterns use the
  exact payee casing as it appears in exports.
- One-off check numbers use `exact` on the full `Check #NNNN` payee.
- Recurring merchants use `contains` on a stable name fragment.
- Backfills are scoped to the Uncategorized bucket so bank-provided or
  previously reviewed categories are never overwritten.