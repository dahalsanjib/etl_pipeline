# Quality gates and failure actions

Every rule has a layer boundary (where it is checked) and a failure action. Rejected rows are never dropped: they go to `CTL.C_REJ_TABLE` with the rule id, reason code and the original row.

| Rule | Check | Boundary | Failure action |
|---|---|---|---|
| R1 | Required file arrived and has at least one data row | Source to Bronze (STG) | **Fail the run.** The sensor times out after 10 minutes or the load task raises. Nothing is loaded. |
| R2 | Business keys present (`ITEM_ID`, `STORE_ID`, `TXN_ID`/line, `SUBCLASS_ID`, `DISTRICT_ID`) | Bronze to Silver (TMP) | **Reject the row**, rule id `R2`. |
| R3 | Values cast to the right type (numbers, dates) | Bronze to Silver | **Reject the row**, reason `INVALID_VALUE`. |
| R4 | Item and store exist in the clean masters | Bronze to Silver | **Reject the row and alert.** The Silver step is logged as `WARNING` in `CTL.C_BATCH_LOG`. The run continues. |
| R5 | Ranges: no negative quantity, price, discount or cost, no zero quantity on sales, discount not above gross, net = qty x price - discount, transaction/snapshot date = file date | Bronze to Silver | **Reject the row**, rule id `R5`. |
| R6 | No duplicate keys in a file (first row by `SRC_ROW_NUM` is kept) | Bronze to Silver | **Reject the later rows**, rule id `R6`. |
| R7 | Bronze rows = Silver rows + rejected rows, per feed and date | Silver (end of step) | **Fail the run.** Rows were lost or duplicated. |
| R8 | Gold reconciles: row counts, amounts, orphans, freshness, duplicates | Gold (TGT), after the facts load | **Fail the run.** `CTL.C_LOAD_DATES` is not updated. |
| Guard | Run date is not older than the date the Gold target already loaded | Silver to Gold | **Fail before changing Gold** ("Out-of-order load blocked"). |

## Reason codes by feed

| Feed | Reason codes |
|---|---|
| item_master | `NULL_ITEM_ID`, `NULL_SUBCLASS_ID`, `INVALID_VALUE`, `NEGATIVE_AMOUNT`, `DUPLICATE_KEY` |
| store_master | `NULL_STORE_ID`, `NULL_DISTRICT_ID`, `DUPLICATE_KEY` |
| pos_sales | `NULL_TXN_KEY`, `NULL_STORE_ID`, `NULL_ITEM_ID`, `INVALID_VALUE`, `ITEM_NOT_IN_MASTER`, `UNKNOWN_STORE`, `NEGATIVE_QTY`, `ZERO_QTY`, `NEGATIVE_PRICE`, `NEGATIVE_DISCOUNT`, `DISCOUNT_TOO_HIGH`, `NET_MISMATCH`, `TXN_DATE_MISMATCH`, `DUPLICATE_LINE` |
| inventory_snapshot | `NULL_STORE_ID`, `NULL_ITEM_ID`, `INVALID_VALUE`, `ITEM_NOT_IN_MASTER`, `UNKNOWN_STORE`, `NEGATIVE_QTY`, `SNAPSHOT_DATE_MISMATCH`, `DUPLICATE_KEY` |

A row with several problems gets the first reason in the order above (rule order).

## R8 reconciliation checks (`include/sql/control/reconcile.sql`)

`ROWCOUNT_SALES`, `AMOUNT_SALES` (Gold net + rejected raw net = source file net), `ORPHANS_SALES`, `FRESHNESS_SALES`, `DUPLICATES_SALES`, `ROWCOUNT_INVENTORY`, `POSITIONS_INVENTORY`, `DUPLICATES_INVENTORY`.
