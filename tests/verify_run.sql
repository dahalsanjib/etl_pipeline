-- =============================================================================
-- Run-result test. Run this in Snowsight AFTER the 13-17 Nov backfill.
-- Every row must say PASS (failures are listed first). The expected numbers
-- come from the sample data generator and were checked by hand.
-- =============================================================================
USE DATABASE RETAIL_DW;

WITH exp AS (
    -- feed, date, Bronze rows, clean (Silver) rows, rejected rows
    SELECT column1 AS FEED, TO_DATE(column2) AS D, column3 AS BRONZE, column4 AS CLEAN, column5 AS REJ
    FROM VALUES
        ('item_master',        '2026-11-13',  6,  6, 0),
        ('item_master',        '2026-11-14',  8,  6, 2),
        ('item_master',        '2026-11-15',  8,  6, 2),
        ('item_master',        '2026-11-16',  7,  6, 1),
        ('item_master',        '2026-11-17',  6,  6, 0),
        ('store_master',       '2026-11-13',  3,  3, 0),
        ('store_master',       '2026-11-15',  5,  3, 2),
        ('pos_sales',          '2026-11-13',  8,  8, 0),
        ('pos_sales',          '2026-11-14',  8,  7, 1),
        ('pos_sales',          '2026-11-15', 12,  6, 6),
        ('pos_sales',          '2026-11-16',  9,  5, 4),
        ('pos_sales',          '2026-11-17',  9,  4, 5),
        ('inventory_snapshot', '2026-11-13', 18, 18, 0),
        ('inventory_snapshot', '2026-11-14', 18, 18, 0),
        ('inventory_snapshot', '2026-11-15', 20, 18, 2),
        ('inventory_snapshot', '2026-11-16', 19, 16, 3),
        ('inventory_snapshot', '2026-11-17', 21, 18, 3)
),
bronze AS (
    SELECT 'item_master' AS FEED, FILE_DATE AS D, COUNT(*) AS N FROM STG.STG_ITEM_MASTER GROUP BY FILE_DATE
    UNION ALL SELECT 'store_master', FILE_DATE, COUNT(*) FROM STG.STG_STORE_MASTER GROUP BY FILE_DATE
    UNION ALL SELECT 'pos_sales', FILE_DATE, COUNT(*) FROM STG.STG_POS_SALES GROUP BY FILE_DATE
    UNION ALL SELECT 'inventory_snapshot', FILE_DATE, COUNT(*) FROM STG.STG_INVENTORY_SNAPSHOT GROUP BY FILE_DATE
),
clean AS (
    SELECT 'item_master' AS FEED, FILE_DATE AS D, COUNT(*) AS N FROM TMP.TMP_ITEM_MASTER GROUP BY FILE_DATE
    UNION ALL SELECT 'store_master', FILE_DATE, COUNT(*) FROM TMP.TMP_STORE_MASTER GROUP BY FILE_DATE
    UNION ALL SELECT 'pos_sales', FILE_DATE, COUNT(*) FROM TMP.TMP_POS_SALES GROUP BY FILE_DATE
    UNION ALL SELECT 'inventory_snapshot', FILE_DATE, COUNT(*) FROM TMP.TMP_INVENTORY_SNAPSHOT GROUP BY FILE_DATE
),
rej AS (
    SELECT FEED_NAME AS FEED, FILE_DATE AS D, COUNT(*) AS N FROM CTL.C_REJ_TABLE GROUP BY FEED_NAME, FILE_DATE
),
all_checks AS (
    -- Bronze, Silver and reject counts per feed and date (R7: Bronze = clean + rejected)
    SELECT 'BRONZE ' || e.FEED || ' ' || TO_VARCHAR(e.D) AS CHECK_NAME, e.BRONZE AS EXPECTED, COALESCE(b.N, 0) AS ACTUAL
    FROM exp e LEFT JOIN bronze b ON b.FEED = e.FEED AND b.D = e.D
    UNION ALL
    SELECT 'CLEAN ' || e.FEED || ' ' || TO_VARCHAR(e.D), e.CLEAN, COALESCE(c.N, 0)
    FROM exp e LEFT JOIN clean c ON c.FEED = e.FEED AND c.D = e.D
    UNION ALL
    SELECT 'REJECTED ' || e.FEED || ' ' || TO_VARCHAR(e.D), e.REJ, COALESCE(r.N, 0)
    FROM exp e LEFT JOIN rej r ON r.FEED = e.FEED AND r.D = e.D
    -- Gold final state
    UNION ALL SELECT 'DIM_SUBCLASS rows', 3, (SELECT COUNT(*) FROM TGT.DIM_SUBCLASS)
    UNION ALL SELECT 'DIM_DISTRICT rows', 3, (SELECT COUNT(*) FROM TGT.DIM_DISTRICT)
    UNION ALL SELECT 'DIM_ITEM rows', 7, (SELECT COUNT(*) FROM TGT.DIM_ITEM)
    UNION ALL SELECT 'DIM_ITEM current rows', 6, (SELECT COUNT(*) FROM TGT.DIM_ITEM WHERE CURR_FLG = 'Y')
    UNION ALL SELECT 'DIM_ITEM I1003 versions', 2, (SELECT COUNT(*) FROM TGT.DIM_ITEM WHERE ITEM_ID = 'I1003')
    UNION ALL SELECT 'DIM_LOCN rows', 4, (SELECT COUNT(*) FROM TGT.DIM_LOCN)
    UNION ALL SELECT 'DIM_LOCN current rows', 3, (SELECT COUNT(*) FROM TGT.DIM_LOCN WHERE CURR_FLG = 'Y')
    UNION ALL SELECT 'DIM_LOCN S003 versions', 2, (SELECT COUNT(*) FROM TGT.DIM_LOCN WHERE STORE_ID = 'S003')
    UNION ALL SELECT 'DIM_DATE rows', 5844, (SELECT COUNT(*) FROM TGT.DIM_DATE)
    UNION ALL SELECT 'FCT_SALES rows', 30, (SELECT COUNT(*) FROM TGT.FCT_SALES)
    UNION ALL SELECT 'FCT_SALES valid date keys', 30, (SELECT COUNT(*) FROM TGT.FCT_SALES f JOIN TGT.DIM_DATE d ON f.DATE_KEY = d.DATE_KEY)
    UNION ALL SELECT 'FCT_SALES net amount', 13575, (SELECT COALESCE(SUM(NET_SALES_AMT), 0) FROM TGT.FCT_SALES)
    UNION ALL SELECT 'FCT_INVENTORY rows', 88, (SELECT COUNT(*) FROM TGT.FCT_INVENTORY)
    UNION ALL SELECT 'FCT_INVENTORY open rows', 18, (SELECT COUNT(*) FROM TGT.FCT_INVENTORY WHERE CURR_FLG = 'Y')
    -- Control tables
    UNION ALL SELECT 'C_BATCH_LOG FAILED steps', 0, (SELECT COUNT(*) FROM CTL.C_BATCH_LOG WHERE STATUS = 'FAILED')
    UNION ALL SELECT 'C_BATCH_LOG WARNING steps (R4 rejects)', 4, (SELECT COUNT(*) FROM CTL.C_BATCH_LOG WHERE STATUS = 'WARNING')
    UNION ALL SELECT 'C_BATCH_LOG total steps', 69, (SELECT COUNT(*) FROM CTL.C_BATCH_LOG)
    UNION ALL SELECT 'C_LOAD_DATES targets at 2026-11-17', 12, (SELECT COUNT(*) FROM CTL.C_LOAD_DATES WHERE LAST_LOAD_DATE = '2026-11-17')
    UNION ALL SELECT 'C_LOAD_DATES targets at 2026-11-15', 2, (SELECT COUNT(*) FROM CTL.C_LOAD_DATES WHERE LAST_LOAD_DATE = '2026-11-15')
)
SELECT CHECK_NAME, EXPECTED, ACTUAL, IFF(EXPECTED = ACTUAL, 'PASS', 'FAIL') AS STATUS
FROM all_checks
ORDER BY STATUS, CHECK_NAME;
