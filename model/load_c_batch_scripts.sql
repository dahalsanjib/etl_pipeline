-- Register the scripts the DAG runs, in order. Documentation table: the DAG reads feeds.yaml, not this table.
USE DATABASE RETAIL_DW;
TRUNCATE TABLE CTL.C_BATCH_SCRIPTS;
INSERT INTO CTL.C_BATCH_SCRIPTS (SCRIPT_NAME, STEP_ORDER, TARGET_NAME) VALUES
    ('tmp_item_master.sql',        1,  'TMP.TMP_ITEM_MASTER'),
    ('tmp_store_master.sql',       2,  'TMP.TMP_STORE_MASTER'),
    ('tmp_pos_sales.sql',          3,  'TMP.TMP_POS_SALES'),
    ('tmp_inventory_snapshot.sql', 4,  'TMP.TMP_INVENTORY_SNAPSHOT'),
    ('tgt_dim_subclass.sql',       5,  'TGT.DIM_SUBCLASS'),
    ('tgt_dim_district.sql',       6,  'TGT.DIM_DISTRICT'),
    ('tgt_dim_item.sql',           7,  'TGT.DIM_ITEM'),
    ('tgt_dim_locn.sql',           8,  'TGT.DIM_LOCN'),
    ('tgt_fct_sales.sql',          9,  'TGT.FCT_SALES'),
    ('tgt_fct_inventory.sql',      10, 'TGT.FCT_INVENTORY'),
    ('reconcile.sql',              11, 'TGT (gate R8)');
SELECT * FROM CTL.C_BATCH_SCRIPTS ORDER BY STEP_ORDER;
