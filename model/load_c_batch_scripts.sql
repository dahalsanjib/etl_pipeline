-- Register the scripts the DAG runs, in order. Documentation table: the DAG reads feeds.yaml, not this table.
USE DATABASE RETAIL_DW;
TRUNCATE TABLE CTL.C_BATCH_SCRIPTS;
INSERT INTO CTL.C_BATCH_SCRIPTS (SCRIPT_NAME, STEP_ORDER, TARGET_NAME) VALUES
    ('silver/tmp_item_master.sql',        1,  'TMP.TMP_ITEM_MASTER'),
    ('silver/tmp_store_master.sql',       2,  'TMP.TMP_STORE_MASTER'),
    ('silver/tmp_pos_sales.sql',          3,  'TMP.TMP_POS_SALES'),
    ('silver/tmp_inventory_snapshot.sql', 4,  'TMP.TMP_INVENTORY_SNAPSHOT'),
    ('gold/tgt_dim_subclass.sql',         5,  'TGT.DIM_SUBCLASS'),
    ('gold/tgt_dim_district.sql',         6,  'TGT.DIM_DISTRICT'),
    ('gold/tgt_dim_item.sql',             7,  'TGT.DIM_ITEM'),
    ('gold/tgt_dim_locn.sql',             8,  'TGT.DIM_LOCN'),
    ('gold/tgt_fct_sales.sql',            9,  'TGT.FCT_SALES'),
    ('gold/tgt_fct_inventory.sql',        10, 'TGT.FCT_INVENTORY'),
    ('control/reconcile.sql',             11, 'TGT (gate R8)');
SELECT * FROM CTL.C_BATCH_SCRIPTS ORDER BY STEP_ORDER;
