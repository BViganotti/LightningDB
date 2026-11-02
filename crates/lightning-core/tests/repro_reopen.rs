use arrow::array::Array;
use lightning_core::{Database, SystemConfig};
use std::collections::BTreeSet;

fn scan_ids(db: &std::sync::Arc<Database>) -> BTreeSet<i64> {
    let res = db
        .connect()
        .execute("MATCH (t:T) RETURN t.id", None)
        .unwrap();
    let mut out = BTreeSet::new();
    for b in &res.batches {
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for i in 0..a.len() {
            if a.is_valid(i) {
                out.insert(a.value(i));
            }
        }
    }
    out
}

#[test]
fn repro_checkpoint_reopen_must_not_change_results() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let db = Database::new(&path, SystemConfig::default()).unwrap();
    let c = db.connect();
    let stmts = [
        "CREATE NODE TABLE T(id INT64, v INT64, s STRING, PRIMARY KEY (id))",
        "CREATE (:T {id: 24, v: 9859, s: 'v9859'})",
        "CREATE (:T {id: 11, v: 9870, s: 'v9870'})",
        "CREATE (:T {id: 54, v: 620, s: 'v620'})",
        "CREATE (:T {id: 8, v: 5472, s: 'v5472'})",
        "CREATE (:T {id: 44, v: 7208, s: 'v7208'})",
        "CREATE (:T {id: 22, v: 2607, s: 'v2607'})",
        "CREATE (:T {id: 33, v: 6708, s: 'v6708'})",
        "CREATE (:T {id: 18, v: 9660, s: 'v9660'})",
        "MATCH (t:T {id: 18}) SET t.v = 3089, t.s = 'u3089'",
        "CREATE (:T {id: 1, v: 8303, s: 'v8303'})",
        "MATCH (t:T {id: 1}) SET t.v = 7459, t.s = 'u7459'",
        "CREATE (:T {id: 50, v: 3455, s: 'v3455'})",
        "MATCH (t:T) DELETE t",
        "CREATE (:T {id: 10, v: 1096, s: 'v1096'})",
        "CREATE (:T {id: 59, v: 1205, s: 'v1205'})",
        "CREATE (:T {id: 3, v: 4400, s: 'v4400'})",
        "MATCH (t:T {id: 3}) DELETE t",
        "CREATE (:T {id: 27, v: 3880, s: 'v3880'})",
        "CREATE (:T {id: 15, v: 7426, s: 'v7426'})",
        "CREATE (:T {id: 16, v: 9282, s: 'v9282'})",
        "CREATE (:T {id: 34, v: 8558, s: 'v8558'})",
        "CREATE (:T {id: 35, v: 7706, s: 'v7706'})",
        "CREATE (:T {id: 54, v: 5472, s: 'v5472'})",
        "CREATE (:T {id: 49, v: 8715, s: 'v8715'})",
        "CREATE (:T {id: 4, v: 4996, s: 'v4996'})",
        "CREATE (:T {id: 31, v: 1807, s: 'v1807'})",
        "MATCH (t:T {id: 4}) DELETE t",
        "CREATE (:T {id: 36, v: 5447, s: 'v5447'})",
        "CREATE (:T {id: 13, v: 4697, s: 'v4697'})",
        "CREATE (:T {id: 7, v: 6262, s: 'v6262'})",
        "MATCH (t:T {id: 7}) SET t.v = 6814, t.s = 'u6814'",
        "CREATE (:T {id: 19, v: 807, s: 'v807'})",
        "CREATE (:T {id: 45, v: 6979, s: 'v6979'})",
        "MATCH (t:T {id: 49}) SET t.v = 4270, t.s = 'u4270'",
        "CREATE (:T {id: 57, v: 3771, s: 'v3771'})",
        "CREATE (:T {id: 37, v: 1645, s: 'v1645'})",
        "MATCH (t:T {id: 59}) SET t.v = 2770, t.s = 'u2770'",
        "CREATE (:T {id: 62, v: 5485, s: 'v5485'})",
        "MATCH (t:T {id: 59}) SET t.v = 790, t.s = 'u790'",
        "CREATE (:T {id: 38, v: 9120, s: 'v9120'})",
        "CREATE (:T {id: 9, v: 2012, s: 'v2012'})",
        "CREATE (:T {id: 0, v: 7731, s: 'v7731'})",
        "CREATE (:T {id: 28, v: 627, s: 'v627'})",
        "CREATE (:T {id: 41, v: 1169, s: 'v1169'})",
        "CREATE (:T {id: 56, v: 2650, s: 'v2650'})",
        "CREATE (:T {id: 26, v: 3893, s: 'v3893'})",
        "CREATE (:T {id: 14, v: 8836, s: 'v8836'})",
        "CREATE (:T {id: 29, v: 8343, s: 'v8343'})",
        "CREATE (:T {id: 21, v: 9634, s: 'v9634'})",
        "MATCH (t:T {id: 29}) SET t.v = 7780, t.s = 'u7780'",
        "MATCH (t:T {id: 0}) SET t.v = 4902, t.s = 'u4902'",
        "CREATE (:T {id: 47, v: 1071, s: 'v1071'})",
        "CREATE (:T {id: 43, v: 7541, s: 'v7541'})",
        "MATCH (t:T) DELETE t",
        "CREATE (:T {id: 45, v: 8588, s: 'v8588'})",
        "CREATE (:T {id: 26, v: 6952, s: 'v6952'})",
        "CREATE (:T {id: 35, v: 8997, s: 'v8997'})",
        "CREATE (:T {id: 19, v: 9877, s: 'v9877'})",
        "CREATE (:T {id: 38, v: 9533, s: 'v9533'})",
        "CREATE (:T {id: 46, v: 8575, s: 'v8575'})",
        "CREATE (:T {id: 12, v: 4141, s: 'v4141'})",
        "CREATE (:T {id: 3, v: 8350, s: 'v8350'})",
        "CREATE (:T {id: 14, v: 2034, s: 'v2034'})",
        "CREATE (:T {id: 57, v: 7197, s: 'v7197'})",
        "MATCH (t:T) DELETE t",
        "CREATE (:T {id: 53, v: 6440, s: 'v6440'})",
        "CREATE (:T {id: 59, v: 1259, s: 'v1259'})",
        "CREATE (:T {id: 11, v: 500, s: 'v500'})",
        "CREATE (:T {id: 55, v: 3182, s: 'v3182'})",
        "CREATE (:T {id: 54, v: 6298, s: 'v6298'})",
        "CREATE (:T {id: 40, v: 6461, s: 'v6461'})",
        "CREATE (:T {id: 17, v: 4655, s: 'v4655'})",
        "MATCH (t:T {id: 59}) SET t.v = 9805, t.s = 'u9805'",
        "MATCH (t:T {id: 54}) SET t.v = 7128, t.s = 'u7128'",
        "CREATE (:T {id: 7, v: 8258, s: 'v8258'})",
        "CREATE (:T {id: 34, v: 7533, s: 'v7533'})",
        "CREATE (:T {id: 61, v: 2711, s: 'v2711'})",
        "CREATE (:T {id: 38, v: 4447, s: 'v4447'})",
        "CREATE (:T {id: 21, v: 8271, s: 'v8271'})",
        "CREATE (:T {id: 50, v: 7395, s: 'v7395'})",
        "CREATE (:T {id: 48, v: 523, s: 'v523'})",
        "CREATE (:T {id: 32, v: 4273, s: 'v4273'})",
        "MATCH (t:T {id: 21}) DELETE t",
        "CREATE (:T {id: 41, v: 2412, s: 'v2412'})",
        "MATCH (t:T {id: 40}) SET t.v = 473, t.s = 'u473'",
        "MATCH (t:T) DELETE t",
        "CREATE (:T {id: 62, v: 2032, s: 'v2032'})",
        "CREATE (:T {id: 6, v: 3576, s: 'v3576'})",
        "MATCH (t:T {id: 62}) SET t.v = 5017, t.s = 'u5017'",
        "CREATE (:T {id: 7, v: 4071, s: 'v4071'})",
        "CREATE (:T {id: 18, v: 6601, s: 'v6601'})",
        "CREATE (:T {id: 41, v: 1248, s: 'v1248'})",
        "CREATE (:T {id: 55, v: 1182, s: 'v1182'})",
        "CREATE (:T {id: 35, v: 1141, s: 'v1141'})",
        "CREATE (:T {id: 47, v: 9229, s: 'v9229'})",
        "CREATE (:T {id: 48, v: 4147, s: 'v4147'})",
        "CREATE (:T {id: 54, v: 9595, s: 'v9595'})",
        "MATCH (t:T) DELETE t",
    ];
    for s in stmts {
        c.execute(s, None).unwrap();
    }
    drop(c);
    let before = scan_ids(&db);
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::new(&path, SystemConfig::default()).unwrap();
    let after = scan_ids(&db);
    assert_eq!(before, after, "checkpoint+reopen changed the visible rows");
}
