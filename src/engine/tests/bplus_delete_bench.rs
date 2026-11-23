// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "B+ batched delete and serializer old/new/new/old; release single thread"]
fn bplus_delete_buffer_cost() {
    bplus_delete_cost(false, false, false);
}

#[test]
#[ignore = "B+ delete page projection ABBA; release single thread"]
fn bplus_delete_read_cost() {
    bplus_delete_cost(true, false, false);
}

#[test]
#[ignore = "B+ packed leaf deletion ABBA; release single thread"]
fn bplus_delete_leaf_cost() {
    bplus_delete_cost(false, true, false);
}
