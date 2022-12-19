def getConfigurations():
    response = {
        "table_arn": "arn:aws:dynamodb:us-east-2:1234567890:table/table1",
        "another_table_arn": "arn:aws:dynamodb:us-east-2:1234567890:table/table2",
        "gsi_index_name": "main_index",
        "another_table_gsi_index_name": "another_table_index",
        "TARGET_ACC_TABLE_NAME": "main_table",
        "TARGET_ACC_REGION": "us-east-2",
        "TARGET_ACC_HMS_TABLE_NAME": "target_table",
        "target_account_id": "0987654321",
        "target_account_role_name": "AccessRoleForMainAcc",
    }
    return response
