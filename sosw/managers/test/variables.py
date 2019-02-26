TEST_CONFIG = {
    'init_clients':            [],
    'dynamo_db_config': {
        'row_mapper':       {
            'hash_col':  'S',
            'range_col': 'N',
            'other_int': 'N',
        },
        'required_fields':  ['hash_col'],
        'table_name':       'autotest_dynamo_db',
        'index_greenfield': 'autotest_index_int_int_index',
        'field_names':      {
            'task_id':    'hash_col',
            'worker_id':  'range_col',
            'greenfield': 'other_int_col',
        }
    },
}
