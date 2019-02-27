TEST_CONFIG = {
    'init_clients':          [],
    'task_client_config':    {
        'init_clients':     [],
        'dynamo_db_config': {
            'row_mapper':       {
                'hash_col':  'S',
                'range_col': 'N',
                'other_int': 'N',
                'attempts':  'N',
            },
            'required_fields':  ['hash_col'],
            'table_name':       'autotest_dynamo_db',
            'index_greenfield': 'autotest_index_int_int_index',
            'field_names':      {
                'task_id':     'hash_col',
                'labourer_id': 'range_col',
                'greenfield':  'other_int_col',
            }
        },
    },
    'ecology_client_config': {
        'test': True
    },
    'labourers':             {
            'some_function': {
                'arn': 'arn:aws:lambda:us-west-2:0000000000:function:some_function',
                'max_simultaneous_invocations': 10,
            }
    },
}
