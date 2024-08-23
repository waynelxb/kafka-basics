SCHEMA_STR = '{"type":"record","name":"value_wikimedia","namespace":"wikimedia","fields":[{"name":"bot","type":["null","boolean"],"default":null},{"name":"comment","type":["null","string"],"default":null},{"name":"id","type":["null","int"],"default":null},{"name":"length","type":["null",{"type":"record","name":"Length","fields":[{"name":"new","type":["null","int"],"default":null},{"name":"old","type":["null","int"],"default":null}]}],"default":null},{"name":"meta","type":["null",{"type":"record","name":"Meta","fields":[{"name":"domain","type":["null","string"],"default":null},{"name":"dt","type":["null","string"],"default":null},{"name":"id","type":["null","string"],"default":null},{"name":"offset","type":["null","long"],"default":null},{"name":"partition","type":["null","int"],"default":null},{"name":"request_id","type":["null","string"],"default":null},{"name":"stream","type":["null","string"],"default":null},{"name":"topic","type":["null","string"],"default":null},{"name":"uri","type":["null","string"],"default":null}]}],"default":null},{"name":"minor","type":["null","boolean"],"default":null},{"name":"namespace","type":["null","int"],"default":null},{"name":"parsedcomment","type":["null","string"],"default":null},{"name":"patrolled","type":["null","boolean"],"default":null},{"name":"revision","type":["null",{"type":"record","name":"Revision","fields":[{"name":"new","type":["null","int"],"default":null},{"name":"old","type":["null","int"],"default":null}]}],"default":null},{"name":"schema","type":["null","string"],"doc":"Theoriginalfieldnamewas\'$schema\'butsomecharactersisnotacceptedinthefieldnameofAvrorecord","default":null},{"name":"server_name","type":["null","string"],"default":null},{"name":"server_script_path","type":["null","string"],"default":null},{"name":"server_url","type":["null","string"],"default":null},{"name":"timestamp","type":["null","int"],"default":null},{"name":"title","type":["null","string"],"default":null},{"name":"type","type":["null","string"],"default":null},{"name":"user","type":["null","string"],"default":null},{"name":"wiki","type":["null","string"],"default":null}]}'


# SCHEMA_STR = """
# {"type":"record","name":"value_wikimedia","namespace":"wikimedia",
# "fields":[
#     {"name":"bot","type":["null","boolean"],"default":null},
#     {"name":"comment","type":["null","string"],"default":null},
#     {"name":"id","type":["null","int"],"default":null},
#     {"name":"length","type":["null",{"type":"record","name":"Length","fields":[
# 																				{"name":"new","type":["null","int"],"default":null},
# 																				{"name":"old","type":["null","int"],"default":null}]}],"default":null},
#     {"name":"meta","type":["null",{"type":"record","name":"Meta","fields":[
#                                     {"name":"domain","type":["null","string"],"default":null},
#                                     {"name":"dt","type":["null","string"],"default":null},
#                                     {"name":"id","type":["null","string"],"default":null},
#                                     {"name":"offset","type":["null","long"],"default":null},
#                                     {"name":"partition","type":["null","int"],"default":null},
#                                     {"name":"request_id","type":["null","string"],"default":null},
#                                     {"name":"stream","type":["null","string"],"default":null},
#                                     {"name":"topic","type":["null","string"],"default":null},
#                                     {"name":"uri","type":["null","string"],"default":null}]}],"default":null},
# 	{"name":"minor","type":["null","boolean"],"default":null},
# 	{"name":"namespace","type":["null","int"],"default":null},
#     {"name":"parsedcomment","type":["null","string"],"default":null},
#     {"name":"patrolled","type":["null","boolean"],"default":null},
#     {"name":"revision","type":["null",{"type":"record","name":"Revision","fields":[
# 																					{"name":"new","type":["null","int"],"default":null},
# 																					{"name":"old","type":["null","int"],"default":null}]}],"default":null},
#     {"name":"schema","type":["null","string"],"doc":"Theoriginalfieldnamewas\'$schema\'butsomecharactersisnotacceptedinthefieldnameofAvrorecord","default":null},
# 	{"name":"server_name","type":["null","string"],"default":null},
# 	{"name":"server_script_path","type":["null","string"],"default":null},
# 	{"name":"server_url","type":["null","string"],"default":null},
#     {"name":"timestamp","type":["null","int"],"default":null},
#     {"name":"title","type":["null","string"],"default":null},
#     {"name":"type","type":["null","string"],"default":null},
#     {"name":"user","type":["null","string"],"default":null},
#     {"name":"wiki","type":["null","string"],"default":null}]
# }"""













# SCHEMA_STR = """
# {
#     "type": "record",
#     "namespace": "com.kubertenes1",
#     "name": "AvroDeployment",
#     "fields": [
#         {"name": "Date", "type": "string"},
#         {"name": "Open", "type": "float"},
#         {"name": "High", "type": "float"},
#         {"name": "Low", "type": "float"},
#         {"name": "Close", "type": "float"},
#         {"name": "Volume", "type": "float", "default": 0.0},
#         {"name": "Dividends", "type": "float", "default": 0.0},
#         {"name": "StockSplits", "type": "float", "default": 0.0}
#     ]
# }
# """