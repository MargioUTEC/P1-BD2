from test_parser.core.parser.parser_sql import ParserSQL

sql = 'SELECT * FROM restaurants USING AVL WHERE city = "Taguig City"'

parser = ParserSQL()
tree = parser.parser.parse(sql)
print("\n=== ÁRBOL DEL PARSER ===")
print(tree.pretty())
