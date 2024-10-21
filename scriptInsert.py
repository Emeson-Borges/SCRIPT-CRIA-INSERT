import psycopg2
import datetime

# Função para formatar os valores em uma string SQL amigável
def format_value(value):
    if value is None:
        return 'NULL'
    elif isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, datetime.date):
        return f"'{value}'"
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, bytes):
        return f"'\\x{value.hex()}'"
    else:
        raise ValueError(f"Tipo de dado não suportado: {type(value)}")

def generate_insert_sql(table_name, columns, values):
    formatted_values = [format_value(value) for value in values]
    columns_str = ', '.join(columns)
    values_str = ', '.join(formatted_values)
    
    return f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"

# Conexão com o PostgreSQL
try:
    connection = psycopg2.connect(
        host="localhost",
        database="santoantoniodotaua_backup",
        user="postgres",
        password="161213"
    )
    
    cursor = connection.cursor()

    # Executar a consulta
    query = "SELECT * FROM esocial.s1200 WHERE perapur='2022-10';"
    cursor.execute(query)

    # Obter nomes das colunas
    columns = [desc[0] for desc in cursor.description]

    # Abrir o arquivo para escrita
    with open('sqlS1200.sql', 'w') as file:
        # Processar cada linha de resultado
        rows = cursor.fetchall()
        for row in rows:
            insert_sql = generate_insert_sql('esocial.s1200', columns, row)
            file.write(insert_sql + '\n')  # Escrever cada linha no arquivo

    print("Arquivo sqlS1200.sql gerado com sucesso.")

except Exception as error:
    print(f"Erro ao acessar o banco de dados: {error}")

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
