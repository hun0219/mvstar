data = [
    ("1", '{"name": "John Doe", "age": 30}'),
    ("2", '{"city": "New York", "country": "USA", "zipcode": "10001"}'),
    ("3", '{"product": "Laptop", "brand": "Dell", "specs": {"RAM": "16GB", "Storage": "512GB SSD"}}')
]

df = spark.createDataFrame(data, ["id", "json_string"])
z.show(df)

dynamic_schema = spark.read.json(
            df.rdd.map(lambda row: row.json_string)
        ).schema

dynamic_schema

from pyspark.sql.functions import from_json, col
df = df.withColumn("json_struct", 
            from_json(col("json_string"), dynamic_schema)
        )

z.show(df)

from pyspark.sql.types import StructType, ArrayType

def get_json_keys(schema, prefix):
    keys = []
    for field in schema.fields:
        if isinstance(field.dataType, StructType):
            if prefix:
                new_prefix = f"{prefix}.{field.name}"
            else:
                new_prefix = field.name
            keys += get_json_keys(field.dataType, new_prefix)
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType): 
            if prefix:
                new_prefix = f"{prefix}.{field.name}"
            else:
                new_prefix = field.name
            keys += get_json_keys(field.dataType.elementType, new_prefix)
        else:
            if prefix:
                keys.append(f"{prefix}.{field.name}")
            else:
                keys.append(field.name)
    return keys

dynamic_schema

col_list = get_json_keys(dynamic_schema, 'json_struct')

col_list

type(col_list)

df.select("id", "json_string", "json_struct.age")

# GOAL
df.select("id", *col_list)

ccc = ["id", "json_string", "json_struct.age"]

df.select(*ccc)
