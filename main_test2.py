from pyspark.sql import DataFrame
from pyspark.sql.functions import col

#функция, которая возвращает датафрейм с парами «Имя продукта – Имя категории» и имена всех продуктов, у которых нет категорий
def get_product_category(products_df: DataFrame, categories_df: DataFrame, name_product_not_categories_df: DataFrame) -> DataFrame:

    #соединение продуктов с их категориями 
    products_with_categories = (
        products_df.join(
            name_product_not_categories_df, 
            on = 'product_id',
            how = 'left'
        )
        .join(
            categories_df,
            on = 'category_id',
            how = 'left'
        )
        .select(
            col('products_name'),
            col('category_name')
        )

    )

    return products_with_categories
