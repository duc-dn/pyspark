#### PySpark map() transformation
---
- map() is an RDD transformation that is used to apply the transformation function (lambda) on every element of RDD/DataFrame and returns a new RDD.
- RDD map() tranformation is used to apply any complex operations like adding a column, updating a column, transforming the data
- Note:
  - Note1: DataFrame doesn't have map() transformation to use with DataFrame. Therefore, you need to DataFrame to RDD first.

#### PySpark map() transformation with DataFrame
- PySpark DataFrame doesn't have map() transformation to apply the lambda function, when you wanted to apply the custome transformation, you need to convert the DataFrame to RDD and apply the map() transformation.