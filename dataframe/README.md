#### DataFrame Distributed
---
```
data = [("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)
```
- Bản thân DF không có partitions nên muốn biết DF có bao nhiêu partitions, ta gọi qua RDD của nó. Khi dataframe được tạo, nó sẽ được spark partion thành các partion khác nhau (không chuyển thành RDD) các partion này sẽ chứa các row của dataframe có format như sau 
  `Row(first_name='Maria', middle_name='Anne', last_name='Jones', dob='39192', gender='F', salary=500000)`
- Mỗi partition trong DataFrame sẽ chứa một tập hợp các hàng (rows) của DataFrame. Trong quá trình tính toán, các partition sẽ được xử lý độc lập trên các node trong cụm, cho phép Spark thực hiện tính toán song song trên từng partition.
- Khi bạn chuyển đổi một DataFrame thành RDD trong PySpark bằng phương thức rdd(), kết quả trả về sẽ là một RDD chứa các hàng của DataFrame ban đầu `df.rdd()`. VD:
```
Row(first_name='James', middle_name='', last_name='Smith', dob='36636', gender='M', salary=60000)
Row(first_name='Michael', middle_name='Rose', last_name='', dob='40288', gender='M', salary=70000)
Row(first_name='Robert', middle_name='', last_name='Williams', dob='42114', gender='', salary=400000)
Row(first_name='Maria', middle_name='Anne', last_name='Jones', dob='39192', gender='F', salary=500000)
Row(first_name='Jen', middle_name='Mary', last_name='Brown', dob='', gender='F', salary=0)
```