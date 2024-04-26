# Speed and Performance for our Data Pipeline

We derived our data pipeline using Polars. Polars is a data processing platform similar to pandas demonstrating superior speed and performance, especially when dealing with large datasets. Polars is built using Rust, which allows for highly efficient execution and parallelization across multiple processors. In contrast, pandas is primarily built on Python and NumPy, while being vectorized, is slower for some fundamental operations on dataframes.

## Notable Performance Improvements offered by Polars

### Parallelization 

Polars can safely utilize all available cores on a machine for complex operations involving multiple columns, providing a massive boost over pandas which is limited to single-core operations. 

### Memory Efficiency 

Polars requires significantly less memory (2-4 times the dataset size) compared to pandas (5-10 times the dataset size) for carrying out operations, thus reducing unnecessary runtime overhead.

### Larger Dataset Handling

Polars can handle larger datasets before running into out-of-memory errors compared to pandas because of the aforementioned reduction in memory usage. 

## Apache Arrow Integration
 
### Data Representation 

Polars is built on top of the Apache Arrow data format, which provides a standardized, efficient in-memory data representation. Arrow integration allows polars to avoid costly data serialization/deserialization and enables better interoperability with other data tools and databases that also use Arrow. 