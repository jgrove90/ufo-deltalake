<h1>UFO Sighting Data ETL Pipeline</h1>

<p>This project showcases an Extract, Load, Transform (ELT) pipeline built with Python, Apache Spark, Delta Lake, and Docker. The objective of the project is to scrape UFO sighting data from the National UFO Reporting Center (NUFORC) and process it through the Medallion architecture to create a star schema in the Gold layer.</p>

<p>The pipeline begins by utilizing Python for web scraping, extracting UFO sighting data from NUFORC. The scraped data is then transformed using Apache Spark, a powerful big data processing framework. Spark enables efficient data manipulation, cleansing, and aggregation tasks on the extracted data.</p>

<p>To ensure reliability and scalability, the data is stored in Delta Lake, an open-source storage layer built on top of Apache Parquet and Apache Spark. Delta Lake provides ACID transactions, schema enforcement, and versioning capabilities, making it ideal for data pipeline workflows.</p>

<p>The project is containerized using Docker, allowing for easy deployment and reproducibility across different environments. Docker enables seamless packaging and distribution of the entire pipeline, ensuring consistent execution and dependency management.</p>

<p>The result is a well-organized ELT pipeline that follows the Medallion architecture principles, with Bronze, Silver, and Gold layers. The Bronze layer contains the raw, unprocessed data. The Silver layer represents the transformed and cleansed data, while the Gold layer consists of a star schema, enabling efficient querying and analysis.</p>

<h3>Key Technologies:</h3>
<ul>
  <li>Python: Web scraping and scripting language</li>
  <li>Apache Spark: Big data processing and transformation framework</li>
  <li>Delta Lake: Data storage layer with ACID transactions and versioning</li>
  <li>Docker: Containerization platform for easy deployment and reproducibility</li>
</ul>

<p>By leveraging the power of Python, Apache Spark, Delta Lake, and Docker, this project provides a robust and scalable solution for extracting, transforming, and loading UFO sighting data into a star schema for advanced analysis and insights.</p>

