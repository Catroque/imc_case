CREATE EXTERNAL TABLE projetct_id.silver_layer.financial_data
(
    date 		TIMESTAMP OPTIONS(description = 'Data da cotação'),
    ticker 		STRING  OPTIONS(description = 'Símbolo da ação'),
    open 		INTEGER OPTIONS(description = 'Preço de abertura'),
    adj_close 	INTEGER OPTIONS(description = 'Preço de fechamento ajustado'),
    close       INTEGER OPTIONS(description = 'Preço de fechamento'),
    high 		INTEGER OPTIONS(description = 'Preço mais alto do dia'),
    low 		INTEGER OPTIONS(description = 'Preço mais baixo do dia'),
    volume 		INTEGER OPTIONS(description = 'Volume de negociações')
)
OPTIONS 
(
	uris = ['gs://{bucket_name}/*.parquet']
	format = 'PARQUET',
	hive_partition_uri_prefix = 'gs://[bucket_name]',
	require_hive_partition_filter = false
);
