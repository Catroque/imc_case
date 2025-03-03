CREATE TABLE projetct_id.gold_layer.financial_data_hour
(
    ticker 		STRING  OPTIONS(description = 'Símbolo da ação'),
    date 		TIMESTAMP OPTIONS(description = 'Data da cotação'),
    volume 		INTEGER OPTIONS(description = 'Volume de negociações')
    execution_id STRING OPTIONS(description = 'Identificador de execução do job de ingestão'),
)
PARTITIONS 
(
    DATE(date)
)
CLUSTER BY
    ticker,
    execution_id
;