# Documentação do Serviço Financial Extract

## Visão Geral

O serviço `financial_extract` é uma solução para extração e processamento de dados financeiros, implementado como parte de uma arquitetura Medallion para um pipeline de dados na Google Cloud Platform (GCP). Este serviço é responsável pela extração de dados financeiros (cotações de ações) e seu armazenamento na camada Bronze de um Data Lakehouse.

## Arquitetura

O serviço está alinhado à primeira fase da arquitetura Medallion conforme solicitado no desafio técnico, sendo responsável pela ingestão e armazenamento dos dados brutos (camada Bronze):

1. **Extração**: O serviço é implementado como uma Cloud Function que extrai dados financeiros de tickers especificados
2. **Armazenamento**: Os dados são salvos em formato Parquet no Google Cloud Storage
3. **Orquestração**: O serviço é executado via requisições HTTP e registra metadados de execução em um banco PostgreSQL

## Componentes Tecnológicos

### Cloud Function
- **Nome**: financial_extract
- **Responsabilidade**: Extração de dados financeiros de ativos especificados na configuração do job
- **Runtime**: Python
- **Trigger**: Requisição HTTP
- **Entrada**: Configurações obtidas da tabela `ingestion_job_config` e última execução com status de sucesso da tabela `ingestion_job_execution`
- **Saída**: Arquivo Parquet no GCS com dados financeiros estruturados

### Cloud Storage
- **Funcionalidade**: Armazenamento dos arquivos Parquet contendo os dados financeiros extraídos
- **Organização**: Os arquivos são armazenados de acordo com o bucket especificado na configuração

### PostgreSQL
- **Funcionalidade**: Armazenamento de configurações e registro de execuções
- **Tabelas**:
  - `ingestion_job_config`: Armazena configurações para diferentes jobs de extração
  - `ingestion_job_execution`: Registra detalhes sobre cada execução do job

## Estrutura do Repositório

```
financial_extract/
├── Makefile                # Comandos para build e implantação
├── version                 # Arquivo contendo o número da versão atual
├── requirements.txt        # Dependências Python
├── main.py                 # Ponto de entrada da Cloud Function
├── service.py              # Implementação do serviço
└── ...
```

1. O código-fonte é mantido em um repositório GitHub
2. O Makefile no repositório permite gerar um arquivo ZIP versionado do código
3. O arquivo ZIP é armazenado no Cloud Storage
4. A Cloud Function é atualizada com a nova versão do código

## Modelo de Dados

### Dados Extraídos (Formato Parquet)

| Coluna      | Tipo      | Descrição                            |
|-------------|-----------|--------------------------------------|
| Date        | Datetime  | Data da cotação                      |
| Ticker      | string    | Símbolo da ação                      |
| Open        | float64   | Preço de abertura                    |
| Adj Close   | float64   | Preço de fechamento ajustado         |
| Close       | float64   | Preço de fechamento                  |
| High        | float64   | Preço mais alto do dia               |
| Low         | float64   | Preço mais baixo do dia              |
| Volume      | int64     | Volume de negociações                |

### Tabelas de Metadados PostgreSQL

#### Tabela: ingestion_job_config
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| job_name | VARCHAR(64) | Nome único do job (Chave Primária) |
| tickers | VARCHAR(1000) | Lista de símbolos dos ativos a serem extraídos |
| interval | INTEGER | Intervalo entre extrações |
| overlap | INTEGER | Sobreposição permitida entre extrações |
| bucket_name | VARCHAR(128) | Nome do bucket no GCP Storage |
| updated_by | VARCHAR(64) | Usuário que atualizou a configuração |
| created_at | TIMESTAMP | Data de criação do registro |
| updated_at | TIMESTAMP | Data da última atualização |

#### Tabela: ingestion_job_execution
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| id | SERIAL | ID sequencial (Chave Primária) |
| execution_id | VARCHAR(64) | ID único de execução vindo do Airflow |
| job_name | VARCHAR(64) | Nome do job executado |
| tickers | VARCHAR(1000) | Lista de símbolos processados |
| interval | INTEGER | Intervalo utilizado |
| overlap | INTEGER | Sobreposição utilizada |
| bucket_name | VARCHAR(128) | Bucket onde os dados foram armazenados |
| start_time | TIMESTAMP | Timestamp do início dos dados extraídos |
| end_time | TIMESTAMP | Timestamp do final dos dados extraídos |
| filename | VARCHAR(128) | Nome do arquivo gerado |
| records | INTEGER | Número de registros processados |
| status | VARCHAR(64) | Status da execução (ex: SUCCESS, FAILED) |
| created_at | TIMESTAMP | Data de criação do registro |
| updated_at | TIMESTAMP | Data da última atualização |

## Fluxo de Execução

1. Uma solicitação HTTP é enviada para a Cloud Function `financial_extract`
2. A função consulta a tabela `ingestion_job_config` para obter configurações
3. Cria um registro na tabela `ingestion_job_execution` com status inicial
4. Extrai dados financeiros para os tickers especificados
5. Converte os dados para formato parquet
6. Armazena o arquivo no bucket especificado no GCP Storage
7. Atualiza o registro de execução com informações de conclusão e status final

## Implantação

### Processo de Build

O serviço utiliza um arquivo Makefile para criar o arquivo compactado que deve carregado no Cloud Storage para criação da Cloud Function. O número da versão é definido no arquivo `version` localizado na raiz do repositório. O comando para geração do arquivo:

```bash
make config
```

O resultado deve ser a criação de um arquivo chamado `cf_financial_extract_v1.0.zip`, localizado no diretório imediatamente abaixo da pasta do projeto.

### Processo de Deploy

O processo de deploy pode ser realizado manualmente na console do GCP ou através dos comandos abaixo:

```bash
# Autenticação
gcloud auth login

# Configurar o projeto
gcloud config set project <PROJET_ID>

# Deploy da Cloud Function
gcloud functions deploy cf_financial_extract \
  --region=us-east4 \
  --runtime=python310 \
  --source=gs://<source-bucket>/cf_financial_extract_v1.0.zip \
  --entry-point=main \
  --trigger-http \
  --allow-unauthenticated \
  --memory=256MB
```

## Uso da API

### Parâmetros da Requisição HTTP

| Parâmetro | Tipo   | Obrigatório | Descrição                               |
|-----------|--------|-------------|------------------------------------------|
| project_id  | string | Sim         | Identificador do projeto               |
| job_name  | string | Sim         | Nome do job de extração                  |
| execution_id  | string (UUID) | Sim         | Identificador da execução do job     |



### Exemplo de Requisição

Para executar uma extração de dados:

```bash
curl -X POST https://[REGION]-[PROJECT_ID].cloudfunctions.net/financial_extract \
     -H "Content-Type: application/json" \
     -d '{"project_id": "9876543210", "job_name": "daily_stock_prices", "execution_id": "5a370b52-d012-4230-a05d-24d67c806585"}'
```

### Resposta

Sucesso, código HTTP 200 com body:
```json
{
  "status":       "success",
  "execution_id": "5a370b52-d012-4230-a05d-24d67c806585",
  "ingestion_id": "26588144-7cf2-4213-93d0-df9e65e71e5b",
  "records":      1024,
  "filename":     "financial_data_20250302.parquet",
}
```

Erro, código HTTP 400, 404, 500 ou 501 com body:
```json
{
  "status":       "error",
  "execution_id": "5a370b52-d012-4230-a05d-24d67c806585",
  "message":      "Exception getting...",
}
```

## Monitoramento e Logs

O serviço utiliza logs padrão do Google Cloud Functions que podem ser acessados através do Cloud Logging. Os registros de execução também são armazenados na tabela `ingestion_job_execution` no PostgreSQL para fins de auditoria e rastreamento.

## Considerações Operacionais

- O serviço deve ser monitorado para garantir a extração correta e completa dos dados
- É importante verificar a tabela `ingestion_job_execution` para identificar falhas de execução
- Recomenda-se configurar alertas para execuções com status 'FAILED'
- O dimensionamento adequado da Cloud Function deve ser considerado com base no volume de dados a serem extraídos

