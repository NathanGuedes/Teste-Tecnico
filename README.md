# Teste Técnico

## 1.1. Acesso à API de Dados Abertos da ANS
Na etapa 1.1, para que os dados pudessem ser acessados pelo software, foi necessário realizar o parse do HTML fornecido pelo link presente no PDF, uma vez que se trata de um repositório de arquivos FTP.

O projeto foi desenvolvido em Java 17 e utilizou a biblioteca Jsoup para realizar o parse do conteúdo HTML disponível em
https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/
.

Com isso, o sistema conseguiu identificar os diretórios existentes no repositório, verificar qual deles era o mais recente e retorná-lo. Em seguida, o conteúdo desse diretório foi acessado para identificar e processar os arquivos trimestrais mais recentes.

## 1.2. Processamento de Arquivos
Na etapa 2, o sistema realizou o download dos arquivos trimestrais mais recentes identificados na etapa anterior, utilizando a classe ArchiveHandler. Os arquivos compactados (.zip) foram baixados a partir do repositório da ANS e extraídos automaticamente para um diretório local do projeto.

Após a extração, os arquivos foram listados e lidos utilizando a API java.nio.file do Java 17. Em seguida, a classe FileIOService aplicou um filtro nos arquivos de dados, selecionando apenas os registros cuja coluna DESCRICAO corresponde a “Despesas com Eventos/Sinistros”, conforme o delimitador definido.

### Trade-off técnico:

A decisão pelo método incremental foi tomada como medida preventiva, considerando que não há conhecimento prévio das dimensões dos arquivos nem da memória disponível
Esta abordagem garante maior flexibilidade e segurança no processamento de arquivos de diferentes tamanhos evitando possível estouro de memória.

## 1.3. Consolidação e Análise de Inconsistências & 2.2. Enriquecimento de Dados com Tratamento de Falhas

Os arquivos CSV disponibilizados pela ANS continham apenas os campos DATA, REG_ANS, CD_CONTA_CONTABIL, DESCRICAO, VL_SALDO_INICIAL e VL_SALDO_FINAL. Informações como Ano, Trimestre, CNPJ, Razão Social, UF e Modalidade não estavam presentes no arquivo original por esse motivos descidi adiantar na etapa 2.2 para enrriquecer esses dados.

Por esse motivo, foi necessário gerar as colunas Ano e Trimestre a partir do campo DATA, que estava no formato yyyy-mm-dd. Esse processamento foi realizado no método addYearAndQuarterColumns, e registros com datas inválidas ou ausentes não tiveram essas informações calculadas.

Durante a consolidação dos três trimestres, foram encontrados valores zerados ou negativos e registros com inconsistências cadastrais. Esses dados não foram removidos nem corrigidos automaticamente, sendo mantidos para garantir fidelidade ao dado original. A consolidação e preservação dos registros ocorre durante o fluxo de processamento dos arquivos e na geração das colunas calculadas, especialmente no método calculateNewColumn.

No enriquecimento cadastral, o relacionamento entre os dados financeiros e cadastrais foi feito utilizando o CNPJ como chave, por meio do método mergeCsvByKey. Registros sem correspondência no cadastro da ANS foram mantidos com campos vazios. Quando um mesmo CNPJ apareceu mais de uma vez no cadastro com informações diferentes, foi considerada a última ocorrência processada durante o merge.

Essas decisões visam manter o processo simples, transparente e alinhado aos dados realmente disponíveis na fonte oficial, evitando inferências ou correções que poderiam distorcer a informação original.