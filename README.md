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

### Trade-off técnico 1.2:

A decisão pelo método incremental foi tomada como medida preventiva, considerando que não há conhecimento prévio das dimensões dos arquivos nem da memória disponível
Esta abordagem garante maior flexibilidade e segurança no processamento de arquivos de diferentes tamanhos evitando possível estouro de memória.
ências ou correções que poderiam distorcer a informação original.

## 1.3.  Consolidação e Análise de Inconsistência && Diferentes && 2.2. Enriquecimento de Dados com Tratamento de Falhas


Nesta etapa era esperadoa consolidacao dos dados da etapa anterior em um novo csv com os campos CNPJ , RazaoSocial , Trimestre , Ano , ValorDespesas, contudo os dados vindo do link apresentado no PDF nao havia nenhum desses campos, descidi por adiantar o passo 2.2 onde baixei os arquivo do link https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_
saude_ativas/ onde realisei uma maescla dos arquivo realizando um JOIN relacionando a chave "REG_ANS" do arquivo original e "REGISTRO_OPERADORA" do arquivo que foi baixado do link apresentado anteriormente, foi gerado um arquivo onde os campos neles presente foram "DATA", "CNPJ", "RAZAO_SOCIAL", "DESCRICAO", "TRIMESTRE", "ANO", "VL_SALDO_INICIAL", "VL_SALDO_FINAL", "VALOR_DESPESAS", "REG_ANS", "MODALIDADE", "UF";"CNPJ_VALIDO", por conta da desisao de adiantar a estapa 2.2 que representa a adição de  "RegistroANS" , "Modalidade".

## 2.1 Validação de Dados com Estratégias

Na estapa 2.1 após a consolidação e o enriquecimento dos dados, foi aplicada uma etapa de validação para assegurar a consistência e a qualidade do CSV final. Foram validados o CNPJ (formato e dígitos verificadores, com indicação na coluna CNPJ_VALIDO), a Razão Social como campo obrigatório e os valores numéricos, garantindo que apenas valores positivos fossem considerados.

Durante o processo, registros com CNPJ inválido foram mantidos, recebendo uma anotação no campo OBSERVACAO, enquanto registros com Razão Social não preenchida ou valores numéricos iguais a zero ou negativos foram removidos, resultando em um conjunto de dados final consistente e adequado para análise

### Trade-off técnico 2.1

Foi adotada uma estratégia distinta para cada validação, de acordo com o impacto do campo na qualidade dos dados. Registros com CNPJ inválido foram mantidos, com indicação no campo OBSERVACAO, preservando informações que ainda podem ser úteis para análise.

Já os registros com valores numéricos não positivos foram removidos, por comprometerem a confiabilidade dos cálculos, garantindo a consistência do dataset final.

### Trade-off técnico 2.2:

O CSV da direita é carregado em memória como um HashMap, indexado pela chave, enquanto o CSV da esquerda é percorrido linha a linha para realizar o merge. Essa abordagem permite buscas rápidas por chave, com complexidade O(1) por registro, além de ser simples de implementar e manter. Quando um registro do CSV da esquerda não possui correspondência no CSV da direita, as colunas do segundo arquivo são preenchidas como vazias e a coluna OBSERVACAO recebe a mensagem "REGISTRO_NAO_ENCONTRADO". O uso de memória é considerado aceitável para o tamanho esperado dos arquivos neste projeto; para volumes muito grandes de dados, outras estratégias, como processamento por streaming ou uso de banco de dados, seriam mais adequadas.