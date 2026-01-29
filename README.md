# Teste Técnico

## 1.1. Acesso à API de Dados Abertos da ANS
Na etapa 1.1, para que os dados pudessem ser acessados pelo software, foi necessário realizar o parse do HTML fornecido pelo link presente no PDF, uma vez que se trata de um repositório de arquivos FTP.

O projeto foi desenvolvido em Java 17 e utilizou a biblioteca Jsoup para realizar o parse do conteúdo HTML disponível em
https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/
.

Com isso, o sistema conseguiu identificar os diretórios existentes no repositório, verificar qual deles era o mais recente e retorná-lo. Em seguida, o conteúdo desse diretório foi acessado para identificar e processar os arquivos trimestrais mais recentes.