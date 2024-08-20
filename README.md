# pece_usp_especializacao
Repositório para trabalhos na Especialização em Eng de Dados e Big Data pela Poli USP (PECE)

# Trabalho 2
https://github.com/leander-silveira/Poli_USP_Ingestao_de_dados/tree/main/Trabalho_2

# Trabalho 3
[Google Colab](https://colab.research.google.com/drive/1BTdwwQXOreTCbTK7afUlJQFMsKopWBTi?usp=sharing)

Dados foram salvos como parquet nas camadas raw, trusted e delivery

Transformações aplicadas na camada trusted:
- decodificação dos nomes
- remoção de cnpj duplicados priorizando os nomes que não contém o sufixo '- PRUDENCIAL'
- remoção do sufixo '- PRUDENCIAL' do campo Nome

