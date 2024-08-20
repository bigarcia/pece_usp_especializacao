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

Arquivos de input
![image](https://github.com/user-attachments/assets/e0bc3181-c626-4aa1-ad82-a0cc14f7bf6c)

Arquivos gerados na camada raw 

![image](https://github.com/user-attachments/assets/db636f67-fcce-4d08-a6e7-9719a6195bc8)

Arquivos gerados na camada trusted

![image](https://github.com/user-attachments/assets/d699b421-a8d9-4fbf-89da-971b9fd90a43)

Arquivos gerados na camada delivery

![image](https://github.com/user-attachments/assets/960ebb9f-78eb-4dc9-906a-092ee256fd35)


