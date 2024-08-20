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


Arquivos gerados na camada raw 

![image](https://github.com/user-attachments/assets/33739b72-c964-4f9b-868b-acc2d1515bd7)

Arquivos gerados na camada trusted

![image](https://github.com/user-attachments/assets/dd8bf5f1-6cfb-4cdd-8cf1-80755e2638bd)

Arquivos de input
![image](https://github.com/user-attachments/assets/e0bc3181-c626-4aa1-ad82-a0cc14f7bf6c)
