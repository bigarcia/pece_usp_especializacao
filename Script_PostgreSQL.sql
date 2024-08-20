--cria banco de dados trabalho_2
create database trabalho_2;

--lista todos os bancos de dados
select datname from pg_database;

create schema _raw;
create schema _trusted;
create schema _delivery;


--criação de tabelas no SCHEMA _RAW
create table _raw.bancos(
	segmento VARCHAR(5),
	CNPJ VARCHAR(20) NOT NULL PRIMARY KEY,
	nome VARCHAR(255)
);

create table _raw.empregados_glassdor_match_less_v2(
	employer_name VARCHAR(50),	
	reviews_count int,	
	culture_count int, 	
	salaries_count int,	
	benefits_count int,
	employer_website varchar(100),	
	employer_headquarters varchar(50),	
	employer_founded float,	
	employer_industry varchar(255),	
	employer_revenue varchar(80),	
	url varchar(255),
	geral float,	
	cultura_e_valores float,	
	diversidade_e_inclusao float,	
	qualidade_de_vida float,	
	alta_lideranca float, 	
	remuneracao_e_beneficios float,	
	oportunidades_de_carreira float,	
	recomendam_para_outras_pessoas_PCT float,	
	perspectiva_positiva_da_empresa_PCT float,	
	CNPJ varchar(30),
	nome varchar(255),	
	match_percent varchar(3)
);

create table _raw.empregados_glassdor_match_v2(
	employer_name VARCHAR(50),	
	reviews_count int,	
	culture_count int, 	
	salaries_count int,	
	benefits_count int,
	employer_website varchar(100),	
	employer_headquarters varchar(50),	
	employer_founded float,	
	employer_industry varchar(255),	
	employer_revenue varchar(80),	
	url varchar(255),
	geral float,	
	cultura_e_valores float,	
	diversidade_e_inclusao float,	
	qualidade_de_vida float,	
	alta_lideranca float, 	
	remuneracao_e_beneficios float,	
	oportunidades_de_carreira float,	
	recomendam_para_outras_pessoas_PCT float,	
	perspectiva_positiva_da_empresa_PCT float,	
	Segmento varchar(30),
	nome varchar(255),	
	match_percent varchar(3)
);

create table _raw.reclamacoes(
	ano int,
	trimestre varchar(4),
	categoria varchar(100),
	tipo varchar(30),
	CNPJ_IF varchar(10),
	instituicao_financeira varchar(100),
	indice float,
	quantidade_de_reclamacoes_reguladas_procedentes bigint,
	quantidade_de_reclamacoes_reguladas_outras bigint,
	quantidade_de_reclamacoes_nao_reguladas bigint,
	quantidade_total_de_reclamacoes bigint,
	quantidade_total_de_clientes_ccs_e_scr bigint,
	quantidade_de_clientes_ccs bigint,
	quantidade_de_clientes_scr bigint
);

--criação de tabelas no SCHEMA _TRUSTED
create table _trusted.bancos(
	segmento VARCHAR(5),
	CNPJ VARCHAR(20) NOT NULL PRIMARY KEY,
	nome VARCHAR(255),
	nome_sem_prudencial VARCHAR(255)
);

create table _trusted.empregados(
	employer_name VARCHAR(50),	
	reviews_count int,	
	culture_count int, 	
	salaries_count int,	
	benefits_count int,
	employer_website varchar(100),	
	employer_headquarters varchar(50),	
	employer_founded float,	
	employer_industry varchar(255),	
	employer_revenue varchar(80),	
	url varchar(255),
	geral float,	
	cultura_e_valores float,	
	diversidade_e_inclusao float,	
	qualidade_de_vida float,	
	alta_lideranca float, 	
	remuneracao_e_beneficios float,	
	oportunidades_de_carreira float,	
	recomendam_para_outras_pessoas_PCT float,	
	perspectiva_positiva_da_empresa_PCT float,	
	CNPJ_Segmento varchar(30),
	nome varchar(255),	
	match_percent varchar(3)
);

create table _trusted.reclamacoes(
	ano int,
	trimestre varchar(4),
	categoria varchar(100),
	tipo varchar(30),
	CNPJ_IF varchar(10),
	instituicao_financeira varchar(100),
	indice float,
	quantidade_de_reclamacoes_reguladas_procedentes bigint,
	quantidade_de_reclamacoes_reguladas_outras bigint,
	quantidade_de_reclamacoes_nao_reguladas bigint,
	quantidade_total_de_reclamacoes bigint,
	quantidade_total_de_clientes_ccs_e_scr bigint,
	quantidade_de_clientes_ccs bigint,
	quantidade_de_clientes_scr bigint
);

-------------------------------------------------------------------------------------------
------------------------------------  JOIN  -----------------------------------------------
-------------------------------------------------------------------------------------------

--criação da tabela empregados no SCHEMA _TRUSTED
CREATE TABLE "_trusted".db_empregados AS
SELECT E.*, B."CNPJ"
FROM "_trusted".empregados AS E
INNER JOIN "_trusted".bancos AS B
ON B."NOME_SEM_PRUDENCIAL" = E."Nome";

SELECT * FROM _trusted.db_empregados
SELECT COUNT(*) FROM _trusted.db_empregados    --29 registros

--criação da tabale reclamacoes no SCHEMA _TRUSTED
CREATE TABLE "_trusted".db_reclamacoes AS
SELECT R.*, B."Segmento"
FROM "_trusted".reclamacoes AS R
INNER JOIN "_trusted".bancos AS B
ON R."CNPJ_IF" = B."CNPJ";

SELECT * FROM _trusted.db_reclamacoes
SELECT COUNT(*) FROM _trusted.db_reclamacoes   --802 registros


--Criação da tabela f_reclamacoes_empregados com a junção das tabelas f_empregados e f_reclamacoes no SCHEMA _DELIVERY
CREATE TABLE "_delivery".f_reclamacoes_empregados AS
SELECT E.*, R.*
FROM "_trusted".db_empregados AS E
INNER JOIN "_trusted".db_reclamacoes AS R         
ON E."CNPJ" = R."CNPJ_IF";                   --12 registros


