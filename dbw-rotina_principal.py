#!/usr/bin/env python
# coding: utf-8

# # Sessão 1: Obtendo parametros de acesso ao Storage Account

# In[ ]:


# Declarando os parametros de acesso ao storage account

storage_account_name = "stdescargadados"
storage_account_key = "rZrtPoGhkWo79V7VUTA+9R6+c5ZyuCSlVfac/QbGu1S0s6kb3F/30fXDJCK2KazztchpbKQs2uzW+ASt04Q7sw=="
container_origem = "bronze"
container_dev = "desenvolvimento"
container_dest_ouro = "ouro"
container_dest_prata = "prata"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)


# # Sessão 2: Tratamento dos dados

# ## 2.0 - Contas

# In[ ]:


# Importando as bibliotecas

from pyspark.sql import functions as F

# Fazendo a leitura dos arquivos

accounts_balances = spark.read.option("header",True)                              .option("inferSchema", True)                              .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/accounts_balances.parquet")

accounts_identification = spark.read.option("header",True)                                    .option("inferSchema", True)                                    .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/accounts_identification_v2.parquet")

accounts_overdraft_limits = spark.read.option("header",True)                                      .option("inferSchema", True)                                      .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/accounts_overdraft_limits.parquet")
                                      
accounts_transactions = spark.read.option("header",True)                                  .option("inferSchema", True)                                  .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/accounts_transactions.parquet")

 # Tratamento das colunas duplicadas

accounts_identification = accounts_identification.withColumnRenamed("type", "type_identification")
accounts_transactions = accounts_transactions.withColumnRenamed("type", "type_transactions")

 # Realização do join entre os Dataframes tratados 

Df_accounts = accounts_balances.join(accounts_identification, ["accountId","consentId"], how="left")
Df_accounts = Df_accounts.join(accounts_overdraft_limits, ["accountId","consentId"], how="left")
Df_accounts = Df_accounts.join(accounts_transactions, ["accountId","consentId"], how="left")


# In[ ]:


# Realizando a tradução das colunas

accountsCol = Df_accounts.columns
accountsCol_new = ['ID_da_conta',
                   'ID_de_consentimento',
                   'valor_bloqueado',
                   'Valor_Investido_automaticamente',
                   'valor_bloqueado_Moeda',
                   'Moeda_do_valor_investido_automaticamente',
                   'quantidade_disponivel',
                   'codigo_de_concorrencia',
                   'Codigo_da_Agencia',
                   'numero',
                   'digito_de_verificacao',
                   'identificacao_do_tipo',
                   'subtipo',
                   'cheque_especial_Limite_contratado',
                   'limite_de_cheque_especial_usado',
                   'Valor_de_cheque_especial_nao_combinado',
                   'Tipo_de_pagamento_autorizado_concluido',
                   'ID_da_transacao',
                   'Data_da_transacao',
                   'tipo_de_transacoes',
                   'nome_da_transacao',
                   'credito_Tipo_de_debito',
                   'quantia']

for old,new in zip(accountsCol,accountsCol_new):
  Df_accounts = Df_accounts.withColumnRenamed(f"{old}",f"{new}")
  
  


# In[ ]:


# Realizando a persistencia dos dados 

Df_accounts.repartition(1).write.mode('overwrite').csv(f"wasbs://{container_dest_prata}@{storage_account_name}.blob.core.windows.net/accounts",header = "true")


# ## 2.1 - Consentimento

# In[ ]:


# Fazendo a leitura dos arquivos

Df_consents = spark.read.option("header",True)                        .option("inferSchema", True)                        .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/consents.parquet")


# In[ ]:


# Realizando tradução das colunas

Lista_Colunas_Consentimentos = Df_consents.columns
Lista_Novas_Colunas = ['identificacao_da_marca',
                       'ID_de_consentimento',
                       'status',
                       'criacao_Data_Hora',
                       'expiracao_Data_Hora',
                       'ID_de_marca_interna',
                       'identificacao',
                       'tipo_pessoa',
                       'marca']

for old,new in zip(Lista_Colunas_Consentimentos,Lista_Novas_Colunas):
    Df_consents = Df_consents.withColumnRenamed(f"{old}",f"{new}")
    

# Realizando a persistencia dos dados 

Df_consents.repartition(1).write.mode('overwrite').csv(f"wasbs://{container_dest_prata}@{storage_account_name}.blob.core.windows.net/consents",header = "true")


# ## 2.2 - Credito

# In[ ]:


# Fazendo a leitura dos arquivos

credit_cards_accounts_bill = spark.read.option("header",True)                                       .option("inferSchema", True)                                       .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/credit_cards_accounts_bill.parquet")

credit_cards_accounts_limits = spark.read.option("header",True)                                         .option("inferSchema", True)                                         .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/credit_cards_accounts_limits.parquet")

credit_cards_accounts_transactions = spark.read.option("header",True)                                               .option("inferSchema", True)                                               .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/credit_cards_accounts_transactions.parquet")

credit_cards_identification = spark.read.option("header",True)                                        .option("inferSchema", True)                                        .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/credit_cards_identification.parquet")
                                        
credit_cards_lists = spark.read.option("header",True)                               .option("inferSchema", True)                               .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/credit_cards_lists.parquet")


# Realizando tratamento dos dados

credit_cards_identification = credit_cards_identification.withColumnRenamed("name","creditCardAccountId")

credit_cards_accounts_limits = credit_cards_accounts_limits.withColumnRenamed("identificationNumber","creditCardAccountId")

credit_cards_accounts_transactions = credit_cards_accounts_transactions.withColumnRenamed("amount","amount_transactions")
credit_cards_accounts_transactions = credit_cards_accounts_transactions.withColumnRenamed("billid","billid_transactions")
credit_cards_accounts_transactions = credit_cards_accounts_transactions.withColumnRenamed("currency","currency_transactions")
credit_cards_accounts_transactions = credit_cards_accounts_transactions.withColumnRenamed("linename","linename_transactions")
credit_cards_accounts_transactions = credit_cards_accounts_transactions.withColumnRenamed("identificationnumber","identificationnumber_transactions")


# Realizando Join dentre as tabelas

Df_credit = credit_cards_accounts_bill.join(credit_cards_accounts_limits, ["creditCardAccountId","consentId"], how="left")
Df_credit = Df_credit.join(credit_cards_accounts_transactions, ["creditCardAccountId","consentId"], how="left")
Df_credit = Df_credit.join(credit_cards_identification, ["creditCardAccountId","consentId"], how="left")
Df_credit = Df_credit.join(credit_cards_lists, ["creditCardAccountId","consentId"], how="left")


# In[ ]:


# Realizando tradução das colunas

lista_credit = Df_credit.columns
lista_traducao_credit = ['ID_da_conta_do_cartao_de_credito',
                         'ID_de_consentimento',
                         'codigo_da_conta',
                         'moeda_do_valor_total_da_fatura',
                         'e_Prestacao',
                         'moeda_do_valor_minimo_da_fatura',
                         'data_de_vencimento',
                         'valor_minimo_da_fatura',
                         'valor_total_da_fatura',
                         'data_de_pagamento',
                         'moeda_de_pagamento',
                         'tipo_de_valor',
                         'modo_de_pagamento',
                         'Valor_do_pagamento',
                         'tipo',
                         'moeda',
                         'informacao_adicional',
                         'quantia',
                         'moeda_do_valor_disponivel',
                         'Informacoes_adicionais_do_nome_da_linha',
                         'moeda_do_valor_usado',
                         'moeda_do_valor_limite',
                         'Tipo_de_consolidacao',
                         'Tipo_de_limite_de_linha_de_credito',
                         'Nome_da_linha',
                         'Valor_limite',
                         'Quantidade_usada',
                         'quantidade_disponivel',
                         'e_flexivel_no_limite',
                         'ID_da_transacao',
                         'data_de_postagem_da_fatura',
                         'beneficiario_MCC',
                         'transacoes_de_moeda',
                         'Identificador_de_cobranca',
                         'transacoes_de_numero_de_identificacao',
                         'outros_tipos_de_creditos',
                         'Informacoes_adicionais_do_tipo_de_taxa',
                         'transacoes_de_boletos',
                         'Tipo_de_transacao',
                         'tipo_de_pagamento',
                         'Data_da_transacao',
                         'Tipo_de_taxa',
                         'Nome_da_transacao',
                         'Informacoes_Adicionais_da_transacao',
                         'Tipo_de_debito_de_credito',
                         'outras_informacoes_adicionais_de_creditos',
                         'transacoes_de_nome_de_linha',
                         'Quantidade_brasileira',
                         'Numero_de_cobrança',
                         'transacoes_de_valor',
                         'numero_de_identificacao',
                         'e_Cartao_de_Credito_Multiplo',
                         'informacoes_adicionais_da_rede',
                         'Rede_de_Cartoes_de_Credito',
                         'produto_adicional']

for old,new in zip(lista_credit,lista_traducao_credit):
  Df_credit = Df_credit.withColumnRenamed(f"{old}",f"{new}")
  
  
# Realizando persistencia dos dados
Df_credit = Df_credit.limit(1000000)
Df_credit.repartition(1).write.mode('overwrite').parquet(f"wasbs://{container_dest_prata}@{storage_account_name}.blob.core.windows.net/credit")


# ## 2.3 - Financiamento

# In[ ]:


# Fazendo a leitura dos arquivos

financings_contracts_payments = spark.read.option("header",True)                                          .option("inferSchema", True)                                          .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/financings_contracts_payments.parquet")

financings_contracts_scheduled_instalments = spark.read.option("header",True)                                                       .option("inferSchema", True).parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/financings_contracts_scheduled_instalments.parquet")
                                                       
financings_contracts = spark.read.option("header",True)                                 .option("inferSchema", True)                                 .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/financings_contracts.parquet")

financings_warranties = spark.read.option("header",True)                                  .option("inferSchema", True)                                  .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/financings_warranties.parquet")


# Realizando tratamento dos dados

financings_contracts = financings_contracts.withColumnRenamed("paidinstalments","paidinstalments_contracts")
financings_contracts_payments = financings_contracts_payments.withColumnRenamed("paidinstalments","paidinstalments_payments_financings")

# Realizando Join dentre as tabelas

Df_financings = financings_contracts_payments.join(financings_contracts_scheduled_instalments, ["contractId","consentId"], how="left")
Df_financings = Df_financings.join(financings_contracts, ["contractId","consentId"], how="left")
Df_financings = Df_financings.join(financings_warranties, ["contractId","consentId"], how="left")


# In[ ]:


# Realizando tradução das colunas

Lista_financings = Df_financings.columns
Lista_financings_traducao = ['ID_do_contrato',
                             'ID_de_consentimento',
                             'data_de_pagamento',
                             'pagamentos_em_prestacoes_pagos',
                             'saldo_pendente_do_contrato',
                             'Prestacoes_devidas',
                             'Numero_total_de_prestacoes',
                             'digite_Numero_de_Parcelas',
                             'tipo_contrato_restante',
                             'Prestacoes_vencidas',
                             'Prestacoes_pagas',
                             'numero_restante_do_contrato',
                             'data_de_vencimento_da_primeira_parcela',
                             'data_de_liquidacao',
                             'periodicidade_fiscal',
                             'Subtipo_de_indexador_de_taxa_referencial',
                             'Calculo',
                             'data_de_vencimento',
                             'data',
                             'Nome_do_Produto',
                             'Periodicidade_das_parcelas',
                             'Valor_do_contrato',
                             'CET',
                             'amortizacao_programada',
                             'Tipo_de_imposto',
                             'Tipo_de_taxa_de_juros',
                             'taxa_pre-fixada',
                             'pos_Taxa_Fixa',
                             'Subtipo_de_produto',
                             'Tipo_de_garantia',
                             'Valor_da_garantia',
                             'Subtipo_de_garantia']


for old,new in zip(Lista_financings,Lista_financings_traducao):
  Df_financings = Df_financings.withColumnRenamed(f"{old}",f"{new}")
  
# Realizando persistencia dos dados

Df_financings.repartition(1).write.mode('overwrite').csv(f"wasbs://{container_dest_prata}@{storage_account_name}.blob.core.windows.net/financings",header = "true")


# ## 2.4 - Emprestimo

# In[ ]:


# Fazendo a leitura dos arquivos

loans_contracts_payments = spark.read.option("header",True)                                     .option("inferSchema", True)                                     .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/loans_contracts_payments.parquet")

loans_contracts_scheduled_instalments = spark.read.option("header",True)                                                  .option("inferSchema", True)                                                  .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/loans_contracts_scheduled_instalments.parquet")

loans_contracts_warranties = spark.read.option("header",True)                                       .option("inferSchema", True)                                       .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/loans_contracts_warranties.parquet")

loans_contracts = spark.read.option("header",True)                            .option("inferSchema", True)                            .parquet(f"wasbs://{container_origem}@{storage_account_name}.blob.core.windows.net/loans_contracts.parquet")


# Realizando tratamento dos dados

loans_contracts_payments = loans_contracts_payments.withColumnRenamed("paidinstalments","paidinstalments_payments_loans")

# Realizando Join dentre as tabelas

Df_loans = loans_contracts_payments.join(loans_contracts_scheduled_instalments, ["contractId","consentId"], how="left")
Df_loans = Df_loans.join(loans_contracts_warranties, ["contractId","consentId"], how="left")
Df_loans = Df_loans.join(loans_contracts, ["contractId","consentId"], how="left")


# In[ ]:


# Realizando tradução das colunas

lista_Df_loans = Df_loans.columns
lista_Df_loans_traducao = ['ID_do_contrato',
                           'ID_de_consentimento',
                           'data_de_pagamento',
                           'pagamentos_em_prestacoes_pagos',
                           'saldo_pendente_do_contrato',
                           'Prestacoes_devidas',
                           'Numero_total_de_prestacoes',
                           'digite_Numero_de_Parcelas',
                           'tipo_contrato_restante',
                           'Prestacoes_vencidas',
                           'Prestacoes_pagas',
                           'numero_restante_do_contrato',
                           'Tipo_de_garantia',
                           'Valor_da_garantia',
                           'Subtipo_de_garantia',
                           'Nome_do_Produto',
                           'Periodicidade_das_parcelas',
                           'Valor_do_contrato',
                           'CET',
                           'amortizacao_programada',
                           'Tipo_de_imposto',
                           'Tipo_de_taxa_de_juros',
                           'taxa_pre-fixada',
                           'pós_Taxa_Fixa',
                           'Tipo_de_indexador_de_taxa_referencial',
                           'periodicidade_fiscal',
                           'Subtipo_indexador_de_taxa_referencial',
                           'Calculo',
                           'data_de_vencimento',
                           'data',
                           'data_de_vencimento_da_primeira_parcela',
                           'data_de_liquidacao']


for old,new in zip(lista_Df_loans,lista_Df_loans_traducao):
  Df_loans = Df_loans.withColumnRenamed(f"{old}",f"{new}")
  
# Realizando persistencia dos dados

Df_loans.repartition(1).write.mode('overwrite').csv(f"wasbs://{container_dest_prata}@{storage_account_name}.blob.core.windows.net/loans",header = "true")


# ## 2.5 - Definindo as views

# In[ ]:


# Definindo as views de cada DataFrame

Df_accounts.createOrReplaceTempView("accounts_view")
Df_consents.createOrReplaceTempView("consents_view")
Df_credit.createOrReplaceTempView("credit_view")
Df_financings.createOrReplaceTempView("financings_view")
Df_loans.createOrReplaceTempView("loans_view")


# ### 2.5.1 - Uniando todas as views

# In[ ]:


Df_InvBradesco = spark.sql(""" SELECT 
                               acc.ID_de_consentimento                       AS ID_de_consentimento,
                               acc.valor_bloqueado                           AS Conta_valor_bloqueado,
                               acc.quantidade_disponivel                     AS Conta_quantidade_disponivel,
                               acc.cheque_especial_Limite_contratado         AS Conta_cheque_especial_Limite_contratado,
                               acc.limite_de_cheque_especial_usado           AS Conta_limite_de_cheque_especial_usado,
                               acc.Valor_de_cheque_especial_nao_combinado    AS Conta_Valor_de_cheque_especial_nao_combinado,
                               acc.Data_da_transacao                         AS Conta_Data_da_transacao,
                               acc.quantia                                   AS quantia_Conta,
                               cred.data_de_vencimento                       AS Cartao_data_de_vencimento,
                               cred.valor_minimo_da_fatura                   AS Cartao_valor_minimo_da_fatura,
                               cred.valor_total_da_fatura                    AS Cartao_valor_total_da_fatura,
                               cred.data_de_pagamento                        AS Cartao_data_de_pagamento,
                               cred.Valor_do_pagamento                       AS Cartao_Valor_do_pagamento,
                               cred.quantia                                  AS quantia_Cartao,
                               cred.Valor_limite                             AS Cartao_Valor_limite,
                               cred.Quantidade_usada                         AS Cartao_Quantidade_usada,
                               cred.quantidade_disponivel                    AS Cartao_quantidade_disponivel,
                               cred.Data_da_transacao                        AS Cartao_Data_da_transacao,
                               cred.transacoes_de_valor                      AS Cartao_transacoes_de_valor
                               FROM accounts_view acc
                               LEFT JOIN credit_view AS cred
                               ON acc.ID_de_consentimento = cred.ID_de_consentimento
                           """)


# In[ ]:


Df_InvBradesco.createOrReplaceTempView("InvBradesco_view")


# In[ ]:


Df_InvBradesco = spark.sql(""" SELECT 
                               Invbrades.ID_de_consentimento,
                               Invbrades.Conta_valor_bloqueado,
                               Invbrades.Conta_quantidade_disponivel,
                               Invbrades.Conta_cheque_especial_Limite_contratado,
                               Invbrades.Conta_limite_de_cheque_especial_usado,
                               Invbrades.Conta_Valor_de_cheque_especial_nao_combinado,
                               Invbrades.Conta_Data_da_transacao,
                               Invbrades.quantia_Conta,
                               Invbrades.Cartao_data_de_vencimento,
                               Invbrades.Cartao_valor_minimo_da_fatura,
                               Invbrades.Cartao_valor_total_da_fatura,
                               Invbrades.Cartao_data_de_pagamento,
                               Invbrades.Cartao_Valor_do_pagamento,
                               Invbrades.quantia_Cartao,
                               Invbrades.Cartao_Valor_limite,
                               Invbrades.Cartao_Quantidade_usada,
                               Invbrades.Cartao_quantidade_disponivel,
                               Invbrades.Cartao_Data_da_transacao,
                               Invbrades.Cartao_transacoes_de_valor,
                               loans.data_de_pagamento                             AS Emprestimo_saldo_pendente_do_contrato, 
                               loans.saldo_pendente_do_contrato                    AS Emprestimo_Prestacoes_devidas,
                               loans.Numero_total_de_prestacoes                    AS Emprestimo_Numero_total_de_prestacoes,
                               loans.Prestacoes_vencidas                           AS Emprestimo_Prestacoes_vencidas,
                               loans.Prestacoes_pagas                              AS Emprestimo_Prestacoes_pagas,
                               loans.Nome_do_Produto                               AS Emprestimo_Nome_do_Produto,
                               loans.Periodicidade_das_parcelas                    AS Emprestimo_Periodicidade_das_parcelas,
                               loans.Valor_do_contrato                             AS Emprestimo_Valor_do_contrato,
                               loans.data_de_vencimento                            AS Emprestimo_data_de_vencimento
                               FROM InvBradesco_view Invbrades
                               LEFT JOIN loans_view AS loans
                               ON Invbrades.ID_de_consentimento = loans.ID_de_consentimento
                           """)


# In[ ]:


Df_InvBradesco.createOrReplaceTempView("InvBradesco_view")


# In[ ]:


Df_InvBradesco = spark.sql(""" SELECT 
                               Invbrades.ID_de_consentimento,
                               Invbrades.Conta_valor_bloqueado,
                               Invbrades.Conta_quantidade_disponivel,
                               Invbrades.Conta_cheque_especial_Limite_contratado,
                               Invbrades.Conta_limite_de_cheque_especial_usado,
                               Invbrades.Conta_Valor_de_cheque_especial_nao_combinado,
                               Invbrades.Conta_Data_da_transacao,
                               Invbrades.quantia_Conta,
                               Invbrades.Cartao_data_de_vencimento,
                               Invbrades.Cartao_valor_minimo_da_fatura,
                               Invbrades.Cartao_valor_total_da_fatura,
                               Invbrades.Cartao_data_de_pagamento,
                               Invbrades.Cartao_Valor_do_pagamento,
                               Invbrades.quantia_Cartao,
                               Invbrades.Cartao_Valor_limite,
                               Invbrades.Cartao_Quantidade_usada,
                               Invbrades.Cartao_quantidade_disponivel,
                               Invbrades.Cartao_Data_da_transacao,
                               Invbrades.Cartao_transacoes_de_valor,
                               Invbrades.Emprestimo_saldo_pendente_do_contrato, 
                               Invbrades.Emprestimo_Prestacoes_devidas,
                               Invbrades.Emprestimo_Numero_total_de_prestacoes,
                               Invbrades.Emprestimo_Prestacoes_vencidas,
                               Invbrades.Emprestimo_Prestacoes_pagas,
                               Invbrades.Emprestimo_Nome_do_Produto,
                               Invbrades.Emprestimo_Periodicidade_das_parcelas,
                               Invbrades.Emprestimo_Valor_do_contrato,
                               Invbrades.Emprestimo_data_de_vencimento,
                               finan.data_de_pagamento               AS Financiamento_data_de_pagamento,
                               finan.pagamentos_em_prestacoes_pagos  AS Financiamento_pagamentos_em_prestacoes_pagos,
                               finan.saldo_pendente_do_contrato      AS Financiamento_saldo_pendente_do_contrato,
                               finan.Prestacoes_devidas              AS Financiamento_Prestacoes_devidas,
                               finan.Numero_total_de_prestacoes      AS Financiamento_Numero_total_de_prestacoes,
                               finan.Prestacoes_vencidas             AS Financiamento_Prestacoes_vencidas,
                               finan.Prestacoes_pagas                AS Financiamento_Prestacoes_pagas,
                               finan.data_de_vencimento              AS Financiamento_data_de_vencimento,
                               finan.Valor_do_contrato               AS Financiamento_Valor_do_contrato
                               FROM InvBradesco_view Invbrades
                               LEFT JOIN financings_view AS finan
                               ON Invbrades.ID_de_consentimento = finan.ID_de_consentimento
                              
                           """)


# In[ ]:


Df_InvBradesco = Df_InvBradesco.limit(3000000)
#Df_InvBradesco.repartition(1).write.mode('overwrite').csv(f"wasbs://{container_dest_ouro}@{storage_account_name}.blob.core.windows.net/Df_InvBradesco")


# # Sessão 3: Aplicação ML

# In[ ]:


import pyspark.sql.functions as F
columns = Df_InvBradesco.columns
for column in columns:
    Df_InvBradesco = Df_InvBradesco.withColumn(column,F.when(F.isnan(F.col(column)),0).otherwise(F.col(column)))


# In[ ]:


Df_teste = Df_InvBradesco.select("ID_de_consentimento","Conta_quantidade_disponivel","Cartao_valor_total_da_fatura","quantia_Conta","Financiamento_Prestacoes_devidas","Emprestimo_Prestacoes_devidas","Financiamento_Numero_total_de_prestacoes")

# Df_teste = Df_teste.filter(Df_teste.Conta_quantidade_disponivel.isNotNull())
# Df_teste = Df_teste.filter(Df_teste.Cartao_valor_total_da_fatura.isNotNull())
# Df_teste = Df_teste.filter(Df_teste.quantia_Conta.isNotNull())
# Df_teste = Df_teste.filter(Df_teste.Financiamento_Prestacoes_devidas.isNotNull())
# Df_teste = Df_teste.filter(Df_teste.Emprestimo_Prestacoes_devidas.isNotNull())
# Df_teste = Df_teste.filter(Df_teste.Financiamento_Numero_total_de_prestacoes.isNotNull())


# In[ ]:


Df_teste_1 = Df_teste.toPandas()
#Df_aplicacao = Df_InvBradesco.toPandas()


# In[ ]:


import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression

# Carrega o conjunto de dados em um DataFrame
df = Df_teste_1

# Remove a coluna 'score', se ela existir
if 'score' in df.columns:
    df.drop('score', axis=1, inplace=True)

# Substitui os valores ausentes pela mediana
df.fillna(df.median(), inplace=True)

# Divide os dados em variáveis independentes (X) e variável dependente (y)
X = df.drop(['ID_de_consentimento'], axis=1)
y = df['quantia_Conta']

# Divide os dados em treino e teste
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Aplica a normalização apenas nas variáveis independentes
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Cria um modelo de Regressão Logística
model = LogisticRegression()

# Treina o modelo com os dados de treino
model.fit(X_train, y_train)

# Gera as pontuações para os clientes com base nas variáveis financeiras
scores = model.predict_proba(X)[:, 1]

# Adiciona as pontuações na coluna 'score' do DataFrame
df['score'] = scores

# Exibe as primeiras linhas do DataFrame com a nova coluna 'score'
print(df.head())

