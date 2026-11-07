# Lar das Delícias — loja + administração

Sistema web executável com **FastAPI, SQLite e interface responsiva**. A API e o painel usam o mesmo banco. Não há produtos, avaliações ou vendas inventados. O texto inicial da história foi escrito apenas a partir do briefing disponibilizado.

## Iniciar localmente

1. Use Python 3.10 ou superior e instale as dependências: `pip install -r requirements.txt`.
2. Copie `.env.example` para `.env` e defina uma chave secreta de pelo menos 32 caracteres, e-mail e senha administrativa forte (mínimo de 12 caracteres). O projeto **não carrega `.env` automaticamente**; exporte essas variáveis no terminal ou use o comando abaixo.
3. Linux/macOS: `set -a; source .env; set +a; uvicorn main:app --reload`.
4. Windows PowerShell: `$env:APP_SECRET="..."; $env:ADMIN_EMAIL="..."; $env:ADMIN_PASSWORD="..."; uvicorn main:app --reload`.
5. Acesse `http://127.0.0.1:8000` para loja e `http://127.0.0.1:8000/admin` para administração.

**Importante:** a conta administrativa só é criada automaticamente na inicialização se o banco ainda não tiver usuários e as credenciais forem informadas corretamente. Configure a conta antes de iniciar pela primeira vez.

## Fluxos disponíveis

- Loja: listagem real de produtos, busca e filtros, carrinho, cálculo do preço a partir do servidor, retirada/entrega com taxa fixa, registro de pedidos, aprovação manual pela proprietária, Pix após aceite com envio de comprovante PNG/JPG/PDF (até 5 MB), consulta de status com código + WhatsApp, avaliações com moderação e solicitação de orçamento.
- Gestão: autenticação, produtos/categorias, conteúdo da loja, clientes, orçamentos, estoque e movimentações, fichas técnicas, precificação básica, despesas, pedidos com aceite/recusa, alerta de comprovantes enviados e pagamento confirmado manualmente, agenda administrativa editável e visualização das datas de pedidos, produção com baixa única de ingredientes, dashboard, exportação CSV e simulador de descontinuação baseado no histórico efetivamente confirmado.
- Segurança básica: senha com PBKDF2-HMAC, cookie HttpOnly/SameSite, assinatura de sessão, operações administrativas autenticadas, consultas parametrizadas e validação do pedido com reprecificação e conferência de disponibilidade no servidor.

## Limites antes de produção

Esta é uma **base funcional demonstrável**, não uma implantação certificada para comércio eletrônico real. Não foi publicada nem conectada a uma conta de pagamento; o Pix não é gerado automaticamente com QR Code e sua compensação exige confirmação humana. Para lançamento comercial, implementar gateway com webhook validado, confirmação de disponibilidade de datas/agenda, política de frete por endereço/CEP, cálculo de custo de embalagens/perdas/mão de obra/impostos, controle dos 30% de participação com regra contratual validada, alertas automáticos, lembretes, backups e recuperação de desastre, políticas de retenção LGPD, rate limit, auditoria, CSRF reforçado, testes de carga, HTTPS, monitoramento, migrações e um banco persistente de produção (recomendado PostgreSQL). Em hospedagem serverless como Vercel, **não use SQLite local como banco de produção**, pois a persistência não é garantida.

A logo original foi recebida em PDF e convertida para `static/logo.png`. Não foram recebidos um catálogo autorizado/atualizado, fotos licenciadas nem dados reais de pagamento e entrega. A administração permite configurar a URL pública da logo original. **Não confundir produtos sem lactose com produtos próprios para APLV**: os campos de alergênicos, ingredientes e contato cruzado devem ser preenchidos e conferidos pela proprietária antes da publicação.

O site de referência consultado é https://lar-das-delicias.vercel.app/. Os itens e preços vistos nele não foram copiados automaticamente porque podem estar desatualizados.

## Revisão de fluxo de pedidos (novembro de 2026)

1. Cliente solicita o pedido; o status inicial é `aguardando_aprovacao`. A data é apenas solicitada.
2. Jociara visualiza o alerta em **Visão geral > Pedidos**, e clica **Aceitar** ou **Recusar**. Não há bloqueio automático de capacidade.
3. Se aceito, o cliente consulta o pedido com código + WhatsApp e recebe a chave Pix cadastrada no painel.
4. Cliente anexa comprovante PNG, JPG ou PDF (limite de 5 MB). O arquivo é armazenado fora da pasta pública. O sistema marca `comprovante_enviado` e exibe alerta administrativo.
5. Jociara abre o arquivo e confirma o Pix manualmente. O comprovante não prova por si só que houve crédito: confira diretamente no banco.
6. **Agenda**: os compromissos cadastrados são editáveis e há uma lista de datas dos pedidos, com status de aceite.

**Banco de dados:** SQLite, arquivo `data/lar.db` por padrão, compartilhado pela loja e pelo painel, criado na primeira inicialização. Os arquivos de comprovantes ficam em `data/comprovantes/` e precisam de backup. Não há banco de nuvem provisionado ou integrado a hospedagem. Para deploy em Vercel ou múltiplas instâncias é necessário adaptar para banco externo durável e armazenamento privado de arquivos.

**Entrega:** a configuração continua usando taxa fixa escolhida pela proprietária (zero por padrão); não há serviço de cálculo por endereço. **Agenda:** não há controle de capacidade: a aceitação é sempre decisão humana.

**Cartão (simulação):** com `SIMULATE_CARD=true` no `.env`, pedidos pagos por cartão recebem, após o aceite, um link de pagamento **simulado** (`/pagar/<código>`). Ele não cobra nada nem usa gateway real; apenas marca o pagamento como aguardando conferência da proprietária. Deixe `SIMULATE_CARD=false` em produção.
