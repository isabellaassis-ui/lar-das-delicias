"""Lar das Delícias: API e interface. Não há dados fictícios nem confirmação automática de Pix."""
import os, sqlite3, json, secrets, hashlib, hmac, base64, csv, io, re, html, shutil, tempfile, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent
ON_VERCEL = bool(os.getenv('VERCEL'))
# Na Vercel só /tmp é gravável. Sem DB_PATH (ou com um caminho não gravável), o banco vai para lá.
DB = Path(os.getenv('DB_PATH') or ('/tmp/lar.db' if ON_VERCEL else str(ROOT/'data'/'lar.db')))
SECRET = os.getenv('APP_SECRET', '')
COOKIE_SECURE = os.getenv('COOKIE_SECURE', 'false').lower() == 'true'
SIMULATE_CARD = os.getenv('SIMULATE_CARD', 'false').lower() == 'true'
TURSO_URL = os.getenv('TURSO_DATABASE_URL', '').strip()   # banco remoto (Turso): dados permanentes
TURSO_TOKEN = os.getenv('TURSO_AUTH_TOKEN', '').strip()
CONFIG_ERROR = ''   # problema de configuração mostrado como página clara (em vez de erro 500)
INIT_ERROR = ''
if len(SECRET) < 32:
    if not ON_VERCEL:
        raise RuntimeError('Configure APP_SECRET com 32 ou mais caracteres. Veja .env.example.')
    CONFIG_ERROR = 'Falta a variável APP_SECRET (32 ou mais caracteres). Adicione em Settings > Environment Variables da Vercel e faça um novo deploy.'
    SECRET = secrets.token_urlsafe(48)

if TURSO_URL and not TURSO_TOKEN:
    CONFIG_ERROR = 'Falta a variável TURSO_AUTH_TOKEN (token do banco Turso).'

def _dir_writable(folder):
    try:
        folder.mkdir(parents=True, exist_ok=True)
        return os.access(folder, os.W_OK)
    except OSError:
        return False

if not TURSO_URL and not _dir_writable(DB.parent):
    DB = Path(tempfile.gettempdir())/'lar.db'
    DB.parent.mkdir(parents=True, exist_ok=True)
SEED_DB = ROOT/'seed'/'lar.db'
if not TURSO_URL and not DB.exists() and SEED_DB.exists():  # primeira execução: parte do cardápio já cadastrado, sem dados de clientes
    try: shutil.copyfile(SEED_DB, DB)
    except OSError: pass
PROOFS = DB.parent/'comprovantes'
app = FastAPI(title='Lar das Delícias', docs_url=None, redoc_url=None)
if (ROOT/'static').is_dir():
    app.mount('/static', StaticFiles(directory=ROOT/'static'), name='static')

@app.middleware('http')
async def config_guard(request: Request, call_next):
    problem = CONFIG_ERROR or INIT_ERROR
    if problem:
        return HTMLResponse('<!doctype html><meta charset="utf-8"><title>Configuração necessária</title><body style="font-family:sans-serif;max-width:640px;margin:10vh auto;padding:0 20px"><h2>Lar das Delícias: configuração necessária</h2><p>'+html.escape(problem)+'</p></body>', status_code=503)
    return await call_next(request)

class _Row(dict):
    """Linha de resultado: acessa por nome ou por posição, como sqlite3.Row."""
    def __getitem__(self, k):
        if isinstance(k, int): return list(self.values())[k]
        return dict.__getitem__(self, k)

class _Cur:
    def __init__(self, rows, lastrowid=None, rowcount=0):
        self._r = rows; self.lastrowid = lastrowid; self.rowcount = rowcount
    def fetchall(self): return self._r
    def fetchone(self): return self._r[0] if self._r else None
    def __iter__(self): return iter(self._r)

class RemoteDB:
    """Cliente mínimo do Turso/libSQL por HTTP (Hrana). Imita só o que este sistema usa do sqlite3."""
    def __init__(self, url, token):
        self.base = (url.replace('libsql://', 'https://', 1) if url.startswith('libsql://') else url).rstrip('/')
        self.token = token; self.baton = None; self.first = True
    @staticmethod
    def _enc(v):
        if v is None: return {'type': 'null'}
        if isinstance(v, bool): return {'type': 'integer', 'value': str(int(v))}
        if isinstance(v, int): return {'type': 'integer', 'value': str(v)}
        if isinstance(v, float): return {'type': 'float', 'value': v}
        if isinstance(v, (bytes, bytearray)): return {'type': 'blob', 'base64': base64.b64encode(bytes(v)).decode()}
        return {'type': 'text', 'value': str(v)}
    @staticmethod
    def _dec(v):
        t = v.get('type')
        if t == 'null': return None
        if t == 'integer': return int(v['value'])
        if t == 'float': return float(v['value'])
        if t == 'blob': return base64.b64decode(v['base64'] + '=' * (-len(v['base64']) % 4))
        return v.get('value')
    def _post(self, reqs):
        body = json.dumps({'baton': self.baton, 'requests': reqs}).encode()
        r = urllib.request.Request(self.base + '/v2/pipeline', data=body, method='POST', headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(r, timeout=25) as x: res = json.loads(x.read())
        except urllib.error.HTTPError as e:
            raise sqlite3.OperationalError(f'Turso respondeu {e.code}: {e.read()[:200]!r}')
        except urllib.error.URLError as e:
            raise sqlite3.OperationalError(f'Turso indisponível: {e.reason}')
        self.baton = res.get('baton')
        if res.get('base_url'): self.base = res['base_url'].rstrip('/')
        return res['results']
    @staticmethod
    def _raise(err):
        msg = err.get('message', 'erro do banco')
        raise (sqlite3.IntegrityError if 'constraint' in msg.lower() else sqlite3.OperationalError)(msg)
    def execute(self, sql, args=()):
        reqs = []
        if self.first: reqs.append({'type': 'execute', 'stmt': {'sql': 'PRAGMA foreign_keys=ON', 'want_rows': False}})
        reqs.append({'type': 'execute', 'stmt': {'sql': sql, 'args': [self._enc(a) for a in args], 'want_rows': True}})
        results = self._post(reqs); self.first = False
        last = results[-1]
        if last['type'] == 'error': self._raise(last['error'])
        res = last['response']['result']
        cols = [c['name'] for c in res['cols']]
        rows = [_Row(zip(cols, [self._dec(v) for v in r])) for r in res['rows']]
        lid = res.get('last_insert_rowid')
        return _Cur(rows, int(lid) if lid is not None else None, res.get('affected_row_count', 0))
    def executescript(self, script):
        script = '\n'.join(l for l in script.splitlines() if not l.strip().upper().startswith('PRAGMA JOURNAL_MODE'))
        results = self._post([{'type': 'sequence', 'sql': script}])
        if results[-1]['type'] == 'error': self._raise(results[-1]['error'])
    def close(self): pass

def seed_if_empty(db):
    """Na primeira execução, carrega o cardápio inicial (produtos, categorias, buffet e textos da loja)."""
    if row(db, "SELECT value FROM settings WHERE key='seeded'"): return
    if not row(db, 'SELECT id FROM products LIMIT 1') and SEED_DB.exists():
        src = sqlite3.connect(SEED_DB); src.row_factory = sqlite3.Row
        for table in ('categories', 'products', 'buffet_items', 'settings'):
            for r in src.execute(f'SELECT * FROM {table}').fetchall():
                cols = list(r.keys())
                db.execute(f"INSERT OR REPLACE INTO {table}({','.join(cols)}) VALUES({','.join('?' for _ in cols)})", tuple(r))
        src.close()
    db.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('seeded','1')")

@contextmanager
def conn():
    if TURSO_URL:
        yield RemoteDB(TURSO_URL, TURSO_TOKEN)
        return
    db = sqlite3.connect(DB, timeout=20, isolation_level=None)
    db.row_factory=sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA busy_timeout=20000')
    try:
        yield db
    finally:
        db.close()

def now(): return datetime.now(timezone.utc).isoformat(timespec='seconds')
def cents(x):
    try:
        v=Decimal(str(x))
        if not v.is_finite() or v < 0: raise ValueError()
        return int((v*100).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    except Exception: raise HTTPException(422,'Valor monetário inválido')
def rows(db,sql,args=()): return [dict(x) for x in db.execute(sql,args).fetchall()]
def row(db,sql,args=()):
    x=db.execute(sql,args).fetchone()
    return dict(x) if x else None

BUFFET_SEED=[
 ('Carboidratos','Arroz','',12,'por kg',10),
 ('Carboidratos','Arroz de forno','',40,'por assadeira',6),
 ('Carboidratos','Arroz à grega','',60,'por assadeira',6),
 ('Carboidratos','Lasanha','',80,'por assadeira',6),
 ('Carboidratos','Macarronada','',40,'por assadeira',6),
 ('Proteínas','Frango assado (coxa com sobrecoxa)','',20,'',4),
 ('Proteínas','Pernil','',40,'',6),
 ('Proteínas','Costelinha barbecue','',60,'',4),
 ('Proteínas','Lombo recheado','',140,'',6),
 ('Proteínas','Alcatra','',160,'',8),
 ('Proteínas','Strogonoff de carne','',160,'',5),
 ('Proteínas','Strogonoff de frango','',130,'',5),
 ('Saladas','Salada verde','Alface, tomate',30,'',0),
 ('Saladas','Salada refrescante','Repolho, cenoura, temperos, passas',60,'',0),
 ('Saladas','Tabule','Trigo para quibe, tomate, pepino, hortelã, cebola, limão, azeite',80,'',0),
 ('Saladas','Salada de macarrão','Macarrão, cenoura, milho, ervilha, azeitona',60,'',0),
 ('Saladas','Salpicão','Frango, cenoura, pimentão, ervilha, milho, maçã, uva passa, creme de leite',80,'',0),
 ('Saladas','Salada de maionese','Batata, cenoura, milho, ervilha, maionese',70,'',0),
]
CSV_COL={'id':'ID','name':'Nome','category_id':'Categoria (ID)','description':'Descrição','price_cents':'Preço (centavos)','unit':'Unidade','line':'Linha','ingredients':'Ingredientes','allergens':'Alergênicos','cross_contact':'Contato cruzado','claims':'Informações alimentares','image_url':'Foto','available':'Disponível','featured':'Destaque','options_json':'Opções','created_at':'Criado em','compare_group':'Grupo de comparação','phone':'Telefone','email':'E-mail','birthday':'Aniversário','notes':'Notas','preferences':'Preferências','public_code':'Código','customer_id':'Cliente (ID)','status':'Situação','payment_status':'Pagamento','payment_method':'Forma de pagamento','subtotal_cents':'Subtotal (centavos)','delivery_fee_cents':'Frete (centavos)','total_cents':'Total (centavos)','delivery_type':'Recebimento','delivery_address':'Endereço de entrega','delivery_date':'Data de entrega','delivery_time':'Horário','source':'Origem','proof_file':'Comprovante','proof_uploaded_at':'Comprovante enviado em','customer_name':'Cliente','request_text':'Pedido do orçamento','value_cents':'Valor (centavos)','stock_qty':'Estoque atual','min_qty':'Estoque mínimo','cost_cents_per_unit':'Custo por unidade (centavos)','supplier':'Fornecedor','amount_cents':'Valor (centavos)','occurred_on':'Data','category':'Categoria'}
def brl(c):return 'R$ '+f'{c/100:,.2f}'.replace(',','X').replace('.',',').replace('X','.')

def init():
    with conn() as db:
        db.executescript('''
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,email TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS categories(id INTEGER PRIMARY KEY,name TEXT NOT NULL UNIQUE,sort_order INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS products(id INTEGER PRIMARY KEY,category_id INTEGER REFERENCES categories(id),name TEXT NOT NULL,description TEXT NOT NULL DEFAULT '',price_cents INTEGER NOT NULL CHECK(price_cents>=0),unit TEXT NOT NULL DEFAULT 'un',line TEXT NOT NULL CHECK(line IN ('tradicional','inclusiva')),ingredients TEXT NOT NULL DEFAULT '',allergens TEXT NOT NULL DEFAULT '',cross_contact TEXT NOT NULL DEFAULT '',claims TEXT NOT NULL DEFAULT '',image_url TEXT NOT NULL DEFAULT '',available INTEGER NOT NULL DEFAULT 1,featured INTEGER NOT NULL DEFAULT 0,options_json TEXT NOT NULL DEFAULT '[]',created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS customers(id INTEGER PRIMARY KEY,name TEXT NOT NULL,phone TEXT NOT NULL,email TEXT NOT NULL DEFAULT '',birthday TEXT NOT NULL DEFAULT '',notes TEXT NOT NULL DEFAULT '',preferences TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY,public_code TEXT NOT NULL UNIQUE,customer_id INTEGER REFERENCES customers(id),status TEXT NOT NULL DEFAULT 'aguardando_pagamento',payment_status TEXT NOT NULL DEFAULT 'pendente',payment_method TEXT NOT NULL DEFAULT 'pix',subtotal_cents INTEGER NOT NULL,delivery_fee_cents INTEGER NOT NULL DEFAULT 0,total_cents INTEGER NOT NULL,delivery_type TEXT NOT NULL,delivery_address TEXT NOT NULL DEFAULT '',delivery_date TEXT NOT NULL,delivery_time TEXT NOT NULL DEFAULT '',notes TEXT NOT NULL DEFAULT '',source TEXT NOT NULL DEFAULT 'loja',created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS order_items(id INTEGER PRIMARY KEY,order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,product_id INTEGER REFERENCES products(id) ON DELETE SET NULL,product_name TEXT NOT NULL,product_line TEXT NOT NULL,unit_price_cents INTEGER NOT NULL,qty REAL NOT NULL,unit TEXT NOT NULL,options_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS quotes(id INTEGER PRIMARY KEY,customer_name TEXT NOT NULL,phone TEXT NOT NULL,request_text TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pendente',value_cents INTEGER NOT NULL DEFAULT 0,notes TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS ingredients(id INTEGER PRIMARY KEY,name TEXT NOT NULL UNIQUE,unit TEXT NOT NULL DEFAULT 'g',stock_qty REAL NOT NULL DEFAULT 0,min_qty REAL NOT NULL DEFAULT 0,cost_cents_per_unit REAL NOT NULL DEFAULT 0,supplier TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS stock_moves(id INTEGER PRIMARY KEY,ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),delta REAL NOT NULL,reason TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS recipes(id INTEGER PRIMARY KEY,product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),qty REAL NOT NULL,UNIQUE(product_id,ingredient_id));
        CREATE TABLE IF NOT EXISTS expenses(id INTEGER PRIMARY KEY,description TEXT NOT NULL,category TEXT NOT NULL,amount_cents INTEGER NOT NULL,occurred_on TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS incomes(id INTEGER PRIMARY KEY,description TEXT NOT NULL,category TEXT NOT NULL DEFAULT '',amount_cents INTEGER NOT NULL CHECK(amount_cents>=0),occurred_on TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS testimonials(id INTEGER PRIMARY KEY,name TEXT NOT NULL,content TEXT NOT NULL,approved INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS calendar_events(id INTEGER PRIMARY KEY,title TEXT NOT NULL,starts_at TEXT NOT NULL,ends_at TEXT NOT NULL DEFAULT '',description TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS transitions(id INTEGER PRIMARY KEY,product_id INTEGER NOT NULL UNIQUE REFERENCES products(id),plan TEXT NOT NULL DEFAULT '',target_date TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'planejado');
        CREATE TABLE IF NOT EXISTS buffet_items(id INTEGER PRIMARY KEY,category TEXT NOT NULL,name TEXT NOT NULL,description TEXT NOT NULL DEFAULT '',price_cents INTEGER NOT NULL CHECK(price_cents>=0),unit_label TEXT NOT NULL DEFAULT '',serves INTEGER NOT NULL DEFAULT 0,available INTEGER NOT NULL DEFAULT 1,sort_order INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS proofs(filename TEXT PRIMARY KEY,mime TEXT NOT NULL,content BLOB NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
        CREATE INDEX IF NOT EXISTS idx_orders_delivery ON orders(delivery_date);
        CREATE INDEX IF NOT EXISTS idx_items_order ON order_items(order_id);
        ''')
        order_cols={x['name'] for x in db.execute('PRAGMA table_info(orders)')}
        if 'proof_file' not in order_cols: db.execute("ALTER TABLE orders ADD COLUMN proof_file TEXT NOT NULL DEFAULT ''")
        if 'proof_uploaded_at' not in order_cols: db.execute("ALTER TABLE orders ADD COLUMN proof_uploaded_at TEXT NOT NULL DEFAULT ''")
        product_cols={x['name'] for x in db.execute('PRAGMA table_info(products)')}
        if 'compare_group' not in product_cols: db.execute("ALTER TABLE products ADD COLUMN compare_group TEXT NOT NULL DEFAULT ''")
        if not row(db,'SELECT id FROM buffet_items LIMIT 1'):
            for n,(cat,name,desc,price,unit,serves) in enumerate(BUFFET_SEED):
                db.execute('INSERT INTO buffet_items(category,name,description,price_cents,unit_label,serves,sort_order) VALUES(?,?,?,?,?,?,?)',(cat,name,desc,price*100,unit,serves,n))
        defaults={'brand_story': 'Tudo começou na cozinha, em 2016, com um desejo simples: transformar ingredientes em carinho. A Jociara começou preparando alimentos artesanais, feitos com cuidado, do jeito que se faz em casa, e cada encomenda levava um pouco do seu afeto até a mesa de quem a recebia.\n\nFoi dentro da própria família que a vida mudou o rumo da sua cozinha. Ao viver de perto os desafios da APLV, a alergia à proteína do leite de vaca, ela entendeu na prática o que é ler cada rótulo com atenção e o quanto importa que ninguém se sinta de fora na hora de celebrar.\n\nDessa vivência nasceu a vontade de estudar, testar e criar receitas inclusivas, que respeitam diferentes necessidades alimentares sem abrir mão do sabor, da beleza e do aconchego de um bolo bem feito.\n\nHoje, o Lar das Delícias segue se transformando aos poucos, receita por receita, sempre guiado pelo cuidado, pela transparência e pelo prazer de compartilhar a mesa. Por isso cada item mostra seus ingredientes e, quando existe, a versão tradicional e a inclusiva lado a lado: para que cada pessoa escolha com segurança e carinho.\n\nMais do que um cardápio, é um convite: venha para a mesa. Aqui, tem lugar para todo mundo.', 'hero_title':'Sabores para compartilhar, carinho em cada detalhe', 'hero_subtitle':'Encomendas artesanais e opções inclusivas, feitas com atenção às suas necessidades.', 'phone':'', 'instagram':'https://www.instagram.com/lar_das_delicias?mdxt=NDBydnhleWhjdm9o', 'address':'', 'opening_hours':'', 'pix_key':'', 'pix_instructions':'O pagamento é confirmado manualmente pela equipe.', 'delivery_fee':'0', 'logo_url':'/static/logo.png', 'hero_image_url':'', 'email':'', 'cross_contact_notice':'Consulte os ingredientes, os alergênicos e a possibilidade de contato cruzado de cada produto antes de encomendar.'}
        pairs=list(defaults.items())
        db.execute('INSERT OR IGNORE INTO settings(key,value) VALUES '+','.join(['(?,?)']*len(pairs)),tuple(x for kv in pairs for x in kv))
        email=os.getenv('ADMIN_EMAIL','').lower().strip()
        pw=os.getenv('ADMIN_PASSWORD','')
        if email and len(pw)>=12 and not row(db,'SELECT id FROM users LIMIT 1'):
            db.execute('INSERT INTO users(email,password_hash,created_at) VALUES(?,?,?)',(email,hashpw(pw),now()))
        seed_if_empty(db)

def hashpw(pw):
    salt=secrets.token_bytes(16)
    h=hashlib.pbkdf2_hmac('sha256',pw.encode(),salt,240000)
    return base64.b64encode(salt+h).decode()
def checkpw(pw,stored):
    try:
        x=base64.b64decode(stored)
        return hmac.compare_digest(hashlib.pbkdf2_hmac('sha256',pw.encode(),x[:16],240000),x[16:])
    except Exception:return False

try:
    init()
except Exception as e:
    if not ON_VERCEL: raise
    INIT_ERROR = f'Erro ao iniciar o banco de dados: {type(e).__name__}: {e}'

def token(uid):
    msg=f'{uid}:{int((datetime.now(timezone.utc)+timedelta(hours=12)).timestamp())}'
    sig=hmac.new(SECRET.encode(),msg.encode(),hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f'{msg}:{sig}'.encode()).decode()
def auth(request):
    val=request.cookies.get('lar_admin','')
    try:
        uid, exp, sig=base64.urlsafe_b64decode(val.encode()).decode().split(':')
        msg=f'{uid}:{exp}'
        if not hmac.compare_digest(hmac.new(SECRET.encode(),msg.encode(),hashlib.sha256).hexdigest(),sig):raise ValueError()
        if int(exp)<int(datetime.now(timezone.utc).timestamp()):raise ValueError()
        with conn() as db:
            user=row(db,'SELECT id,email FROM users WHERE id=?',(int(uid),))
        if not user: raise ValueError()
        return user
    except Exception:raise HTTPException(401,'Autenticação administrativa necessária')

def write_guard(request):
    auth(request)
    origin=request.headers.get('origin')
    host=request.headers.get('host','')
    if origin and not origin.endswith('://'+host):raise HTTPException(403,'Origem não permitida')

def obj(data, keys):
    if not isinstance(data,dict): raise HTTPException(422,'Objeto JSON esperado')
    return {k:data[k] for k in keys if k in data}
def allowedline(s):
    if s not in ('tradicional','inclusiva'):raise HTTPException(422,'Linha inválida')
    return s

def require_str(s,maximum=200):
    if not isinstance(s,str) or not s.strip() or len(s)>maximum:raise HTTPException(422,'Campo obrigatório ou muito longo')
    return s.strip()

def safe_date(s):
    try:return datetime.strptime(s,'%Y-%m-%d').date().isoformat()
    except Exception:raise HTTPException(422,'Data inválida (AAAA-MM-DD)')
def finiteqty(x):
    try:
        v=float(x)
        if not (0<v<=1000000) or v!=v: raise ValueError()
        return v
    except Exception:raise HTTPException(422,'Quantidade inválida')

@app.get('/')
def home(): return FileResponse(ROOT/'static'/'index.html')
@app.get('/admin')
def admin():return FileResponse(ROOT/'static'/'admin.html')
@app.get('/api/public')
def public():
    with conn() as db:
        settings={x['key']:x['value'] for x in rows(db,'SELECT * FROM settings') if x['key'] not in ('private','pix_key','pix_instructions')}
        products=rows(db,'SELECT p.*,c.name category FROM products p LEFT JOIN categories c ON c.id=p.category_id WHERE p.available=1 ORDER BY p.featured DESC,p.id DESC')
        categories=rows(db,'SELECT * FROM categories ORDER BY sort_order,name')
        buffet=rows(db,'SELECT * FROM buffet_items WHERE available=1 ORDER BY sort_order,id')
        testimonials=rows(db,'SELECT name,content FROM testimonials WHERE approved=1 ORDER BY id DESC LIMIT 8')
    return {'settings':settings,'products':products,'categories':categories,'testimonials':testimonials,'buffet':buffet}

@app.post('/api/login')
async def login(request:Request,response:Response):
    data=await request.json()
    with conn() as db: user=row(db,'SELECT * FROM users WHERE email=?',(str(data.get('email','')).lower().strip(),))
    if not user or not checkpw(str(data.get('password','')),user['password_hash']):raise HTTPException(401,'Credenciais inválidas')
    response.set_cookie('lar_admin',token(user['id']),httponly=True,secure=COOKIE_SECURE,samesite='lax',max_age=43200,path='/')
    return {'email':user['email']}
@app.post('/api/logout')
def logout(response:Response):
    response.delete_cookie('lar_admin',path='/')
    return {'ok':True}
@app.get('/api/me')
def me(request:Request):return auth(request)

@app.post('/api/orders')
async def place_order(request:Request):
    data=await request.json()
    name=require_str(data.get('name'),150);phone=require_str(data.get('phone'),40)
    items=data.get('items',[])
    if not isinstance(items,list) or not (1<=len(items)<=80):raise HTTPException(422,'Carrinho inválido')
    delivery_type=data.get('delivery_type')
    if delivery_type not in ('entrega','retirada'):raise HTTPException(422,'Tipo de entrega inválido')
    address=str(data.get('delivery_address') or '').strip()
    if delivery_type=='entrega' and not address:raise HTTPException(422,'Informe o endereço de entrega')
    date=safe_date(data.get('delivery_date'))
    if date < datetime.now().date().isoformat():raise HTTPException(422,'Selecione uma data futura')
    method=data.get('payment_method','pix')
    if method not in ('pix','cartao'):raise HTTPException(422,'Forma de pagamento inválida')
    email=str(data.get('email','')).strip()[:200];notes=str(data.get('notes',''))[:2000]
    with conn() as db:
        try:
            db.execute('BEGIN IMMEDIATE')
            total=0; normalized=[]
            for item in items:
                product=row(db,'SELECT * FROM products WHERE id=? AND available=1',(item.get('product_id'),))
                if not product:raise HTTPException(409,'Produto indisponível. Atualize o carrinho.')
                qty=finiteqty(item.get('qty',0))
                if qty!=int(qty):raise HTTPException(422,'A quantidade deve ser inteira')
                options=item.get('options',{})
                if not isinstance(options,dict):raise HTTPException(422,'Personalização inválida')
                allowed=json.loads(product['options_json'] or '[]')
                for key,val in options.items():
                    spec=next((x for x in allowed if x.get('name')==key),None)
                    if not spec or val not in spec.get('choices',[]):raise HTTPException(422,'Personalização indisponível')
                total+=product['price_cents']*int(qty)
                normalized.append((product,int(qty),json.dumps(options,ensure_ascii=False)))
            deliveryfee=0  # entrega a combinar com a proprietária
            old=row(db,'SELECT * FROM customers WHERE phone=? ORDER BY id DESC LIMIT 1',(phone,))
            if old:
                customer_id=old['id']
                db.execute('UPDATE customers SET name=?,email=? WHERE id=?',(name,email,customer_id))
            else:
                customer_id=db.execute('INSERT INTO customers(name,phone,email,created_at) VALUES(?,?,?,?)',(name,phone,email,now())).lastrowid
            code=secrets.token_hex(5).upper()
            oid=db.execute("INSERT INTO orders(public_code,customer_id,payment_method,status,payment_status,subtotal_cents,delivery_fee_cents,total_cents,delivery_type,delivery_address,delivery_date,delivery_time,notes,created_at) VALUES(?,?,?,'aguardando_aprovacao','aguardando_comprovante',?,?,?,?,?,?,?,?,?)",(code,customer_id,method,total,deliveryfee,total+deliveryfee,delivery_type,address,date,str(data.get('delivery_time',''))[:40],notes,now())).lastrowid
            for p,qty,opts in normalized:
                db.execute('INSERT INTO order_items(order_id,product_id,product_name,product_line,unit_price_cents,qty,unit,options_json) VALUES(?,?,?,?,?,?,?,?)',(oid,p['id'],p['name'],p['line'],p['price_cents'],qty,p['unit'],opts))
            db.execute('COMMIT')
        except Exception:
            db.execute('ROLLBACK');raise
        settings={x['key']:x['value'] for x in rows(db,"SELECT * FROM settings WHERE key IN ('pix_key','pix_instructions')")}
    return {'code':code,'total_cents':total+deliveryfee,'status':'aguardando_aprovacao','message':'Solicitação recebida. Aguarde a proprietária aceitar antes de fazer o Pix.'}

def paytoken(code):return hmac.new(SECRET.encode(),('pay:'+code).encode(),hashlib.sha256).hexdigest()[:24]
def paid_order(db,code,t):
    if not SIMULATE_CARD or not re.fullmatch(r'[0-9A-F]{10}',code) or not hmac.compare_digest(paytoken(code),t):raise HTTPException(404,'Link de pagamento inválido')
    o=row(db,"SELECT id,status,payment_status,payment_method,total_cents FROM orders WHERE public_code=?",(code,))
    if not o or o['payment_method']!='cartao' or o['status'] in ('aguardando_aprovacao','recusado','cancelado'):raise HTTPException(404,'Link de pagamento inválido')
    return o

PAY_PAGE='''<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Pagamento (simulação) | Lar das Delícias</title><link rel="stylesheet" href="/static/style.css"></head><body><div class="login"><p class="notice"><b>SIMULAÇÃO.</b> Esta página imita um link de pagamento por cartão. Nenhum cartão é solicitado e nenhum valor é cobrado.</p><h2>Pedido __CODE__</h2><p>Total: <b>__TOTAL__</b></p><p id="msg">__STATE__</p><button class="btn" id="go" __DIS__ onclick="pay()">Simular pagamento aprovado</button><p><a href="/#contato">Voltar à loja</a></p></div><script>async function pay(){let r=await fetch(location.pathname.replace('/pagar/','/api/pagar-simulado/')+location.search,{method:'POST'});let j=await r.json();document.getElementById('msg').textContent=r.ok?j.message:(j.detail||'Erro');if(r.ok)document.getElementById('go').disabled=true}</script></body></html>'''

@app.get('/pagar/{code}',response_class=HTMLResponse)
def pay_page(code:str,t:str=''):
    code=code.upper()
    with conn() as db:o=paid_order(db,code,t)
    done=o['payment_status'] in ('comprovante_enviado','confirmado')
    total=f"R$ {o['total_cents']/100:,.2f}".replace(',','X').replace('.',',').replace('X','.')
    return PAY_PAGE.replace('__CODE__',code).replace('__TOTAL__',total).replace('__STATE__','Pagamento simulado já registrado.' if done else 'Clique para simular a aprovação do pagamento.').replace('__DIS__','disabled' if done else '')

@app.post('/api/pagar-simulado/{code}')
def pay_simulated(code:str,t:str=''):
    code=code.upper()
    with conn() as db:
        o=paid_order(db,code,t)
        if o['payment_status']=='pendente' or o['payment_status']=='aguardando_comprovante':
            db.execute("UPDATE orders SET payment_status='comprovante_enviado',proof_uploaded_at=? WHERE id=?",(now(),o['id']))
    return {'ok':True,'message':'Pagamento simulado registrado. A proprietária fará a conferência final.'}

@app.get('/api/orders/track/{code}')
def track(code:str,phone:str=''):
    if not re.fullmatch(r'[0-9A-F]{10}',code.upper()) or not phone:raise HTTPException(404,'Pedido não encontrado')
    with conn() as db:
        order=row(db,'SELECT o.public_code,o.payment_method,o.status,o.payment_status,o.delivery_date,o.total_cents,o.created_at,o.proof_uploaded_at FROM orders o JOIN customers c ON o.customer_id=c.id WHERE o.public_code=? AND c.phone=?',(code.upper(),phone))
    if not order:raise HTTPException(404,'Pedido não encontrado')
    if order['status'] not in ('aguardando_aprovacao','recusado','cancelado'):
        with conn() as db:
            settings={x['key']:x['value'] for x in rows(db,"SELECT * FROM settings WHERE key IN ('pix_key','pix_instructions')")}
        if order['payment_method']=='cartao':
            if SIMULATE_CARD:order['pay_link']=f"/pagar/{code.upper()}?t={paytoken(code.upper())}"
        else:
            order['pix_key']=settings.get('pix_key','')
            order['pix_instructions']=settings.get('pix_instructions','')
    return order

@app.post('/api/orders/proof/{code}')
async def upload_proof(code:str,request:Request,phone:str=''):
    if not re.fullmatch(r'[0-9A-F]{10}',code.upper()) or not phone:raise HTTPException(404,'Pedido não encontrado')
    with conn() as db:
        order=row(db,"SELECT o.id,o.status,o.proof_file FROM orders o JOIN customers c ON o.customer_id=c.id WHERE o.public_code=? AND c.phone=?",(code.upper(),phone))
    if not order:raise HTTPException(404,'Pedido não encontrado')
    if order['status'] in ('aguardando_aprovacao','recusado','cancelado'):raise HTTPException(409,'Aguarde a aprovação do pedido antes de enviar o comprovante')
    typ=request.headers.get('content-type','').split(';')[0].lower()
    kinds={'image/png':'.png','image/jpeg':'.jpg','application/pdf':'.pdf'}
    if typ not in kinds:raise HTTPException(415,'Envie PNG, JPG ou PDF')
    body=await request.body()
    if not body or len(body)>4*1024*1024:raise HTTPException(413,'Arquivo vazio ou maior que 4 MB')
    if not (body.startswith(b'\x89PNG\r\n\x1a\n') if typ=='image/png' else body.startswith(b'\xff\xd8\xff') if typ=='image/jpeg' else body.startswith(b'%PDF-')):
        raise HTTPException(422,'Formato do arquivo inválido')
    filename=secrets.token_hex(24)+kinds[typ]
    with conn() as db:
        db.execute('INSERT INTO proofs(filename,mime,content) VALUES(?,?,?)',(filename,typ,body))
        db.execute("UPDATE orders SET proof_file=?,proof_uploaded_at=?,payment_status='comprovante_enviado' WHERE id=?",(filename,now(),order['id']))
        if order['proof_file']: db.execute('DELETE FROM proofs WHERE filename=?',(order['proof_file'],))
    if order['proof_file']:
        try:(PROOFS/order['proof_file']).unlink(missing_ok=True)  # comprovantes antigos salvos em arquivo
        except OSError:pass
    return {'ok':True,'message':'Comprovante recebido. A confirmação será feita pela proprietária.'}

@app.get('/api/admin/orders/{order_id}/proof')
def admin_proof(order_id:int,request:Request):
    auth(request)
    with conn() as db: order=row(db,'SELECT proof_file FROM orders WHERE id=?',(order_id,))
    if not order or not order['proof_file']:raise HTTPException(404,'Comprovante indisponível')
    with conn() as db: blob=row(db,'SELECT mime,content FROM proofs WHERE filename=?',(order['proof_file'],))
    if blob:return Response(blob['content'],media_type=blob['mime'],headers={'Cache-Control':'no-store'})
    path=PROOFS/Path(order['proof_file']).name
    if not path.is_file():raise HTTPException(404,'Arquivo não encontrado')
    return FileResponse(path,media_type={'.png':'image/png','.jpg':'image/jpeg','.pdf':'application/pdf'}[path.suffix],headers={'Cache-Control':'no-store'})

@app.post('/api/quotes')
async def quote(request:Request):
    d=await request.json()
    name=str(d.get('name') or '').strip()[:150] or 'Visitante';phone=str(d.get('phone') or '').strip()[:40]
    text=str(d.get('request') or '').strip()[:3000]
    delivery='entrega' if d.get('delivery_type')=='entrega' else 'retirada'
    raw=d.get('items') or []
    if not isinstance(raw,list) or len(raw)>60:raise HTTPException(422,'Itens inválidos')
    guests=str(d.get('guests') or '').strip()
    if guests and not (guests.isdigit() and 0<int(guests)<=100000):raise HTTPException(422,'Número de pessoas inválido')
    event_date=safe_date(d['event_date']) if str(d.get('event_date') or '').strip() else ''
    lines=[];total=0;shown=[]
    with conn() as db:
        for it in raw:
            if not isinstance(it,dict):raise HTTPException(422,'Itens inválidos')
            qty=finiteqty(it.get('qty'))
            if qty!=int(qty):raise HTTPException(422,'A quantidade deve ser inteira')
            b=row(db,'SELECT * FROM buffet_items WHERE id=? AND available=1',(it.get('id'),))
            if not b:raise HTTPException(409,'Item do buffet indisponível. Atualize a página.')
            sub=b['price_cents']*int(qty);total+=sub
            shown.append({'name':b['name'],'qty':int(qty),'unit_label':b['unit_label'],'price_cents':b['price_cents'],'sub_cents':sub,'serves':b['serves']})
            lines.append(f"- {int(qty)}x {b['name']} ({b['category']}), {brl(b['price_cents'])}{' '+b['unit_label'] if b['unit_label'] else ''}{', serve '+str(b['serves'])+' pessoas' if b['serves'] else ''} = {brl(sub)}")
        if not lines and not text:raise HTTPException(422,'Escolha itens do buffet ou descreva sua ideia')
        parts=[]
        if lines:parts.append('BUFFET\n'+'\n'.join(lines)+f'\nEstimativa: {brl(total)}')
        if event_date:parts.append('Data do evento: '+event_date)
        if guests:parts.append('Pessoas: '+guests)
        parts.append('Recebimento: '+('entrega (a combinar)' if delivery=='entrega' else 'retirada'))
        if text:parts.append('Observações do cliente: '+text)
        db.execute('INSERT INTO quotes(customer_name,phone,request_text,value_cents,created_at) VALUES(?,?,?,?,?)',(name,phone,'\n'.join(parts)[:6000],total,now()))
    return {'ok':True,'total_cents':total,'items':shown,'delivery':delivery,'guests':guests,'event_date':event_date}
@app.post('/api/testimonials')
async def testimonial(request:Request):
    d=await request.json()
    with conn() as db:db.execute('INSERT INTO testimonials(name,content,created_at) VALUES(?,?,?)',(require_str(d.get('name'),100),require_str(d.get('content'),1000),now()))
    return {'ok':True,'message':'Avaliação recebida, aguardando moderação.'}

TABLES={
 'categories':('categories',['name','sort_order']),
 'products':('products',['category_id','name','description','price_cents','unit','line','ingredients','allergens','cross_contact','claims','image_url','available','featured','options_json','compare_group']),
 'customers':('customers',['name','phone','email','birthday','notes','preferences']),
 'ingredients':('ingredients',['name','unit','stock_qty','min_qty','cost_cents_per_unit','supplier']),
 'expenses':('expenses',['description','category','amount_cents','occurred_on']),
 'incomes':('incomes',['description','category','amount_cents','occurred_on']),
 'testimonials':('testimonials',['name','content','approved']),
 'transitions':('transitions',['product_id','plan','target_date','status']),
 'recipes':('recipes',['product_id','ingredient_id','qty']),
 'quotes':('quotes',['customer_name','phone','request_text','status','value_cents','notes']),
 'calendar_events':('calendar_events',['title','starts_at','ends_at','description']),
 'buffet_items':('buffet_items',['category','name','description','price_cents','unit_label','serves','available','sort_order'])
}
@app.get('/api/admin/{resource}')
def listing(resource:str,request:Request):
    auth(request)
    with conn() as db:
        if resource in TABLES: return rows(db,f'SELECT * FROM {TABLES[resource][0]} ORDER BY id DESC LIMIT 1000')
        if resource=='orders':return rows(db,'SELECT o.*,c.name customer_name,c.phone customer_phone FROM orders o LEFT JOIN customers c ON c.id=o.customer_id ORDER BY o.created_at DESC LIMIT 1000')
        if resource=='settings':return {x['key']:x['value'] for x in rows(db,'SELECT * FROM settings')}
        if resource=='dashboard':return dashboard(db)
        if resource=='items':return rows(db,'SELECT * FROM order_items ORDER BY id DESC LIMIT 3000')
        if resource=='stock_moves':return rows(db,'SELECT * FROM stock_moves ORDER BY id DESC LIMIT 1000')
    raise HTTPException(404,'Recurso não encontrado')

def dashboard(db):
    orders=rows(db,"SELECT o.*,c.name customer_name FROM orders o LEFT JOIN customers c ON c.id=o.customer_id ORDER BY o.created_at DESC")
    confirmed=[o for o in orders if o['payment_status']=='confirmado' and o['status'] not in ('cancelado','recusado')]
    paid_ids=[o['id'] for o in confirmed]
    items=rows(db,'SELECT * FROM order_items')
    expense_rows=rows(db,'SELECT * FROM expenses')
    revenue=sum(o['total_cents']-o['delivery_fee_cents'] for o in confirmed)
    fees=sum(o['delivery_fee_cents'] for o in confirmed)
    expense=sum(x['amount_cents'] for x in expense_rows)
    other_income=sum(x['amount_cents'] for x in rows(db,'SELECT amount_cents FROM incomes'))
    line_revenue={'tradicional':0,'inclusiva':0}
    sold={}
    for i in items:
        if i['order_id'] in paid_ids:
            line_revenue[i['product_line']]+=int(i['unit_price_cents']*i['qty'])
            sold[i['product_name']]=sold.get(i['product_name'],0)+i['qty']
    low=rows(db,'SELECT * FROM ingredients WHERE stock_qty<=min_qty ORDER BY stock_qty-min_qty')
    upcoming=[o for o in orders if o['status'] not in ('entregue','cancelado') and o['delivery_date']>=datetime.now().date().isoformat()][:10]
    return {'orders_count':len(orders),'pending_payment':sum(o['payment_status'] in ('pendente','aguardando_comprovante','comprovante_enviado') for o in orders),'proof_alerts':sum(o['payment_status']=='comprovante_enviado' for o in orders),'approval_alerts':sum(o['status']=='aguardando_aprovacao' for o in orders),'pending_orders':sum(o['status'] not in ('entregue','cancelado') for o in orders),'revenue_cents':revenue,'delivery_fees_cents':fees,'expenses_cents':expense,'other_income_cents':other_income,'profit_before_costs_cents':revenue+fees+other_income-expense,'line_revenue':line_revenue,'popular':sorted(sold.items(),key=lambda x:-x[1])[:8],'low_stock':low,'upcoming':upcoming,'recent_orders':orders[:10]}

@app.post('/api/admin/{resource}')
async def create(resource:str,request:Request):
    write_guard(request)
    if resource not in TABLES:raise HTTPException(404,'Recurso desconhecido')
    d=await request.json();table,fields=TABLES[resource]
    if resource=='products':
        allowedline(d.get('line'))
        d['price_cents']=cents(d.pop('price')) if 'price' in d else d.get('price_cents',0)
        d['options_json']=json.dumps(d.get('options',[]),ensure_ascii=False)
    if resource=='buffet_items' and 'price' in d:d['price_cents']=cents(d.pop('price'))
    if resource in ('expenses','incomes'):d['amount_cents']=cents(d.pop('amount')) if 'amount' in d else d.get('amount_cents',0)
    if resource=='ingredients' and 'cost' in d:d['cost_cents_per_unit']=cents(d.pop('cost'))
    if resource=='quotes' and 'value' in d:d['value_cents']=cents(d.pop('value'))
    if resource=='recipes':d['qty']=finiteqty(d.get('qty'))
    if resource in ('expenses','incomes') and 'occurred_on' in d:d['occurred_on']=safe_date(d['occurred_on'])
    if resource=='transitions' and d.get('status','planejado') not in ('planejado','em_reducao','descontinuado'):raise HTTPException(422,'Status inválido')
    if 'name' in fields and 'name' in d:d['name']=require_str(d['name'])
    ins=obj(d,fields)
    if resource in ('categories','products','customers','expenses','incomes','testimonials','quotes','calendar_events') and 'created_at' in columns(table):ins['created_at']=now()
    if not ins:raise HTTPException(422,'Nenhum campo válido')
    sql=f"INSERT INTO {table} ({','.join(ins)}) VALUES ({','.join('?' for _ in ins)})"
    try:
        with conn() as db:oid=db.execute(sql,list(ins.values())).lastrowid
    except sqlite3.IntegrityError as e:raise HTTPException(409,'Conflito ou referência inválida') from e
    return {'id':oid}

def columns(table):
    with conn() as db:return [x['name'] for x in rows(db,f'PRAGMA table_info({table})')]

@app.patch('/api/admin/{resource}/{item_id}')
async def update(resource:str,item_id:int,request:Request):
    write_guard(request)
    if resource=='orders':
        d=await request.json()
        ins=obj(d,['status','payment_status','delivery_date','delivery_time','notes'])
        if 'status' in ins and ins['status'] not in ('aguardando_aprovacao','aceito','recusado','aguardando_pagamento','confirmado','em_producao','pronto','em_entrega','entregue','cancelado'):raise HTTPException(422,'Status inválido')
        if 'payment_status' in ins and ins['payment_status'] not in ('pendente','aguardando_comprovante','comprovante_enviado','confirmado','reembolsado'):raise HTTPException(422,'Pagamento inválido')
        table='orders'
        with conn() as db:
            current=row(db,'SELECT status,proof_file,payment_status,payment_method FROM orders WHERE id=?',(item_id,))
        if not current:raise HTTPException(404,'Pedido inexistente')
        if ins.get('status')=='aceito' and current['status']!='aguardando_aprovacao':raise HTTPException(409,'Pedido já analisado')
        if ins.get('status')=='recusado' and current['status'] in ('recusado','cancelado','entregue'):raise HTTPException(409,'Este pedido já foi finalizado')
        if ins.get('payment_status')=='confirmado' and not current['proof_file'] and not (current['payment_method']=='cartao' and current['payment_status']=='comprovante_enviado'):raise HTTPException(409,'Nenhum comprovante enviado para conferir')
        if current['status']=='recusado' and ins.get('payment_status')=='confirmado':raise HTTPException(409,'Pedido recusado')
    elif resource in TABLES:
        table,fields=TABLES[resource];d=await request.json()
        if resource=='products':
            if 'line' in d:allowedline(d['line'])
            if 'price' in d:d['price_cents']=cents(d.pop('price'))
            if 'options' in d:d['options_json']=json.dumps(d.pop('options'),ensure_ascii=False)
        if resource=='buffet_items' and 'price' in d:d['price_cents']=cents(d.pop('price'))
        if resource in ('expenses','incomes') and 'amount' in d:d['amount_cents']=cents(d.pop('amount'))
        if resource in ('expenses','incomes') and 'occurred_on' in d:d['occurred_on']=safe_date(d['occurred_on'])
        if resource=='ingredients' and 'cost' in d:d['cost_cents_per_unit']=cents(d.pop('cost'))
        if resource=='recipes' and 'qty' in d:d['qty']=finiteqty(d['qty'])
        ins=obj(d,fields)
    else:raise HTTPException(404,'Recurso inválido')
    if not ins:raise HTTPException(422,'Nenhum campo válido')
    try:
        with conn() as db:
            cur=db.execute(f"UPDATE {table} SET {','.join(k+'=?' for k in ins)} WHERE id=?",[*ins.values(),item_id])
            if not cur.rowcount:raise HTTPException(404,'Registro não encontrado')
    except sqlite3.IntegrityError:raise HTTPException(409,'Conflito ou referência inválida')
    return {'ok':True}

@app.delete('/api/admin/{resource}/{item_id}')
def delete(resource:str,item_id:int,request:Request):
    write_guard(request)
    if resource not in TABLES:raise HTTPException(404,'Recurso inválido')
    table=TABLES[resource][0]
    try:
        with conn() as db: db.execute(f'DELETE FROM {table} WHERE id=?',(item_id,))
    except sqlite3.IntegrityError:raise HTTPException(409,'Registro utilizado por outros dados; desative em vez de excluir')
    return {'ok':True}

@app.put('/api/admin/settings')
async def save_settings(request:Request):
    write_guard(request);d=await request.json()
    existing={'brand_story','hero_title','hero_subtitle','phone','instagram','address','opening_hours','pix_key','pix_instructions','delivery_fee','logo_url','hero_image_url','email','cross_contact_notice'}
    with conn() as db:
        for k,v in d.items():
            if k not in existing:continue
            if k=='delivery_fee':cents(v)
            db.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(k,str(v)[:5000]))
    return {'ok':True}

@app.post('/api/admin/stock/{ingredient_id}')
async def stock(ingredient_id:int,request:Request):
    write_guard(request);d=await request.json()
    try:delta=float(d.get('delta'))
    except Exception:raise HTTPException(422,'Movimentação inválida')
    if not -1000000<=delta<=1000000 or delta!=delta or delta==0:raise HTTPException(422,'Movimentação inválida')
    reason=require_str(d.get('reason'),300)
    with conn() as db:
        try:
            db.execute('BEGIN IMMEDIATE')
            item=row(db,'SELECT * FROM ingredients WHERE id=?',(ingredient_id,))
            if not item:raise HTTPException(404,'Ingrediente não encontrado')
            if item['stock_qty']+delta<0:raise HTTPException(409,'Estoque insuficiente')
            db.execute('UPDATE ingredients SET stock_qty=stock_qty+? WHERE id=?',(delta,ingredient_id))
            db.execute('INSERT INTO stock_moves(ingredient_id,delta,reason,created_at) VALUES(?,?,?,?)',(ingredient_id,delta,reason,now()))
            db.execute('COMMIT')
        except Exception:db.execute('ROLLBACK');raise
    return {'ok':True}

@app.post('/api/admin/orders/{order_id}/consume-stock')
async def consume(order_id:int,request:Request):
    write_guard(request)
    with conn() as db:
        try:
            db.execute('BEGIN IMMEDIATE')
            o=row(db,'SELECT * FROM orders WHERE id=?',(order_id,))
            if not o:raise HTTPException(404,'Pedido inexistente')
            marker=f'produção pedido #{order_id}'
            if row(db,'SELECT id FROM stock_moves WHERE reason=? LIMIT 1',(marker,)):raise HTTPException(409,'Estoque já baixado para este pedido')
            req=rows(db,'SELECT r.ingredient_id,SUM(r.qty*oi.qty) total FROM order_items oi JOIN recipes r ON r.product_id=oi.product_id WHERE oi.order_id=? GROUP BY r.ingredient_id',(order_id,))
            if not req:raise HTTPException(409,'Nenhuma ficha técnica vinculada aos produtos deste pedido')
            for x in req:
                ing=row(db,'SELECT * FROM ingredients WHERE id=?',(x['ingredient_id'],))
                if ing['stock_qty']<x['total']:raise HTTPException(409,f"Estoque insuficiente: {ing['name']}")
            for x in req:
                db.execute('UPDATE ingredients SET stock_qty=stock_qty-? WHERE id=?',(x['total'],x['ingredient_id']))
                db.execute('INSERT INTO stock_moves(ingredient_id,delta,reason,created_at) VALUES(?,?,?,?)',(x['ingredient_id'],-x['total'],marker,now()))
            db.execute('COMMIT')
        except Exception:db.execute('ROLLBACK');raise
    return {'ok':True}

@app.get('/api/admin/costs')
def costs(request:Request):
    auth(request)
    with conn() as db:
        return rows(db,'''SELECT p.id,p.name,p.price_cents,COALESCE(SUM(r.qty*i.cost_cents_per_unit),0) recipe_cost_cents,COUNT(r.id) ingredient_count FROM products p LEFT JOIN recipes r ON p.id=r.product_id LEFT JOIN ingredients i ON i.id=r.ingredient_id GROUP BY p.id ORDER BY p.id DESC''')

@app.get('/api/admin/transition-simulation')
def simulation(request:Request,remove_ids:str=''):
    auth(request)
    ids=[]
    if remove_ids:
        try:ids=[int(x) for x in remove_ids.split(',') if x.strip()][:100]
        except ValueError:raise HTTPException(422,'IDs inválidos')
    with conn() as db:
        sales=rows(db,"""SELECT oi.product_id,oi.product_name,oi.product_line,SUM(oi.unit_price_cents*oi.qty) revenue_cents FROM order_items oi JOIN orders o ON oi.order_id=o.id WHERE o.payment_status='confirmado' AND o.status!='cancelado' GROUP BY oi.product_id,oi.product_name,oi.product_line ORDER BY revenue_cents DESC""")
    lost=sum(x['revenue_cents'] for x in sales if x['product_id'] in ids)
    total=sum(x['revenue_cents'] for x in sales)
    return {'sales':sales,'simulated_loss_cents':int(lost),'historical_revenue_cents':int(total),'remaining_cents':int(total-lost),'notice':'Simulação retrospectiva baseada nas vendas confirmadas; não prevê substituição de demanda nem custos.'}

@app.get('/api/admin/export/{resource}')
def export(resource:str,request:Request):
    auth(request)
    if resource not in ('orders','products','customers','expenses','incomes','ingredients','quotes'):raise HTTPException(404,'Relatório desconhecido')
    with conn() as db:data=rows(db,f'SELECT * FROM {resource} ORDER BY id DESC')
    output=io.StringIO(); writer=csv.writer(output)
    if data:
        writer.writerow([CSV_COL.get(k,k) for k in data[0].keys()])
        for r in data:writer.writerow([str(v) if v is not None else '' for v in r.values()])
    body='\ufeff'+output.getvalue()
    return Response(body,media_type='text/csv; charset=utf-8',headers={'Content-Disposition':f'attachment; filename="{resource}.csv"'})

@app.exception_handler(sqlite3.IntegrityError)
async def integrity(request,exc):return JSONResponse(status_code=409,content={'detail':'Conflito de dados ou referência inexistente'})
