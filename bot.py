import logging
import asyncio
import pickle
import os
import re
import time
from datetime import datetime, timedelta
import pytz
import pathlib
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatPermissions
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from notion_client import Client
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import google.generativeai as genai
from google.api_core import exceptions as google_exceptions

# --- CARREGA .ENV ---
load_dotenv()

# --- CONFIGURAÇÕES ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
NOTION_TOKEN = os.getenv('NOTION_TOKEN')
NOTION_TICKETS_DB_ID = os.getenv('NOTION_TICKETS_DB_ID')
NOTION_CLIENTS_DB_ID = os.getenv('NOTION_CLIENTS_DB_ID')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
STATE_FILE = os.getenv('STATE_FILE', 'bot_state.pkl')

try: ADMIN_ID = int(os.getenv('ADMIN_ID'))
except: ADMIN_ID = 0

TIMEZONE = pytz.timezone('America/Sao_Paulo')

# --- REGRAS ---
DIAS_UTEIS = [0, 1, 2, 3, 4] 
HORA_INICIO_EXPEDIENTE = 8
HORA_FIM_EXPEDIENTE = 18
MINUTOS_INATIVIDADE = 30 

NUMERO_PLANTONISTA = "(11) 99999-9999"
NOME_PLANTONISTA = "Daniel"

# Setup
notion = Client(auth=NOTION_TOKEN)
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# CONSTANTES
WAITING_NEW_TICKET = 1 
WAITING_COMMENT = 2    

# MEMÓRIA
CLIENT_GROUPS = {} 
user_states = {}   
last_activity = {} 
group_status = {}  
session_logs = {}       
active_ticket_session = {} 
ticket_first_session = {} 
prompt_messages = {}

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- PERSISTÊNCIA ASSÍNCRONA (OTIMIZAÇÃO) ---
async def save_state_async():
    """Salva estado em background para não travar o bot."""
    data = {
        'CLIENT_GROUPS': CLIENT_GROUPS, 
        'user_states': user_states, 
        'last_activity': last_activity, 
        'group_status': group_status,
        'active_ticket_session': active_ticket_session,
        'ticket_first_session': ticket_first_session
    }
    try:
        # Executa I/O em thread separada
        await asyncio.to_thread(write_pickle, data)
    except Exception as e: logger.error(f"Erro save async: {e}")

def write_pickle(data):
    with open(STATE_FILE, 'wb') as f: pickle.dump(data, f)

def load_state():
    global CLIENT_GROUPS, user_states, last_activity, group_status, active_ticket_session, ticket_first_session
    if not os.path.exists(STATE_FILE): return
    try:
        with open(STATE_FILE, 'rb') as f:
            data = pickle.load(f)
            if 'CLIENT_GROUPS' in data: CLIENT_GROUPS = data['CLIENT_GROUPS']
            if 'user_states' in data: user_states = data['user_states']
            if 'last_activity' in data: last_activity = data['last_activity']
            if 'group_status' in data: group_status = data['group_status']
            if 'active_ticket_session' in data: active_ticket_session = data['active_ticket_session']
            if 'ticket_first_session' in data: ticket_first_session = data['ticket_first_session']
    except: pass

# --- FUNÇÕES IA ---
def generate_ai_analysis(messages, current_desc, is_first_session):
    if not GEMINI_API_KEY or not messages: return None
    
    models_to_try = ['gemini-2.0-flash', 'gemini-2.5-flash', 'gemini-flash-latest']
    chat_history = "\n".join(messages)
    
    instruction_title = ""
    if is_first_session:
        instruction_title = (
            f"3. Analise a descrição: '{current_desc}'. "
            "Se vaga, sugira título técnico curto (Max 50 chars). "
            "Tag: [NOVO_TITULO: Titulo].\n"
        )

    prompt = (
        "Atue como Consultor Sênior ISP.\n"
        "Gere relatório técnico profissional.\n\n"
        "ESTILO:\n- Direto, listas (•), sem asteriscos (**), use CAIXA ALTA para títulos.\n\n"
        "FECHAMENTO:\n- Se resolvido, adicione [FECHAR_CHAMADO].\n"
        f"{instruction_title}\n"
        "ESTRUTURA:\n"
        "🚩 OCORRÊNCIA\n[Resumo]\n\n"
        "🛠️ AÇÕES REALIZADAS\n• [Ação 1]\n\n"
        "🏁 SITUAÇÃO ATUAL\n[Status]\n\n"
        f"--- LOG (NÃO COPIAR) ---\n{chat_history}"
    )

    last_error = ""
    for model_name in models_to_try:
        try:
            model = genai.GenerativeModel(model_name)
            for attempt in range(3):
                try:
                    response = model.generate_content(prompt)
                    return response.text
                except Exception as e:
                    if "429" in str(e):
                        time.sleep(2)
                        continue
                    else: raise e
        except Exception as e:
            last_error = str(e)
            continue 
    return f"IA Indisponível: {last_error}"

def transcribe_audio(file_path):
    if not GEMINI_API_KEY: return "Erro: Sem Chave API"
    try:
        myfile = genai.upload_file(file_path, mime_type="audio/ogg")
        timeout = 0
        while myfile.state.name == "PROCESSING":
            time.sleep(1)
            timeout += 1
            myfile = genai.get_file(myfile.name)
            if timeout > 30: return "Timeout Google."
        if myfile.state.name == "FAILED": return "Google falhou."

        model = genai.GenerativeModel("gemini-2.0-flash")
        for attempt in range(3):
            try:
                result = model.generate_content(["Transcreva fielmente:", myfile])
                return result.text
            except Exception as e:
                if "429" in str(e): time.sleep(2); continue
                else: return f"Erro IA: {str(e)}"
        return "Cota IA excedida."
    except Exception as e: return f"Erro Técnico: {str(e)}"

# --- NOTION & CLIENTES ---
def refresh_clients_from_notion():
    global CLIENT_GROUPS
    try:
        response = notion.databases.query(database_id=NOTION_CLIENTS_DB_ID, filter={"property": "Ativo", "checkbox": {"equals": True}})
        new_map = {}
        for page in response['results']:
            try:
                name = page['properties']['Name']['title'][0]['text']['content']
                chat_id = int(page['properties']['ChatID']['rich_text'][0]['text']['content'].strip())
                new_map[chat_id] = name
            except: pass
        CLIENT_GROUPS = new_map
        # Salva de forma síncrona aqui pois é job de background
        try: write_pickle({
            'CLIENT_GROUPS': CLIENT_GROUPS, 'user_states': user_states, 
            'last_activity': last_activity, 'group_status': group_status,
            'active_ticket_session': active_ticket_session, 'ticket_first_session': ticket_first_session
        })
        except: pass
        return f"OK: {len(new_map)} clientes."
    except Exception as e: return f"Erro: {str(e)}"

def get_client_name(chat_id):
    if chat_id not in CLIENT_GROUPS: refresh_clients_from_notion()
    return CLIENT_GROUPS.get(chat_id, str(chat_id))

def generate_next_id():
    return datetime.now(TIMEZONE).strftime("%Y%m%d%H%M%S")

def sanitize_notion_text(text):
    if not text: return "Chamado sem Título"
    clean = re.sub(r'[*_`]', '', text)
    clean = clean.replace('\n', ' ').strip()
    return clean[:100]

def create_ticket(user, desc, chat_id):
    try:
        tid = generate_next_id()
        client = get_client_name(chat_id)
        date_iso = datetime.now(TIMEZONE).isoformat()
        safe_desc = str(desc) if desc else "Sem descrição"
        notion.pages.create(
            parent={"database_id": NOTION_TICKETS_DB_ID},
            properties={
                "Name": {"title": [{"text": {"content": tid}}]},
                "Descricao": {"rich_text": [{"text": {"content": safe_desc}}]},
                "Solicitante": {"rich_text": [{"text": {"content": user}}]},
                "Status": {"status": {"name": "Em Andamento"}}, 
                "ChatID": {"rich_text": [{"text": {"content": client}}]},
                "Date": {"date": {"start": date_iso}}
            }
        )
        return tid, None
    except Exception as e: return None, str(e)

def get_ticket_desc(ticket_id):
    try:
        res = notion.databases.query(database_id=NOTION_TICKETS_DB_ID, filter={"property": "Name", "title": {"equals": ticket_id}})
        if not res['results']: return ""
        props = res['results'][0]['properties']
        if 'Descricao' in props and props['Descricao']['rich_text']:
            return props['Descricao']['rich_text'][0]['text']['content']
        return ""
    except: return ""

def update_ticket_properties(ticket_id, updates):
    try:
        res = notion.databases.query(database_id=NOTION_TICKETS_DB_ID, filter={"property": "Name", "title": {"equals": ticket_id}})
        if not res['results']: return False
        page_id = res['results'][0]['id']
        notion.pages.update(page_id=page_id, properties=updates)
        return True
    except: return False

def get_active_tickets_data(chat_id):
    client = get_client_name(chat_id)
    try:
        f = {"and": [{"property": "Status", "status": {"equals": "Em Andamento"}}, {"property": "ChatID", "rich_text": {"equals": client}}]}
        response = notion.databases.query(database_id=NOTION_TICKETS_DB_ID, filter=f)
        data = []
        for p in response['results']:
            try: t_id = p['properties']['Name']['title'][0]['text']['content']
            except: t_id = "?"
            try: d = p['properties']['Descricao']['rich_text'][0]['text']['content']
            except: d = "..."
            if len(d) > 25: d = d[:25] + "..."
            data.append({"id": t_id, "desc": d})
        return data
    except Exception as e: return []

def append_comment_to_ticket(ticket_id, user, text, is_summary=False):
    try:
        res = notion.databases.query(database_id=NOTION_TICKETS_DB_ID, filter={"property": "Name", "title": {"equals": ticket_id}})
        if not res['results']: return False, "Não encontrado."
        pid = res['results'][0]['id']
        ts = datetime.now(TIMEZONE).strftime("%d/%m %H:%M")
        children = []
        if is_summary:
            children = [
                {"object": "block", "type": "heading_3", "heading_3": {"rich_text": [{"type": "text", "text": {"content": f"🤖 Resumo IA ({ts})"}}]}},
                {"object": "block", "type": "quote", "quote": {"rich_text": [{"type": "text", "text": {"content": text}}]}},
                {"object": "block", "type": "divider", "divider": {}}
            ]
        else:
            children = [{"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": f"💬 {ts} - {user}:\n{text}"}}]}}]
        notion.blocks.children.append(block_id=pid, children=children)
        return True, ""
    except Exception as e: return False, str(e)

def get_ticket_history(ticket_id):
    try:
        res = notion.databases.query(database_id=NOTION_TICKETS_DB_ID, filter={"property": "Name", "title": {"equals": ticket_id}})
        if not res['results']: return "Não encontrado."
        blocks = notion.blocks.children.list(block_id=res['results'][0]['id'])
        hist = []
        def get_text(rich_text_list): return "".join([t['plain_text'] for t in rich_text_list]) if rich_text_list else ""
        for b in blocks['results']:
            try:
                b_type = b['type']
                content = ""
                if b_type == 'divider': hist.append("━━━━━━━━━━━━━━━━")
                elif b_type == 'paragraph': content = get_text(b['paragraph'].get('rich_text', []))
                elif b_type == 'to_do': content = f"{'✅' if b['to_do'].get('checked') else '⬜'} {get_text(b['to_do'].get('rich_text', []))}"
                elif b_type == 'toggle': content = f"▶️ {get_text(b['toggle'].get('rich_text', []))}"
                elif b_type == 'bulleted_list_item': content = f"• {get_text(b['bulleted_list_item'].get('rich_text', []))}"
                elif b_type == 'numbered_list_item': content = f"1. {get_text(b['numbered_list_item'].get('rich_text', []))}"
                elif b_type == 'quote': content = f"{get_text(b['quote'].get('rich_text', []))}"
                elif b_type == 'heading_3': content = f"\n**{get_text(b['heading_3'].get('rich_text', []))}**"
                if content: hist.append(content)
            except: continue
        return "\n\n".join(hist) if hist else "Nenhuma observação."
    except Exception as e: return f"Erro leitura: {str(e)}"

# --- HELPER ---
def is_business_hours():
    n = datetime.now(TIMEZONE)
    if n.weekday() not in DIAS_UTEIS: return False
    if HORA_INICIO_EXPEDIENTE <= n.hour < HORA_FIM_EXPEDIENTE: return True
    return False

async def open_group_globally(chat_id, context):
    try:
        p = ChatPermissions(can_send_messages=True, can_send_audios=True, can_send_documents=True, can_send_photos=True, can_send_videos=True, can_send_voice_notes=True, can_send_other_messages=True)
        await context.bot.set_chat_permissions(chat_id, p)
        group_status[chat_id] = 'OPEN'
        last_activity[chat_id] = datetime.now(TIMEZONE)
        session_logs[chat_id] = [] 
        await save_state_async()
        return True, ""
    except Exception as e: return False, str(e)

async def lock_group_globally(chat_id, context):
    try:
        await context.bot.set_chat_permissions(chat_id, ChatPermissions(can_send_messages=False))
        group_status[chat_id] = 'CLOSED'
        
        logs = session_logs.get(chat_id, [])
        ticket_id = active_ticket_session.get(chat_id)
        is_first = ticket_first_session.get(ticket_id, False)
        
        final_msg = "🔒 *Atendimento Encerrado.*" 
        
        if logs and ticket_id:
            current_desc = get_ticket_desc(ticket_id)
            analysis = generate_ai_analysis(logs, current_desc, is_first)
            
            if analysis and "Erro IA" not in analysis:
                notion_updates = {}
                actions_taken = []

                if "[NOVO_TITULO:" in analysis:
                    match = re.search(r'\[NOVO_TITULO: (.*?)\]', analysis)
                    if match:
                        raw_title = match.group(1).strip()
                        new_title = sanitize_notion_text(raw_title)
                        notion_updates["Descricao"] = {"rich_text": [{"text": {"content": new_title}}]}
                        actions_taken.append(f"🔄 Título ajustado: *'{new_title}'*")
                        analysis = analysis.replace(match.group(0), "")

                if "[FECHAR_CHAMADO]" in analysis:
                    analysis = analysis.replace("[FECHAR_CHAMADO]", "")
                    notion_updates["Status"] = {"status": {"name": "Finalizado"}}
                    actions_taken.append("✨ Chamado encerrado (Resolvido).")

                if notion_updates:
                    update_ticket_properties(ticket_id, notion_updates)

                append_comment_to_ticket(ticket_id, "IA Bot", analysis, is_summary=True)
                final_msg += "\n\n✅ *Relatório IA:*"
                if actions_taken:
                    for action in actions_taken: final_msg += f"\n{action}"
                else:
                    final_msg += "\nResumo anexado ao histórico."
            else:
                final_msg += f"\n\n⚠️ Erro na IA: {analysis}"
        
        await context.bot.send_message(chat_id, final_msg, parse_mode='Markdown')
        
        if chat_id in session_logs: del session_logs[chat_id]
        if chat_id in active_ticket_session: del active_ticket_session[chat_id]
        if ticket_id in ticket_first_session: del ticket_first_session[ticket_id]
        
        await save_state_async()
        return True, ""
    except Exception as e: return False, str(e)

# --- COMANDOS ---
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return 
    refresh_clients_from_notion()
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("⚠️ Use: `/aviso msg`")
        return
    st = await update.message.reply_text(f"⏳ Enviando...")
    s, f = 0, 0
    for cid in CLIENT_GROUPS:
        try:
            await context.bot.send_message(cid, f"📢 *COMUNICADO HypeIT*\n━━━━━━━━\n\n{msg}", parse_mode='Markdown')
            s += 1
        except: f += 1
    await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=st.message_id, text=f"✅ OK: {s} | Falhas: {f}")

async def debug_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    sync = refresh_clients_from_notion()
    cid = update.effective_chat.id
    st = group_status.get(cid, "UNK")
    logs = len(session_logs.get(cid, []))
    active = active_ticket_session.get(cid, "Nenhum")
    await update.message.reply_text(f"🛠 *Status*\nSync: {sync}\nGrupo: {st}\nMsgs Log: {logs}\nTicket Ativo: {active}", parse_mode='Markdown')

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    name = get_client_name(cid)
    if str(cid) == name: 
        await context.bot.send_message(cid, f"⚠️ Grupo {cid} não cadastrado.")
        return
    await lock_group_globally(cid, context)
    await show_menu_new_msg(cid, context, f"🤖 *Atendimento {name}*")

async def manual_lock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await lock_group_globally(update.effective_chat.id, context)
    await show_menu_new_msg(update.effective_chat.id, context, "🔒 *Menu*")

async def show_menu_new_msg(chat_id, context, text):
    kb = [
        [InlineKeyboardButton("📝 Abrir Novo Chamado", callback_data='check')],
        [InlineKeyboardButton("🗣️ Falar sobre Chamado", callback_data='list_update')],
        [InlineKeyboardButton("📊 Consultar Andamento", callback_data='list_view')]
    ]
    await context.bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

async def btn_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    
    # 1. RESPOSTA IMEDIATA PARA NÃO TRAVAR O BOTÃO
    try: await q.answer()
    except: pass
    
    uid, cid = q.from_user.id, update.effective_chat.id
    k = f"{uid}_{cid}"

    if q.data == 'check':
        if is_business_hours(): await flow_new(cid, uid, context, q)
        else:
            kb = [[InlineKeyboardButton("✅ Sim, aguardo", callback_data='wait_yes')], [InlineKeyboardButton("❌ Não, urgente", callback_data='wait_no')]]
            await q.edit_message_text("🌙 *Fora de horário.* Pode aguardar?", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
    
    elif q.data == 'wait_yes': await flow_new(cid, uid, context, q)
    elif q.data == 'wait_no':
        ok, e = await open_group_globally(cid, context)
        if ok:
            user_states[k] = {"state": WAITING_NEW_TICKET}
            await save_state_async()
            kb = [[InlineKeyboardButton("🔙 Cancelar", callback_data='cancel')]]
            msg_plantao = (
                f"🚨🚨 *ATENÇÃO: MODO PLANTÃO* 🚨🚨\n\n"
                f"⚠️ Para atendimento imediato, você *DEVE LIGAR* para:\n"
                f"📞 *{NUMERO_PLANTONISTA}* - Falar com {NOME_PLANTONISTA}\n\n"
                f"🔓 O grupo foi liberado para registro, mas *ligue* após enviar a mensagem (texto ou áudio)."
            )
            msg = await q.edit_message_text(msg_plantao, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
            prompt_messages[k] = msg.message_id
        else: await q.edit_message_text(f"Erro: {e}")

    elif q.data == 'cancel':
        if k in user_states: del user_states[k]; await save_state_async()
        await lock_group_globally(cid, context)
        await menu_inline(q, "🚫 *Operação Cancelada.*")

    elif q.data in ['list_update', 'list_view']:
        tkts = get_active_tickets_data(cid)
        if not tkts:
            kb = [[InlineKeyboardButton("🔙 Voltar", callback_data='back')]]
            await q.edit_message_text("📂 Nenhum chamado ativo.", reply_markup=InlineKeyboardMarkup(kb))
            return
        kb = []
        p = "upd_" if q.data == 'list_update' else "vw_"
        for t in tkts: kb.append([InlineKeyboardButton(f"[{t['id']}] {t['desc']}", callback_data=f"{p}{t['id']}")])
        kb.append([InlineKeyboardButton("🔙 Voltar", callback_data='back')])
        await q.edit_message_text("👇 *Selecione:*", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif q.data.startswith('upd_'):
        tid = q.data.split('_')[1]
        ok, e = await open_group_globally(cid, context)
        if ok:
            user_states[k] = {"state": WAITING_COMMENT, "ticket_id": tid}
            active_ticket_session[cid] = tid 
            await save_state_async()
            await q.edit_message_text(f"🔓 *Liberado!*\nFalando sobre: `{tid}`\n\n@{q.from_user.username}, pode digitar ou enviar um áudio.", parse_mode='Markdown')
        else: await q.edit_message_text(f"Erro: {e}")

    elif q.data.startswith('vw_'):
        tid = q.data.split('_')[1]
        hist = get_ticket_history(tid)
        kb = [[InlineKeyboardButton("🔙 Voltar", callback_data='back')]]
        await q.edit_message_text(f"📊 *Histórico {tid}:*\n\n{hist}", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif q.data == 'back':
        await menu_inline(q, "🤖 *Menu Principal*")

async def flow_new(cid, uid, context, q):
    ok, e = await open_group_globally(cid, context)
    if ok:
        user_states[f"{uid}_{cid}"] = {"state": WAITING_NEW_TICKET}
        await save_state_async()
        kb = [[InlineKeyboardButton("🔙 Cancelar", callback_data='cancel')]]
        msg = await q.edit_message_text(f"🔓 *Liberado!*\n@{q.from_user.username}, digite ou grave um áudio sobre o problema:", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        prompt_messages[f"{uid}_{cid}"] = msg.message_id
    else: await q.edit_message_text(f"Erro: {e}")

async def menu_inline(q, text):
    kb = [
        [InlineKeyboardButton("📝 Abrir Novo Chamado", callback_data='check')],
        [InlineKeyboardButton("🗣️ Falar sobre Chamado", callback_data='list_update')],
        [InlineKeyboardButton("📊 Consultar Andamento", callback_data='list_view')]
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

async def msg_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid, cid = update.effective_user.id, update.effective_chat.id
    user_name = update.effective_user.first_name
    
    text_content = ""
    
    if update.message.text:
        text_content = update.message.text
        
    elif update.message.photo:
        text_content = "[O usuário enviou uma IMAGEM]"
        
    elif update.message.voice:
        status_msg = await update.message.reply_text("🎙️ Transcrevendo áudio...")
        try:
            file_id = update.message.voice.file_id
            new_file = await context.bot.get_file(file_id)
            file_path = f"temp_voice_{cid}.ogg"
            await new_file.download_to_drive(file_path)
            
            transcription = transcribe_audio(file_path)
            
            if transcription and not transcription.startswith("Erro"):
                text_content = transcription 
                safe_transcription = sanitize_notion_text(transcription)
                await status_msg.edit_text(f"🎙️ **Transcrição:**\n_{safe_transcription}_", parse_mode='Markdown')
            else:
                error_msg = transcription if transcription else "Erro desconhecido"
                text_content = f"[Áudio enviado ({error_msg})]"
                await status_msg.edit_text(f"⚠️ {error_msg}")
            
            if os.path.exists(file_path): os.remove(file_path)
            
        except Exception as e:
            logger.error(f"Erro audio: {e}")
            text_content = "[Áudio enviado (Erro processamento)]"
            await status_msg.edit_text("⚠️ Erro ao processar áudio.")

    if not text_content: return 

    last_activity[cid] = datetime.now(TIMEZONE)
    await save_state_async()
    
    if group_status.get(cid) == 'OPEN':
        if cid not in session_logs: session_logs[cid] = []
        session_logs[cid].append(f"{user_name}: {text_content}")
        
        if update.message.photo:
            tid = active_ticket_session.get(cid)
            if tid: append_comment_to_ticket(tid, user_name, "📷 [IMAGEM ENVIADA PELO CLIENTE]")

    k = f"{uid}_{cid}"
    data = user_states.get(k)
    
    if data:
        st = data.get("state")
        
        if st == WAITING_NEW_TICKET:
            safe_desc = sanitize_notion_text(text_content)
            
            tid, e = create_ticket(user_name, safe_desc, cid)
            if e: await update.message.reply_text(f"❌ Erro: {e}")
            else: 
                await update.message.reply_text(f"✅ *Chamado {tid} Aberto!*", parse_mode='Markdown')
                active_ticket_session[cid] = tid
                ticket_first_session[tid] = True
                
                try:
                    pid = prompt_messages.get(k)
                    if pid: await context.bot.delete_message(cid, pid); del prompt_messages[k]
                except: pass

            del user_states[k]
            await save_state_async()
            
        elif st == WAITING_COMMENT:
            pass 

async def job_init(app):
    load_state()
    if not CLIENT_GROUPS: refresh_clients_from_notion()
    s = AsyncIOScheduler(timezone=TIMEZONE)
    s.add_job(refresh_clients_from_notion, 'interval', minutes=30)
    
    async def lock():
        for c in CLIENT_GROUPS:
            try: await lock_group_globally(c, app); await show_menu_new_msg(c, app, "🔒 *Menu Automático*")
            except: pass
    async def inact():
        n = datetime.now(TIMEZONE)
        for c in CLIENT_GROUPS:
            if group_status.get(c) == 'OPEN' and last_activity.get(c) and (n - last_activity[c] > timedelta(minutes=MINUTOS_INATIVIDADE)):
                try: await lock_group_globally(c, app); await show_menu_new_msg(c, app, "🔒 *Fechado por Inatividade*")
                except Exception as e: logger.error(f"Erro inact {c}: {e}")
    s.add_job(lock, 'cron', day_of_week='mon-fri', hour=HORA_INICIO_EXPEDIENTE)
    s.add_job(lock, 'cron', day_of_week='mon-fri', hour=HORA_FIM_EXPEDIENTE)
    s.add_job(inact, 'interval', minutes=1)
    s.start()

if __name__ == '__main__':
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(job_init).build()
    application.add_handler(CommandHandler(['start', 'iniciar'], start))
    application.add_handler(CommandHandler('fim', manual_lock))
    application.add_handler(CommandHandler('aviso', broadcast_command))
    application.add_handler(CommandHandler('debug', debug_cmd))
    application.add_handler(CallbackQueryHandler(btn_handler))
    application.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.VOICE) & ~filters.COMMAND, msg_handler))
    print("Bot rodando com persistência e IA v6.6...")
    application.run_polling()
