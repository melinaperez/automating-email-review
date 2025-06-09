import imaplib
import email
from email.mime.multipart import MIMEMultipart
import os
import re
from datetime import datetime, timedelta, timezone
import pandas as pd
import logging
from typing import List, Dict, Tuple, Optional
import json
import time
from email.utils import parsedate_to_datetime

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ImprovedEmailReader:
    def __init__(self, email_config: Dict[str, str]):
        """
        Inicializa el lector de emails mejorado
        
        Args:
            email_config: Diccionario con configuración del email
        """
        self.server = email_config['server']
        self.email = email_config['email']
        self.password = email_config['password']
        self.port = email_config.get('port', 993)
        self.mail = None
        self.processed_ids = set()
        
    def connect(self) -> bool:
        """Conecta al servidor de email"""
        try:
            self.mail = imaplib.IMAP4_SSL(self.server, self.port)
            self.mail.login(self.email, self.password)
            logger.info(f"Conectado exitosamente a {self.email}")
            return True
        except Exception as e:
            logger.error(f"Error conectando al email: {e}")
            return False
    
    def extract_patient_name(self, subject: str, sender: str = "") -> str:
        """Extrae el nombre del paciente del asunto del email y remitente"""
        subject = subject.strip()
        
        logger.debug(f"Analizando asunto: '{subject}' de remitente: '{sender}'")
        
        # Patrones específicos para OMRON (orden de prioridad)
        omron_patterns = [
            r'\[OMRON\]\s*Informe de ECG\s*-\s*([A-Za-z0-9\-_\.]+)',
            r'\[OMRON\]\s*Los datos de medición\s*-\s*([A-Za-z0-9\-_\.]+)',
            r'\[OMRON\].*?-\s*([A-Za-z0-9\-_\.]+)',
            r'OMRON.*?-\s*([A-Za-z0-9\-_\.]+)',
        ]
        
        # Probar patrones OMRON primero
        for pattern in omron_patterns:
            match = re.search(pattern, subject, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                if name and len(name) > 1:
                    clean_name = self.clean_patient_name(name)
                    if clean_name:
                        logger.info(f"Nombre extraído de OMRON: '{clean_name}'")
                        return clean_name
        
        # Patrones para otros formatos médicos
        medical_patterns = [
            r'paciente\s+([A-Za-z0-9\-_\s]+?)(?:\s+de\s+|\s*$)',
            r'[Pp]aciente:?\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)',
            r'^([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)\s*-\s*[Mm]edici[oó]n',
            r'^([A-Za-zÁÉÍÓÚáéíóúñÑ\s]{3,}?)(?:\s*[-:]|\s*$)',
        ]
        
        for pattern in medical_patterns:
            match = re.search(pattern, subject, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                clean_name = self.clean_patient_name(name)
                if clean_name and len(clean_name) > 2:
                    logger.info(f"Nombre extraído de patrón médico: '{clean_name}'")
                    return clean_name
        
        # Si no se encuentra en el asunto, intentar extraer del remitente
        if sender:
            sender_name = self.extract_name_from_sender(sender)
            if sender_name:
                logger.info(f"Nombre extraído del remitente: '{sender_name}'")
                return sender_name
        
        logger.warning(f"No se pudo extraer nombre del asunto: '{subject}'")
        return "PACIENTE_DESCONOCIDO"
    
    def clean_patient_name(self, name: str) -> str:
        """Limpia y normaliza el nombre del paciente"""
        if not name:
            return ""
        
        # Limpiar caracteres de codificación UTF-8 mal decodificados
        name = re.sub(r'Utf-8Q.*?c3A.*?n', '', name)
        name = re.sub(r'[^\w\s\-\.]', ' ', name)
        
        # Remover palabras comunes que no son nombres
        exclude_words = [
            'omron', 'informe', 'datos', 'medicion', 'ecg', 'ekg', 'ta', 
            'dia', 'noche', 'colombia', 'de', 'la', 'el', 'y', 'del', 'utf', '8q'
        ]
        
        words = name.split()
        clean_words = []
        
        for word in words:
            if word.lower() not in exclude_words and len(word) > 1:
                clean_words.append(word)
        
        if clean_words:
            result = ' '.join(clean_words).title()
            if len(result) < 3:
                return name.strip()
            return result
        
        return name.strip()
    
    def extract_name_from_sender(self, sender: str) -> str:
        """Extrae nombre del campo remitente del email"""
        sender = re.sub(r'Utf-8Q.*?c3A.*?n', '', sender)
        
        if '<' in sender:
            name_part = sender.split('<')[0].strip()
            if name_part and len(name_part) > 2:
                return self.clean_patient_name(name_part)
        
        if '@' in sender:
            email_part = sender.split('@')[0]
            email_part = re.sub(r'[<>"]', '', email_part)
            if len(email_part) > 2:
                return self.clean_patient_name(email_part)
        
        return ""
    
    def extract_sender_email(self, sender: str) -> str:
        """Extrae solo la dirección de email del remitente"""
        if not sender:
            return "unknown@email.com"
        
        email_match = re.search(r'<([^>]+)>', sender)
        if email_match:
            return email_match.group(1).strip()
        
        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', sender)
        if email_match:
            return email_match.group(1).strip()
        
        clean_sender = re.sub(r'[^\w\-\.]', '_', sender)
        return clean_sender[:30]
    
    def parse_email_date(self, date_str: str) -> datetime:
        """Parsea la fecha del email y devuelve datetime"""
        if not date_str:
            return datetime.now()
        
        try:
            parsed_date = parsedate_to_datetime(date_str)
            if parsed_date.tzinfo is not None:
                parsed_date = parsed_date.replace(tzinfo=None)
            return parsed_date
        except Exception as e:
            logger.warning(f"Error parseando fecha '{date_str}': {e}")
            return datetime.now()
    
    def get_new_emails(self, days_back: int = 30, force_all: bool = True) -> List[Dict]:
        """Obtiene emails con adjuntos"""
        if not self.mail:
            logger.error("No hay conexión activa al email")
            return []
        
        try:
            self.mail.select('INBOX')
            email_list = []
            
            logger.info("Buscando TODOS los emails...")
            search_criteria = 'ALL'
            status, messages = self.mail.search(None, search_criteria)
            
            if status != 'OK' or not messages[0]:
                logger.error("Error buscando emails o no hay emails")
                return []
            
            message_ids = messages[0].split()
            total_emails = len(message_ids)
            logger.info(f"Total de emails en la bandeja: {total_emails}")
            
            message_ids.reverse()
            
            max_emails = 200
            if len(message_ids) > max_emails:
                logger.info(f"Limitando a los {max_emails} emails más recientes")
                message_ids = message_ids[:max_emails]
            
            processed = 0
            with_attachments = 0
            
            for msg_id in message_ids:
                processed += 1
                if processed % 10 == 0:
                    logger.info(f"Procesando email {processed}/{len(message_ids)}...")
                
                try:
                    if msg_id in self.processed_ids:
                        logger.debug(f"Email {msg_id.decode()} ya procesado anteriormente, omitiendo")
                        continue
                    
                    email_data = self.process_email(msg_id)
                    self.processed_ids.add(msg_id)
                    
                    if email_data and email_data.get('attachments'):
                        with_attachments += 1
                        email_list.append(email_data)
                        logger.info(f"Email con adjuntos encontrado: {email_data['patient_name']} - {len(email_data['attachments'])} archivos")
                        time.sleep(0.1)
                        
                except Exception as e:
                    logger.error(f"Error procesando email {msg_id}: {e}")
                    continue
            
            logger.info(f"Procesados {processed} emails, {with_attachments} con adjuntos relevantes")
            return email_list
            
        except Exception as e:
            logger.error(f"Error obteniendo emails: {e}")
            return []
    
    def process_email(self, msg_id: bytes) -> Optional[Dict]:
        """Procesa un email individual y extrae información relevante"""
        try:
            status, msg_data = self.mail.fetch(msg_id, '(RFC822)')
            if status != 'OK':
                return None
            
            email_body = msg_data[0][1]
            email_message = email.message_from_bytes(email_body)
            
            subject = email_message['Subject'] or ""
            sender = email_message['From'] or ""
            date_str = email_message['Date'] or ""
            
            # Decodificar asunto si está codificado
            if subject:
                try:
                    decoded_subject = email.header.decode_header(subject)
                    subject = ''.join([
                        part[0].decode(part[1] or 'utf-8') if isinstance(part[0], bytes) else part[0]
                        for part in decoded_subject
                    ])
                except Exception as e:
                    logger.warning(f"Error decodificando asunto: {e}")
            
            # Decodificar remitente si está codificado
            if sender:
                try:
                    decoded_sender = email.header.decode_header(sender)
                    sender = ''.join([
                        part[0].decode(part[1] or 'utf-8') if isinstance(part[0], bytes) else part[0]
                        for part in decoded_sender
                    ])
                except Exception as e:
                    logger.warning(f"Error decodificando remitente: {e}")
            
            # Procesar adjuntos primero
            attachments = self.extract_attachments(email_message)
            
            if not attachments:
                return None
            
            patient_name = self.extract_patient_name(subject, sender)
            sender_email = self.extract_sender_email(sender)
            email_date = self.parse_email_date(date_str)
            
            logger.info(f"Email procesado: {patient_name} ({sender_email}) - Fecha: {email_date.strftime('%Y-%m-%d %H:%M')} - Adjuntos: {len(attachments)}")
            
            return {
                'message_id': msg_id.decode(),
                'subject': subject,
                'sender': sender,
                'sender_email': sender_email,
                'date': date_str,
                'email_date': email_date,
                'patient_name': patient_name,
                'attachments': attachments,
                'processed_date': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error procesando email: {e}")
            return None
    
    def extract_attachments(self, email_message) -> List[Dict]:
        """Extrae información de adjuntos del email"""
        attachments = []
        
        for part in email_message.walk():
            content_disposition = part.get_content_disposition()
            
            if content_disposition == 'attachment' or part.get_filename():
                filename = part.get_filename()
                
                if filename:
                    try:
                        decoded_filename = email.header.decode_header(filename)
                        filename = ''.join([
                            part[0].decode(part[1] or 'utf-8') if isinstance(part[0], bytes) else part[0]
                            for part in decoded_filename
                        ])
                    except Exception as e:
                        logger.warning(f"Error decodificando nombre de archivo: {e}")
                    
                    file_type = self.determine_file_type(filename)
                    
                    if file_type:
                        try:
                            content = part.get_payload(decode=True)
                            if content:
                                attachments.append({
                                    'filename': filename,
                                    'type': file_type,
                                    'size': len(content),
                                    'content': content
                                })
                                logger.debug(f"Adjunto encontrado: {filename} ({file_type}) - {len(content)} bytes")
                            else:
                                logger.warning(f"Adjunto sin contenido: {filename}")
                        except Exception as e:
                            logger.error(f"Error extrayendo contenido de {filename}: {e}")
                    else:
                        logger.debug(f"Tipo de archivo no reconocido: {filename}")
        
        logger.info(f"Total adjuntos extraídos: {len(attachments)}")
        return attachments
    
    def determine_file_type(self, filename: str) -> Optional[str]:
        """Determina si el archivo es CSV (presión) o PDF (ECG)"""
        filename_lower = filename.lower()
        
        if filename_lower.endswith('.csv') or 'bloodpressure' in filename_lower or 'pressure' in filename_lower:
            return 'pressure'
        elif filename_lower.endswith('.pdf') or 'complete' in filename_lower or 'ecg' in filename_lower:
            return 'ecg'
        
        return None
    
    def save_attachments(self, email_data: Dict, base_path: str = "data") -> Dict:
        """
        Guarda los adjuntos en el sistema de archivos organizados por paciente
        NUEVA ESTRUCTURA: data/Paciente_email@dominio.com/archivo.ext (sin subcarpetas por fecha)
        """
        patient_name = email_data['patient_name']
        sender_email = email_data['sender_email']
        email_date = email_data['email_date']
        
        # Limpiar caracteres problemáticos para nombres de carpeta
        clean_patient_name = re.sub(r'[<>:"/\\|?*]', '_', patient_name)
        clean_sender_email = re.sub(r'[<>:"/\\|?*]', '_', sender_email)
        
        # Crear nombre de carpeta: Paciente_email@dominio.com (SIN subcarpeta por fecha)
        folder_name = f"{clean_patient_name}_{clean_sender_email}"
        patient_dir = os.path.join(base_path, folder_name)
        os.makedirs(patient_dir, exist_ok=True)
        
        saved_files = []
        
        for i, attachment in enumerate(email_data['attachments']):
            try:
                # Para archivos de presión, usar timestamp del email
                if attachment['type'] == 'pressure':
                    email_timestamp = email_date.strftime("%Y-%m-%d_%H-%M-%S")
                    microseconds = str(email_date.microsecond)[:3]
                    file_extension = '.csv'
                    new_filename = f"pressure_{email_timestamp}_{microseconds}_{i}{file_extension}"
                
                # Para archivos ECG, extraer fecha del contenido del PDF
                elif attachment['type'] == 'ecg':
                    # Primero guardar temporalmente para extraer la fecha
                    temp_filename = f"temp_ecg_{i}.pdf"
                    temp_path = os.path.join(patient_dir, temp_filename)
                    
                    with open(temp_path, 'wb') as f:
                        f.write(attachment['content'])
                    
                    # Extraer fecha del contenido del PDF
                    ecg_date = self.extract_ecg_date_from_content(temp_path)
                    
                    if ecg_date:
                        # Si hay ambigüedad AM/PM, resolver usando archivos de presión
                        resolved_date = self.resolve_am_pm_ambiguity(ecg_date, patient_dir)
                        date_str = resolved_date.strftime("%Y-%m-%d_%H-%M-%S")
                        new_filename = f"ecg_{date_str}.pdf"
                    else:
                        # Fallback: usar fecha del email
                        email_timestamp = email_date.strftime("%Y-%m-%d_%H-%M-%S")
                        new_filename = f"ecg_{email_timestamp}_{i}.pdf"
                    
                    # Renombrar archivo temporal
                    final_path = os.path.join(patient_dir, new_filename)
                    os.rename(temp_path, final_path)
                    file_path = final_path
                else:
                    continue
                
                if attachment['type'] == 'pressure':
                    file_path = os.path.join(patient_dir, new_filename)
                    with open(file_path, 'wb') as f:
                        f.write(attachment['content'])
                
                saved_files.append({
                    'original_name': attachment['filename'],
                    'saved_path': file_path,
                    'type': attachment['type'],
                    'size': attachment['size']
                })
                
                logger.info(f"Archivo guardado: {file_path}")
                
            except Exception as e:
                logger.error(f"Error guardando archivo {attachment['filename']}: {e}")
        
        return {
            'patient_name': patient_name,
            'sender_email': sender_email,
            'email_date': email_date.strftime('%Y-%m-%d'),
            'folder_name': folder_name,
            'saved_files': saved_files
        }
    
    def extract_ecg_date_from_content(self, pdf_path: str) -> Optional[datetime]:
        """Extrae la fecha del contenido del PDF de ECG"""
        try:
            import pdfplumber
            
            with pdfplumber.open(pdf_path) as pdf:
                full_text = ""
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        full_text += page_text + "\n"
                
                if not full_text.strip():
                    return None
                
                # Buscar patrones de fecha en el texto
                datetime_patterns = [
                    # jueves, 22 de may de 2025, 8:15:26
                    r'(\w+),\s*(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})',
                    # 22 de mayo de 2025, 8:15:26
                    r'(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})',
                ]
                
                months_es = {
                    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'may': 5,
                    'junio': 6, 'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10,
                    'noviembre': 11, 'diciembre': 12
                }
                
                for pattern in datetime_patterns:
                    match = re.search(pattern, full_text, re.IGNORECASE)
                    if match:
                        try:
                            groups = match.groups()
                            
                            if len(groups) == 7:  # Con día de semana
                                _, day, month_name, year, hour, minute, second = groups
                            else:  # Sin día de semana
                                day, month_name, year, hour, minute, second = groups[:6]
                            
                            month = months_es.get(month_name.lower())
                            if month:
                                # NOTA: Aquí puede haber ambigüedad AM/PM
                                # Devolvemos la fecha sin resolver la ambigüedad
                                measurement_time = datetime(
                                    int(year), month, int(day),
                                    int(hour), int(minute), int(second)
                                )
                                logger.info(f"Fecha extraída del ECG: {measurement_time}")
                                return measurement_time
                                
                        except Exception as e:
                            logger.warning(f"Error parseando fecha del ECG: {e}")
                            continue
                
                return None
                
        except Exception as e:
            logger.error(f"Error extrayendo fecha del ECG: {e}")
            return None
    
    def resolve_am_pm_ambiguity(self, ecg_date: datetime, patient_dir: str) -> datetime:
        """
        Resuelve la ambigüedad AM/PM usando el archivo de presión más cercano
        """
        try:
            # Buscar archivos de presión en el directorio del paciente
            pressure_files = []
            for file in os.listdir(patient_dir):
                if file.startswith('pressure_') and file.endswith('.csv'):
                    file_path = os.path.join(patient_dir, file)
                    pressure_files.append(file_path)
            
            if not pressure_files:
                logger.warning("No se encontraron archivos de presión para resolver ambigüedad AM/PM")
                return ecg_date
            
            # Seleccionar el archivo de presión con más registros
            best_pressure_file = self.select_best_pressure_file(pressure_files)
            
            if not best_pressure_file:
                return ecg_date
            
            # Leer el archivo de presión y buscar mediciones cercanas
            pressure_times = self.extract_pressure_times(best_pressure_file)
            
            # Buscar la medición de presión más cercana (±2 minutos)
            closest_time = self.find_closest_pressure_time(ecg_date, pressure_times)
            
            if closest_time:
                # Ajustar la hora del ECG basándose en la medición de presión más cercana
                time_diff = abs((closest_time.hour - ecg_date.hour) % 12)
                
                if time_diff <= 2:  # Si la diferencia es pequeña, usar la misma parte del día
                    resolved_date = ecg_date.replace(
                        hour=closest_time.hour,
                        minute=closest_time.minute,
                        second=closest_time.second
                    )
                    logger.info(f"Ambigüedad AM/PM resuelta: {ecg_date} -> {resolved_date}")
                    return resolved_date
            
            return ecg_date
            
        except Exception as e:
            logger.error(f"Error resolviendo ambigüedad AM/PM: {e}")
            return ecg_date
    
    def select_best_pressure_file(self, pressure_files: List[str]) -> Optional[str]:
        """Selecciona el archivo de presión con más registros"""
        best_file = None
        max_records = 0
        
        for file_path in pressure_files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                record_count = len(df)
                
                if record_count > max_records:
                    max_records = record_count
                    best_file = file_path
                    
            except Exception as e:
                logger.warning(f"Error leyendo archivo de presión {file_path}: {e}")
                continue
        
        if best_file:
            logger.info(f"Archivo de presión seleccionado: {best_file} ({max_records} registros)")
        
        return best_file
    
    def extract_pressure_times(self, pressure_file: str) -> List[datetime]:
        """Extrae todas las fechas/horas del archivo de presión"""
        times = []
        
        try:
            df = pd.read_csv(pressure_file, encoding='utf-8')
            
            # Buscar columna de fecha
            date_columns = [col for col in df.columns if 'fecha' in col.lower() or 'date' in col.lower()]
            
            if date_columns:
                date_col = date_columns[0]
                
                for _, row in df.iterrows():
                    try:
                        date_str = str(row[date_col])
                        # Parsear diferentes formatos de fecha
                        parsed_date = pd.to_datetime(date_str)
                        if pd.notna(parsed_date):
                            times.append(parsed_date.to_pydatetime())
                    except Exception:
                        continue
        
        except Exception as e:
            logger.error(f"Error extrayendo tiempos de presión: {e}")
        
        return times
    
    def find_closest_pressure_time(self, ecg_date: datetime, pressure_times: List[datetime]) -> Optional[datetime]:
        """Encuentra la medición de presión más cercana al ECG (±2 minutos)"""
        closest_time = None
        min_diff = timedelta(minutes=2)  # Máximo 2 minutos de diferencia
        
        for pressure_time in pressure_times:
            # Comparar solo considerando la misma fecha
            if pressure_time.date() == ecg_date.date():
                # Calcular diferencia en minutos (ignorando segundos)
                ecg_minutes = ecg_date.hour * 60 + ecg_date.minute
                pressure_minutes = pressure_time.hour * 60 + pressure_time.minute
                
                diff = abs(ecg_minutes - pressure_minutes)
                
                if diff <= 2:  # Dentro del rango de 2 minutos
                    if closest_time is None or diff < min_diff.total_seconds() / 60:
                        closest_time = pressure_time
                        min_diff = timedelta(minutes=diff)
        
        return closest_time
    
    def disconnect(self):
        """Cierra la conexión al servidor de email"""
        if self.mail:
            try:
                self.mail.close()
                self.mail.logout()
                logger.info("Desconectado del servidor de email")
            except Exception as e:
                logger.warning(f"Error cerrando conexión: {e}")
