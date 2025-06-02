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

class EmailReader:
    def __init__(self, email_config: Dict[str, str]):
        """
        Inicializa el lector de emails
        
        Args:
            email_config: Diccionario con configuración del email
                - server: servidor IMAP
                - email: dirección de email
                - password: contraseña
                - port: puerto (default 993)
        """
        self.server = email_config['server']
        self.email = email_config['email']
        self.password = email_config['password']
        self.port = email_config.get('port', 993)
        self.mail = None
        self.processed_ids = set()  # Conjunto para rastrear IDs procesados
        
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
        """
        Extrae el nombre del paciente del asunto del email y remitente
        Especializado para formatos OMRON y médicos
        """
        # Limpiar el asunto
        subject = subject.strip()
        
        logger.debug(f"Analizando asunto: '{subject}' de remitente: '{sender}'")
        
        # Patrones específicos para OMRON (orden de prioridad)
        omron_patterns = [
            # [OMRON] Informe de ECG - Jdo-cardenas
            r'\[OMRON\]\s*Informe de ECG\s*-\s*([A-Za-z0-9\-_\.]+)',
            
            # [OMRON] Los datos de medición - Jdo-cardenas  
            r'\[OMRON\]\s*Los datos de medición\s*-\s*([A-Za-z0-9\-_\.]+)',
            
            # Cualquier patrón OMRON con guión
            r'\[OMRON\].*?-\s*([A-Za-z0-9\-_\.]+)',
            
            # OMRON sin corchetes
            r'OMRON.*?-\s*([A-Za-z0-9\-_\.]+)',
        ]
        
        # Probar patrones OMRON primero
        for pattern in omron_patterns:
            match = re.search(pattern, subject, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                if name and len(name) > 1:
                    # Limpiar y formatear el nombre
                    clean_name = self.clean_patient_name(name)
                    if clean_name:
                        logger.info(f"Nombre extraído de OMRON: '{clean_name}'")
                        return clean_name
        
        # Patrones para otros formatos médicos
        medical_patterns = [
            # EKG omron y TA dia 1 27 mayo noche paciente AEG de CVA COLOMBIA
            r'paciente\s+([A-Za-z0-9\-_\s]+?)(?:\s+de\s+|\s*$)',
            
            # Paciente: Juan Pérez
            r'[Pp]aciente:?\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)',
            
            # Juan Pérez - Mediciones
            r'^([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)\s*-\s*[Mm]edici[oó]n',
            
            # Nombre al inicio del asunto
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
    
    def extract_sender_email(self, sender: str) -> str:
        """Extrae solo la dirección de email del remitente"""
        if not sender:
            return "unknown@email.com"
        
        # Buscar email entre < >
        email_match = re.search(r'<([^>]+)>', sender)
        if email_match:
            return email_match.group(1).strip()
        
        # Si no hay < >, buscar patrón de email
        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', sender)
        if email_match:
            return email_match.group(1).strip()
        
        # Si no se encuentra email, usar el sender completo pero limpiarlo
        clean_sender = re.sub(r'[^\w\-\.]', '_', sender)
        return clean_sender[:30]  # Limitar longitud
    
    def parse_email_date(self, date_str: str) -> datetime:
        """Parsea la fecha del email y devuelve datetime"""
        if not date_str:
            return datetime.now()
        
        try:
            # Usar parsedate_to_datetime para parsear la fecha del email
            parsed_date = parsedate_to_datetime(date_str)
            # Convertir a datetime naive (sin zona horaria) para consistencia
            if parsed_date.tzinfo is not None:
                parsed_date = parsed_date.replace(tzinfo=None)
            return parsed_date
        except Exception as e:
            logger.warning(f"Error parseando fecha '{date_str}': {e}")
            return datetime.now()
    
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
            # Si es muy corto, mantener original
            if len(result) < 3:
                return name.strip()
            return result
        
        return name.strip()
    
    def extract_name_from_sender(self, sender: str) -> str:
        """Extrae nombre del campo remitente del email"""
        # Limpiar caracteres de codificación problemáticos
        sender = re.sub(r'Utf-8Q.*?c3A.*?n', '', sender)
        
        # Extraer nombre antes del email
        if '<' in sender:
            name_part = sender.split('<')[0].strip()
            if name_part and len(name_part) > 2:
                return self.clean_patient_name(name_part)
        
        # Extraer de la dirección de email
        if '@' in sender:
            email_part = sender.split('@')[0]
            # Remover caracteres comunes de emails
            email_part = re.sub(r'[<>"]', '', email_part)
            if len(email_part) > 2:
                return self.clean_patient_name(email_part)
        
        return ""
    
    def get_new_emails(self, days_back: int = 30, force_all: bool = True) -> List[Dict]:
        """
        Obtiene emails con adjuntos
        
        Args:
            days_back: Días hacia atrás para buscar emails
            force_all: Si es True, procesa todos los emails sin importar si están leídos
            
        Returns:
            Lista de diccionarios con información de emails
        """
        if not self.mail:
            logger.error("No hay conexión activa al email")
            return []
        
        try:
            self.mail.select('INBOX')
            email_list = []
            
            # Buscar TODOS los emails (sin filtro)
            logger.info("Buscando TODOS los emails...")
            search_criteria = 'ALL'
            status, messages = self.mail.search(None, search_criteria)
            
            if status != 'OK' or not messages[0]:
                logger.error("Error buscando emails o no hay emails")
                return []
            
            message_ids = messages[0].split()
            total_emails = len(message_ids)
            logger.info(f"Total de emails en la bandeja: {total_emails}")
            
            # Procesar los emails más recientes primero
            message_ids.reverse()
            
            # Limitar a 200 emails para no sobrecargar
            max_emails = 200
            if len(message_ids) > max_emails:
                logger.info(f"Limitando a los {max_emails} emails más recientes")
                message_ids = message_ids[:max_emails]
            
            # Contador para mostrar progreso
            processed = 0
            with_attachments = 0
            
            # Procesar cada email
            for msg_id in message_ids:
                processed += 1
                if processed % 10 == 0:
                    logger.info(f"Procesando email {processed}/{len(message_ids)}...")
                
                try:
                    # Verificar si ya procesamos este ID
                    if msg_id in self.processed_ids:
                        logger.debug(f"Email {msg_id.decode()} ya procesado anteriormente, omitiendo")
                        continue
                    
                    email_data = self.process_email(msg_id)
                    
                    # Registrar que procesamos este ID
                    self.processed_ids.add(msg_id)
                    
                    if email_data and email_data.get('attachments'):
                        with_attachments += 1
                        email_list.append(email_data)
                        logger.info(f"Email con adjuntos encontrado: {email_data['patient_name']} - {len(email_data['attachments'])} archivos")
                        
                        # Pequeña pausa para no sobrecargar el servidor
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
            
            # Extraer información básica
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
                    # Mantener el asunto original si hay error
            
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
                    # Mantener el remitente original si hay error
            
            # Procesar adjuntos primero
            attachments = self.extract_attachments(email_message)
            
            # Solo procesar emails con adjuntos relevantes
            if not attachments:
                return None
            
            # Extraer nombre del paciente (mejorado)
            patient_name = self.extract_patient_name(subject, sender)
            
            # Extraer email del remitente
            sender_email = self.extract_sender_email(sender)
            
            # Parsear fecha del email
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
        """Extrae información de adjuntos del email - MEJORADO"""
        attachments = []
        
        for part in email_message.walk():
            # Verificar si es un adjunto
            content_disposition = part.get_content_disposition()
            
            if content_disposition == 'attachment' or part.get_filename():
                filename = part.get_filename()
                
                if filename:
                    try:
                        # Decodificar nombre del archivo
                        decoded_filename = email.header.decode_header(filename)
                        filename = ''.join([
                            part[0].decode(part[1] or 'utf-8') if isinstance(part[0], bytes) else part[0]
                            for part in decoded_filename
                        ])
                    except Exception as e:
                        logger.warning(f"Error decodificando nombre de archivo: {e}")
                        # Mantener el nombre original si hay error
                    
                    # Determinar tipo de archivo
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
        
        if filename_lower.endswith('.csv') or 'bloodpressure' in filename_lower:
            return 'pressure'
        elif filename_lower.endswith('.pdf') or 'complete' in filename_lower or 'ecg' in filename_lower:
            return 'ecg'
        
        return None
    
    def save_attachments(self, email_data: Dict, base_path: str = "data") -> Dict:
        """
        Guarda los adjuntos en el sistema de archivos organizados por paciente, email y fecha
        
        Estructura: data/Paciente_email@dominio.com/YYYY-MM-DD/archivo_YYYY-MM-DD_HH-MM-SS.ext
        
        Returns:
            Diccionario con rutas de archivos guardados
        """
        patient_name = email_data['patient_name']
        sender_email = email_data['sender_email']
        email_date = email_data['email_date']
        
        # Limpiar caracteres problemáticos para nombres de carpeta
        clean_patient_name = re.sub(r'[<>:"/\\|?*]', '_', patient_name)
        clean_sender_email = re.sub(r'[<>:"/\\|?*]', '_', sender_email)
        
        # Crear nombre de carpeta: Paciente_email@dominio.com
        folder_name = f"{clean_patient_name}_{clean_sender_email}"
        
        # Usar la fecha del email para la subcarpeta
        email_date_str = email_date.strftime("%Y-%m-%d")
        
        # Crear directorio del paciente con email
        patient_dir = os.path.join(base_path, folder_name, email_date_str)
        os.makedirs(patient_dir, exist_ok=True)
        
        saved_files = []
        
        for i, attachment in enumerate(email_data['attachments']):
            try:
                # Usar la fecha y hora del email para el nombre del archivo
                email_timestamp = email_date.strftime("%Y-%m-%d_%H-%M-%S")
                microseconds = str(email_date.microsecond)[:3]  # Primeros 3 dígitos de microsegundos
                file_extension = os.path.splitext(attachment['filename'])[1]
                
                # Si no hay extensión, determinarla por tipo
                if not file_extension:
                    file_extension = '.pdf' if attachment['type'] == 'ecg' else '.csv'
                
                new_filename = f"{attachment['type']}_{email_timestamp}_{microseconds}_{i}{file_extension}"
                
                file_path = os.path.join(patient_dir, new_filename)
                
                # Guardar archivo
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
            'email_date': email_date_str,
            'folder_name': folder_name,
            'saved_files': saved_files
        }
    
    def disconnect(self):
        """Cierra la conexión al servidor de email"""
        if self.mail:
            try:
                self.mail.close()
                self.mail.logout()
                logger.info("Desconectado del servidor de email")
            except Exception as e:
                logger.warning(f"Error cerrando conexión: {e}")

# Ejemplo de uso
if __name__ == "__main__":
    # Configuración del email (reemplazar con datos reales)
    email_config = {
        'server': 'imap.gmail.com',  # o tu servidor IMAP
        'email': 'tu-email@dominio.com',
        'password': 'tu-contraseña',  # Usar contraseña de aplicación para Gmail
        'port': 993
    }
    
    # Crear instancia del lector
    reader = EmailReader(email_config)
    
    # Conectar y procesar emails
    if reader.connect():
        emails = reader.get_new_emails(days_back=30, force_all=True)
        
        print(f"Procesados {len(emails)} emails")
        
        for email_data in emails:
            print(f"\nPaciente: {email_data['patient_name']}")
            print(f"Email: {email_data['sender_email']}")
            print(f"Fecha: {email_data['email_date']}")
            print(f"Adjuntos: {len(email_data['attachments'])}")
            
            # Guardar adjuntos
            saved_info = reader.save_attachments(email_data)
            print(f"Archivos guardados en: {saved_info['folder_name']}/{saved_info['email_date']}")
        
        reader.disconnect()
    else:
        print("No se pudo conectar al servidor de email")
