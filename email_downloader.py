#!/usr/bin/env python3
"""
Descargador de adjuntos de emails - Primera etapa del proceso
Solo se encarga de descargar y organizar archivos, sin validación
"""

import json
import os
import logging
from datetime import datetime
from improved_email_reader import ImprovedEmailReader

logger = logging.getLogger(__name__)

class EmailDownloader:
    def __init__(self, config_file: str = "config.json"):
        """Inicializa el descargador de emails"""
        self.config_file = config_file
        self.config = self.load_config()
        self.email_reader = None
        self.download_summary = {
            'emails_processed': 0,
            'files_downloaded': 0,
            'patients_found': 0,
            'errors': [],
            'warnings': [],
            'download_log': []
        }
    
    def load_config(self) -> dict:
        """Carga la configuración desde el archivo JSON"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"Configuración cargada desde {self.config_file}")
            return config
        except Exception as e:
            logger.error(f"Error cargando configuración: {e}")
            raise
    
    def connect_to_email(self) -> bool:
        """Conecta al servidor de email"""
        try:
            email_config = self.config.get('email', {})
            if not email_config:
                raise ValueError("No se encontró configuración de email")
            
            self.email_reader = ImprovedEmailReader(email_config)
            
            if self.email_reader.connect():
                logger.info("✅ Conexión al email establecida exitosamente")
                return True
            else:
                logger.error("❌ No se pudo conectar al email")
                return False
                
        except Exception as e:
            logger.error(f"Error conectando al email: {e}")
            self.download_summary['errors'].append(f"Error de conexión: {str(e)}")
            return False
    
    def download_all_attachments(self, days_back: int = 30, force_all: bool = True) -> dict:
        """
        Descarga todos los adjuntos de emails sin validación
        
        Args:
            days_back: Días hacia atrás para buscar emails
            force_all: Si procesar todos los emails o solo nuevos
        
        Returns:
            Diccionario con resumen de la descarga
        """
        logger.info("🚀 INICIANDO DESCARGA DE ADJUNTOS")
        logger.info("=" * 50)
        
        if not self.connect_to_email():
            return self.download_summary
        
        try:
            # Obtener emails con adjuntos
            logger.info(f"📧 Buscando emails de los últimos {days_back} días...")
            emails = self.email_reader.get_new_emails(days_back=days_back, force_all=force_all)
            
            self.download_summary['emails_processed'] = len(emails)
            logger.info(f"📧 Emails encontrados con adjuntos: {len(emails)}")
            
            if not emails:
                logger.warning("❌ No se encontraron emails con adjuntos")
                return self.download_summary
            
            # Procesar cada email y descargar adjuntos
            patients_found = set()
            
            for i, email_data in enumerate(emails, 1):
                logger.info(f"\n📧 Procesando email {i}/{len(emails)}")
                logger.info(f"   👤 Paciente: {email_data['patient_name']}")
                logger.info(f"   📅 Fecha: {email_data['email_date'].strftime('%Y-%m-%d %H:%M')}")
                logger.info(f"   📎 Adjuntos: {len(email_data['attachments'])}")
                
                try:
                    # Descargar adjuntos
                    save_result = self.email_reader.save_attachments(email_data, base_path="data")
                    
                    if save_result and save_result.get('saved_files'):
                        patients_found.add(email_data['patient_name'])
                        files_saved = len(save_result['saved_files'])
                        self.download_summary['files_downloaded'] += files_saved
                        
                        # Registrar descarga
                        download_entry = {
                            'patient_name': email_data['patient_name'],
                            'sender_email': email_data['sender_email'],
                            'email_date': email_data['email_date'].isoformat(),
                            'files_saved': files_saved,
                            'folder_name': save_result['folder_name'],
                            'files': [f['saved_path'] for f in save_result['saved_files']]
                        }
                        self.download_summary['download_log'].append(download_entry)
                        
                        logger.info(f"   ✅ {files_saved} archivos descargados en: {save_result['folder_name']}")
                        
                        # Mostrar archivos descargados
                        for file_info in save_result['saved_files']:
                            file_type = "📊 CSV" if file_info['type'] == 'pressure' else "📄 PDF"
                            logger.info(f"      {file_type} {os.path.basename(file_info['saved_path'])} ({file_info['size']} bytes)")
                    else:
                        logger.warning(f"   ❌ No se pudieron guardar adjuntos del email")
                        self.download_summary['warnings'].append(f"Error guardando adjuntos de {email_data['patient_name']}")
                
                except Exception as e:
                    logger.error(f"   ❌ Error procesando email: {e}")
                    self.download_summary['errors'].append(f"Error procesando email de {email_data['patient_name']}: {str(e)}")
                    continue
            
            self.download_summary['patients_found'] = len(patients_found)
            
            # Mostrar resumen final
            self.show_download_summary()
            
            # Guardar log de descarga
            self.save_download_log()
            
        except Exception as e:
            logger.error(f"Error durante la descarga: {e}")
            self.download_summary['errors'].append(f"Error general: {str(e)}")
        
        finally:
            if self.email_reader:
                self.email_reader.disconnect()
        
        return self.download_summary
    
    def show_download_summary(self):
        """Muestra un resumen detallado de la descarga"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 RESUMEN DE DESCARGA DE ADJUNTOS")
        logger.info("=" * 60)
        logger.info(f"📧 Emails procesados: {self.download_summary['emails_processed']}")
        logger.info(f"📎 Archivos descargados: {self.download_summary['files_downloaded']}")
        logger.info(f"👥 Pacientes encontrados: {self.download_summary['patients_found']}")
        logger.info(f"❌ Errores: {len(self.download_summary['errors'])}")
        logger.info(f"⚠️ Advertencias: {len(self.download_summary['warnings'])}")
        
        if self.download_summary['errors']:
            logger.info("\n❌ ERRORES ENCONTRADOS:")
            for error in self.download_summary['errors']:
                logger.info(f"   - {error}")
        
        if self.download_summary['warnings']:
            logger.info("\n⚠️ ADVERTENCIAS:")
            for warning in self.download_summary['warnings']:
                logger.info(f"   - {warning}")
        
        # Mostrar archivos por paciente
        if self.download_summary['download_log']:
            logger.info("\n📁 ARCHIVOS DESCARGADOS POR PACIENTE:")
            patients_summary = {}
            
            for entry in self.download_summary['download_log']:
                patient = entry['patient_name']
                if patient not in patients_summary:
                    patients_summary[patient] = {'files': 0, 'folder': entry['folder_name']}
                patients_summary[patient]['files'] += entry['files_saved']
            
            for patient, info in patients_summary.items():
                logger.info(f"   👤 {patient}: {info['files']} archivos en {info['folder']}")
    
    def save_download_log(self):
        """Guarda el log de descarga en un archivo JSON"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"logs/download_log_{timestamp}.json"
            
            os.makedirs("logs", exist_ok=True)
            
            with open(log_filename, 'w', encoding='utf-8') as f:
                json.dump(self.download_summary, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📄 Log de descarga guardado en: {log_filename}")
            
        except Exception as e:
            logger.error(f"Error guardando log de descarga: {e}")
    
    def list_downloaded_files(self) -> dict:
        """Lista todos los archivos descargados organizados por paciente"""
        data_dir = "data"
        if not os.path.exists(data_dir):
            logger.warning("Directorio 'data' no encontrado")
            return {}
        
        patients_files = {}
        
        for patient_dir in os.listdir(data_dir):
            patient_path = os.path.join(data_dir, patient_dir)
            if os.path.isdir(patient_path):
                files_info = {
                    'csv_files': [],
                    'pdf_files': [],
                    'total_files': 0
                }
                
                for file in os.listdir(patient_path):
                    file_path = os.path.join(patient_path, file)
                    if os.path.isfile(file_path):
                        file_size = os.path.getsize(file_path)
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        
                        file_info = {
                            'name': file,
                            'path': file_path,
                            'size': file_size,
                            'modified': file_mtime.isoformat()
                        }
                        
                        if file.lower().endswith('.csv') or 'pressure' in file.lower():
                            files_info['csv_files'].append(file_info)
                        elif file.lower().endswith('.pdf') or 'ecg' in file.lower():
                            files_info['pdf_files'].append(file_info)
                        
                        files_info['total_files'] += 1
                
                if files_info['total_files'] > 0:
                    patients_files[patient_dir] = files_info
        
        return patients_files

def main():
    """Función principal para ejecutar solo la descarga"""
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/download_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    print("🚀 DESCARGADOR DE ADJUNTOS DE EMAILS")
    print("=" * 50)
    print("Esta es la PRIMERA ETAPA del proceso:")
    print("1. ✅ Conectar al email")
    print("2. ✅ Buscar emails con adjuntos")
    print("3. ✅ Descargar y organizar archivos")
    print("4. ❌ NO validar archivos (eso es la segunda etapa)")
    print()
    
    try:
        downloader = EmailDownloader()
        
        # Ejecutar descarga
        summary = downloader.download_all_attachments(days_back=30, force_all=True)
        
        # Mostrar archivos descargados
        print("\n📁 ARCHIVOS DISPONIBLES PARA VALIDACIÓN:")
        print("=" * 50)
        
        files_by_patient = downloader.list_downloaded_files()
        
        if files_by_patient:
            for patient, files_info in files_by_patient.items():
                csv_count = len(files_info['csv_files'])
                pdf_count = len(files_info['pdf_files'])
                total_count = files_info['total_files']
                
                print(f"👤 {patient}:")
                print(f"   📊 CSV (presión): {csv_count}")
                print(f"   📄 PDF (ECG): {pdf_count}")
                print(f"   📁 Total: {total_count}")
        else:
            print("❌ No se encontraron archivos descargados")
        
        print(f"\n✅ DESCARGA COMPLETADA")
        print(f"📧 Emails procesados: {summary['emails_processed']}")
        print(f"📎 Archivos descargados: {summary['files_downloaded']}")
        print(f"👥 Pacientes: {summary['patients_found']}")
        
        if summary['errors']:
            print(f"❌ Errores: {len(summary['errors'])}")
        
        print("\n🔄 SIGUIENTE PASO:")
        print("   Ejecutar: python file_analyzer.py")
        print("   Para validar y analizar los archivos descargados")
        
    except Exception as e:
        logger.error(f"Error en descarga: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
