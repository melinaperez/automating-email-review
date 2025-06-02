import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from email_reader import EmailReader
from file_validator import FileValidator

logger = logging.getLogger(__name__)

class MonitoringSystem:
    def __init__(self, config_file: str = "config.json"):
        """
        Inicializa el sistema de monitoreo
        
        Args:
            config_file: Archivo de configuración JSON
        """
        self.config = self.load_config(config_file)
        self.email_reader = EmailReader(self.config['email'])
        self.file_validator = FileValidator()
        self.data_path = self.config.get('data_path', 'data')
        self.reports_path = self.config.get('reports_path', 'reports')
        
        # Crear directorios si no existen
        os.makedirs(self.data_path, exist_ok=True)
        os.makedirs(self.reports_path, exist_ok=True)
        
        # Base de datos en memoria para tracking
        self.patient_data = {}
        self.validation_log = []
    
    def load_config(self, config_file: str) -> Dict:
        """Carga la configuración desde archivo JSON"""
        default_config = {
            'email': {
                'server': 'imap.gmail.com',
                'email': '',
                'password': '',
                'port': 993
            },
            'data_path': 'data',
            'reports_path': 'reports',
            'required_measurements_per_day': 4,
            'study_duration_days': 7,
            'authorized_senders': [],
            'patient_list': []
        }
        
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
        except Exception as e:
            logger.warning(f"No se pudo cargar configuración: {e}. Usando configuración por defecto.")
        
        return default_config
    
    def run_daily_check(self, force_all_emails: bool = True) -> Dict:
        """
        Ejecuta el chequeo diario completo
        
        Args:
            force_all_emails: Si es True, procesa todos los emails sin importar si están leídos
        
        Returns:
            Resumen del procesamiento diario
        """
        logger.info("Iniciando chequeo diario del sistema de monitoreo")
        
        summary = {
            'start_time': datetime.now().isoformat(),
            'emails_processed': 0,
            'files_validated': 0,
            'errors': [],
            'warnings': [],
            'patient_summaries': {}
        }
        
        try:
            # 1. PRIMERO: Escanear archivos existentes en el sistema de archivos
            logger.info("Escaneando archivos existentes en el sistema de archivos...")
            self.scan_existing_files(summary)
            
            # 2. Conectar al email y obtener nuevos mensajes
            if not self.email_reader.connect():
                summary['errors'].append("No se pudo conectar al servidor de email")
                # Continuar con el procesamiento aunque no se pueda conectar al email
            else:
                # 3. Procesar emails (ahora con opción de forzar todos)
                emails = self.email_reader.get_new_emails(days_back=30, force_all=force_all_emails)
                summary['emails_processed'] = len(emails)
                
                # 4. Procesar cada email
                for email_data in emails:
                    try:
                        self.process_email(email_data, summary)
                    except Exception as e:
                        error_msg = f"Error procesando email de {email_data.get('patient_name', 'DESCONOCIDO')}: {e}"
                        summary['errors'].append(error_msg)
                        logger.error(error_msg)
                
                # 5. Desconectar del email
                self.email_reader.disconnect()
            
            # 6. Generar reporte de completitud
            completeness_report = self.generate_completeness_report()
            summary['completeness_report'] = completeness_report
            
            summary['end_time'] = datetime.now().isoformat()
            
        except Exception as e:
            summary['errors'].append(f"Error general en chequeo diario: {e}")
            logger.error(f"Error en chequeo diario: {e}")
        
        return summary
    
    def scan_existing_files(self, summary: Dict):
        """Escanea todos los archivos existentes en el directorio de datos"""
        logger.info("Iniciando escaneo de archivos existentes")
        
        data_dir = self.data_path
        if not os.path.exists(data_dir):
            logger.warning(f"Directorio de datos no encontrado: {data_dir}")
            return
        
        files_scanned = 0
        
        # Recorrer todos los directorios de pacientes
        for patient_dir in os.listdir(data_dir):
            patient_path = os.path.join(data_dir, patient_dir)
            
            # Solo procesar directorios
            if not os.path.isdir(patient_path):
                continue
            
            # Extraer nombre del paciente (antes del guion bajo si hay email)
            patient_name = patient_dir.split('_')[0] if '_' in patient_dir else patient_dir
            
            logger.info(f"Escaneando archivos para paciente: {patient_name}")
            
            # Asegurarse de que el paciente esté en la base de datos
            if patient_name not in self.patient_data:
                self.patient_data[patient_name] = {
                    'measurements': [],
                    'last_update': datetime.now().isoformat()
                }
            
            # Recorrer todos los archivos del paciente
            for root, dirs, files in os.walk(patient_path):
                for file in files:
                    if file.endswith(('.csv', '.pdf')):
                        file_path = os.path.join(root, file)
                        files_scanned += 1
                        
                        try:
                            # Validar archivo
                            if file.endswith('.csv'):
                                result = self.file_validator.validate_csv_file(file_path)
                            else:
                                result = self.file_validator.validate_pdf_file(file_path)
                            
                            # Registrar el archivo si es válido
                            if result['is_valid']:
                                # Verificar si ya tenemos este archivo registrado
                                file_already_registered = any(
                                    m['file_path'] == file_path 
                                    for m in self.patient_data[patient_name]['measurements']
                                )
                                
                                if not file_already_registered:
                                    # Registrar el archivo
                                    log_entry = {
                                        'timestamp': datetime.now().isoformat(),
                                        'patient_name': patient_name,
                                        'file_path': file_path,
                                        'is_valid': True,
                                        'errors': result.get('errors', []),
                                        'warnings': result.get('warnings', []),
                                        'measurement_time': result.get('measurement_time'),
                                        'time_slot': result.get('time_slot')
                                    }
                                    
                                    self.patient_data[patient_name]['measurements'].append(log_entry)
                                    summary['files_validated'] += 1
                                    logger.info(f"Archivo registrado desde escaneo: {file_path} -> {result.get('time_slot', 'sin_franja')}")
                            else:
                                logger.warning(f"Archivo inválido encontrado: {file_path} - {result.get('errors', [])}")
                                
                        except Exception as e:
                            error_msg = f"Error escaneando archivo {file_path}: {e}"
                            summary['errors'].append(error_msg)
                            logger.error(error_msg)
        
        logger.info(f"Escaneo completado: {files_scanned} archivos procesados")
    
    def process_email(self, email_data: Dict, summary: Dict):
        """Procesa un email individual y valida sus archivos"""
        patient_name = email_data['patient_name']
        sender = email_data['sender']
        
        # Verificar remitente autorizado (si está configurado)
        if self.config.get('authorized_senders'):
            if not any(auth_sender in sender for auth_sender in self.config['authorized_senders']):
                # Auto-autorizar remitentes
                logger.info(f"Remitente autorizado automáticamente: {sender}")
        
        # Guardar archivos adjuntos
        saved_info = self.email_reader.save_attachments(email_data, self.data_path)
        
        # Validar cada archivo guardado
        for file_info in saved_info['saved_files']:
            try:
                validation_result = self.validate_file(file_info)
                self.log_validation(patient_name, validation_result)
                summary['files_validated'] += 1
                
                # Agregar errores y warnings al resumen
                if validation_result['errors']:
                    summary['errors'].extend([
                        f"{patient_name} - {file_info['original_name']}: {error}"
                        for error in validation_result['errors']
                    ])
                
                if validation_result['warnings']:
                    summary['warnings'].extend([
                        f"{patient_name} - {file_info['original_name']}: {warning}"
                        for warning in validation_result['warnings']
                    ])
                
            except Exception as e:
                error_msg = f"Error validando archivo {file_info['original_name']}: {e}"
                summary['errors'].append(error_msg)
                logger.error(error_msg)
    
    def validate_file(self, file_info: Dict) -> Dict:
        """Valida un archivo individual según su tipo"""
        file_path = file_info['saved_path']
        file_type = file_info['type']
        
        if file_type == 'pressure':
            return self.file_validator.validate_csv_file(file_path)
        elif file_type == 'ecg':
            return self.file_validator.validate_pdf_file(file_path)
        else:
            return {
                'file_path': file_path,
                'is_valid': False,
                'errors': [f"Tipo de archivo no reconocido: {file_type}"],
                'warnings': []
            }
    
    def log_validation(self, patient_name: str, validation_result: Dict):
        """Registra el resultado de validación en el log"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'patient_name': patient_name,
            'file_path': validation_result['file_path'],
            'is_valid': validation_result['is_valid'],
            'errors': validation_result['errors'],
            'warnings': validation_result['warnings'],
            'measurement_time': validation_result.get('measurement_time'),
            'time_slot': validation_result.get('time_slot')
        }
        
        self.validation_log.append(log_entry)
        
        # Actualizar datos del paciente
        if patient_name not in self.patient_data:
            self.patient_data[patient_name] = {
                'measurements': [],
                'last_update': datetime.now().isoformat()
            }
        
        self.patient_data[patient_name]['measurements'].append(log_entry)
        self.patient_data[patient_name]['last_update'] = datetime.now().isoformat()
        
        # Log para debugging
        if validation_result['is_valid']:
            logger.info(f"Medición válida registrada para {patient_name}: {validation_result.get('time_slot', 'sin_franja')} - {validation_result['file_path']}")
        else:
            logger.warning(f"Medición inválida para {patient_name}: {validation_result['errors']}")
    
    def generate_completeness_report(self) -> Dict:
        """Genera un reporte de completitud para todos los pacientes"""
        report = {
            'generation_date': datetime.now().isoformat(),
            'patients': {},
            'overall_summary': {
                'total_patients': 0,
                'patients_complete': 0,
                'patients_incomplete': 0,
                'total_measurements_expected': 0,
                'total_measurements_received': 0
            }
        }
        
        study_days = self.config.get('study_duration_days', 7)
        measurements_per_day = self.config.get('required_measurements_per_day', 4)
        
        for patient_name, patient_info in self.patient_data.items():
            patient_report = self.analyze_patient_completeness(
                patient_name, 
                patient_info, 
                study_days, 
                measurements_per_day
            )
            
            report['patients'][patient_name] = patient_report
            
            # Actualizar resumen general
            report['overall_summary']['total_patients'] += 1
            if patient_report['is_complete']:
                report['overall_summary']['patients_complete'] += 1
            else:
                report['overall_summary']['patients_incomplete'] += 1
            
            report['overall_summary']['total_measurements_expected'] += patient_report['expected_measurements']
            report['overall_summary']['total_measurements_received'] += patient_report['received_measurements']
        
        return report
    
    def analyze_patient_completeness(self, patient_name: str, patient_info: Dict, 
                                   study_days: int, measurements_per_day: int) -> Dict:
        """Analiza la completitud de un paciente específico"""
        measurements = patient_info['measurements']
        
        # Organizar mediciones por día y franja horaria
        daily_data = {}
        
        logger.info(f"Analizando completitud para {patient_name} con {len(measurements)} mediciones")
        
        for measurement in measurements:
            # Solo procesar mediciones válidas
            if not measurement.get('is_valid', False):
                logger.warning(f"Medición inválida omitida: {measurement.get('file_path', 'unknown')}")
                continue
            
            # Obtener tiempo de medición
            measurement_time_str = measurement.get('measurement_time')
            if not measurement_time_str:
                logger.warning(f"Medición sin tiempo: {measurement.get('file_path', 'unknown')}")
                continue
            
            try:
                # Parsear el tiempo de medición
                if isinstance(measurement_time_str, str):
                    # Remover 'Z' y '+00:00' si están presentes
                    measurement_time_str = measurement_time_str.replace('Z', '').replace('+00:00', '')
                    measurement_time = datetime.fromisoformat(measurement_time_str)
                else:
                    measurement_time = measurement_time_str
                
                date_key = measurement_time.date().isoformat()
                time_slot = measurement.get('time_slot', 'unknown')
                
                logger.info(f"Medición procesada: {date_key} {time_slot} - {measurement.get('file_path', 'unknown')}")
                
                # Inicializar estructura de datos diarios si no existe
                if date_key not in daily_data:
                    daily_data[date_key] = {
                        'matutina_1': {'pressure': False, 'ecg': False},
                        'matutina_2': {'pressure': False, 'ecg': False},
                        'vespertina_1': {'pressure': False, 'ecg': False},
                        'vespertina_2': {'pressure': False, 'ecg': False}
                    }
                
                # Determinar tipo de archivo basado en la ruta
                file_path = measurement.get('file_path', '')
                if 'pressure' in file_path.lower() or '.csv' in file_path.lower():
                    file_type = 'pressure'
                elif 'ecg' in file_path.lower() or '.pdf' in file_path.lower():
                    file_type = 'ecg'
                else:
                    logger.warning(f"Tipo de archivo no determinado: {file_path}")
                    continue
                
                # Marcar la medición como recibida si la franja horaria es válida
                if time_slot in daily_data[date_key]:
                    daily_data[date_key][time_slot][file_type] = True
                    logger.info(f"Marcado {file_type} en {date_key} {time_slot} para {patient_name}")
                else:
                    logger.warning(f"Franja horaria no válida: {time_slot}")
                    
            except Exception as e:
                logger.error(f"Error procesando medición para {patient_name}: {e}")
                continue
        
        # Calcular completitud basado en TODAS las mediciones encontradas
        received_measurements = 0
        missing_measurements = []
        
        logger.info(f"Calculando completitud para {patient_name} basado en TODAS las fechas encontradas")
        
        # Contar mediciones completas en TODAS las fechas encontradas
        for date_key, day_data in daily_data.items():
            for time_slot, measurements_slot in day_data.items():
                if measurements_slot['pressure'] and measurements_slot['ecg']:
                    received_measurements += 1
                    logger.info(f"Medición completa encontrada: {patient_name} {date_key} {time_slot}")
                else:
                    missing_items = []
                    if not measurements_slot['pressure']:
                        missing_items.append('presión')
                    if not measurements_slot['ecg']:
                        missing_items.append('ECG')
                    
                    if missing_items:  # Solo agregar si realmente faltan elementos
                        missing_measurements.append({
                            'date': date_key,
                            'time_slot': time_slot,
                            'missing': missing_items
                        })
        
        # Calcular mediciones esperadas basado en el período de estudio configurado
        expected_measurements = study_days * measurements_per_day
        
        # Si tenemos más mediciones de las esperadas, ajustar las esperadas
        if received_measurements > expected_measurements:
            expected_measurements = received_measurements
        
        completion_percentage = (received_measurements / expected_measurements * 100) if expected_measurements > 0 else 0
        
        logger.info(f"Completitud calculada para {patient_name}: {received_measurements}/{expected_measurements} = {completion_percentage:.2f}%")
        
        return {
            'patient_name': patient_name,
            'expected_measurements': expected_measurements,
            'received_measurements': received_measurements,
            'completion_percentage': round(completion_percentage, 2),
            'is_complete': completion_percentage >= 100,
            'missing_measurements': missing_measurements,
            'daily_data': daily_data,
            'last_update': patient_info['last_update']
        }
    
    def save_report(self, report: Dict, filename: str = None):
        """Guarda el reporte en archivo JSON"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"monitoring_report_{timestamp}.json"
        
        report_path = os.path.join(self.reports_path, filename)
        
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Reporte guardado en: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"Error guardando reporte: {e}")
            return None
    
    def export_to_excel(self, report: Dict, filename: str = None):
        """Exporta el reporte a Excel para fácil visualización"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"monitoring_report_{timestamp}.xlsx"
        
        excel_path = os.path.join(self.reports_path, filename)
        
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Hoja de resumen general
                summary_data = []
                for patient_name, patient_data in report['patients'].items():
                    summary_data.append({
                        'Paciente': patient_name,
                        'Mediciones Esperadas': patient_data['expected_measurements'],
                        'Mediciones Recibidas': patient_data['received_measurements'],
                        'Porcentaje Completitud': patient_data['completion_percentage'],
                        'Estado': 'Completo' if patient_data['is_complete'] else 'Incompleto',
                        'Última Actualización': patient_data['last_update']
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Resumen', index=False)
                
                # Hoja de mediciones faltantes
                missing_data = []
                for patient_name, patient_data in report['patients'].items():
                    for missing in patient_data['missing_measurements']:
                        missing_data.append({
                            'Paciente': patient_name,
                            'Fecha': missing['date'],
                            'Franja Horaria': missing['time_slot'],
                            'Faltantes': ', '.join(missing['missing'])
                        })
                
                if missing_data:
                    missing_df = pd.DataFrame(missing_data)
                    missing_df.to_excel(writer, sheet_name='Mediciones Faltantes', index=False)
            
            logger.info(f"Reporte Excel guardado en: {excel_path}")
            return excel_path
            
        except Exception as e:
            logger.error(f"Error exportando a Excel: {e}")
            return None

    def debug_patient_data(self):
        """Método para debugging - muestra los datos de pacientes"""
        logger.info("=== DEBUG: Datos de pacientes ===")
        for patient_name, patient_info in self.patient_data.items():
            logger.info(f"Paciente: {patient_name}")
            logger.info(f"  Total mediciones: {len(patient_info['measurements'])}")
            
            valid_measurements = [m for m in patient_info['measurements'] if m.get('is_valid')]
            logger.info(f"  Mediciones válidas: {len(valid_measurements)}")
            
            for measurement in valid_measurements:
                logger.info(f"    - {measurement.get('file_path', 'unknown')} -> {measurement.get('time_slot', 'sin_franja')} ({measurement.get('measurement_time', 'sin_tiempo')})")
        logger.info("=== FIN DEBUG ===")

# Ejemplo de uso
if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('monitoring_system.log'),
            logging.StreamHandler()
        ]
    )
    
    # Crear sistema de monitoreo
    system = MonitoringSystem()
    
    # Ejecutar chequeo diario
    daily_summary = system.run_daily_check(force_all_emails=True)
    
    # Mostrar resumen
    print(f"Emails procesados: {daily_summary['emails_processed']}")
    print(f"Archivos validados: {daily_summary['files_validated']}")
    print(f"Errores: {len(daily_summary['errors'])}")
    print(f"Advertencias: {len(daily_summary['warnings'])}")
    
    # Guardar reportes
    if 'completeness_report' in daily_summary:
        report_path = system.save_report(daily_summary['completeness_report'])
        excel_path = system.export_to_excel(daily_summary['completeness_report'])
        
        print(f"Reporte JSON: {report_path}")
        print(f"Reporte Excel: {excel_path}")
